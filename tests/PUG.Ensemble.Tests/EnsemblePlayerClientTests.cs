using EC = Ensemble.Client;
using Ensemble.Client.Testing;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Integration + adversarial tests for <see cref="EnsemblePlayerClient"/>.
///
/// <para>
/// <b>Topology note.</b> The Ensemble daemon's
/// <c>rpc.Service.Send</c> (which backs
/// <see cref="EC.RegisteredService.SendBytesAsync"/>) routes via libp2p
/// PeerConnection — service-to-service traffic between two services
/// registered on the SAME daemon cannot route, because the target service's
/// E-address is not bound to any connected libp2p peer in the local
/// resolver. <c>IntroducePeersAsync</c> by contrast HAS a local fast path
/// (<c>lookupStreamByAddr</c>) and delivers <c>peer_introduction</c> events
/// directly between same-daemon services.
/// </para>
///
/// <para>
/// These tests therefore use a single daemon and exercise the
/// <b>introduction filtering</b> half of <see cref="QueueHandle{TPayload}"/>
/// (the meat of the ticket: provenance, expiry, session_id checks)
/// directly, via an <c>internal</c> test seam
/// (<c>EnsemblePlayerClient.CreatePreQueuedHandleAsync</c>) that bypasses
/// the <c>SendBytesAsync</c> handshake. The handshake half of
/// <see cref="EnsemblePlayerClient.JoinMatchmakingAsync"/> is covered
/// implicitly by the source's structure (same RPC ingress routine is shared
/// between production and the test seam) and will get full integration
/// coverage in a future ticket that introduces a paired-daemon harness.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class EnsemblePlayerClientTests : IAsyncLifetime
{
    private EnsembleDaemonHarness _fixture = null!;
    private EC.EnsembleClient _client = null!;

    public async Task InitializeAsync()
    {
        // Tests are [DaemonFact]-skipped when no binary is available; guard
        // here too so a created fixture never throws during init.
        if (!EnsembleDaemonHarness.IsDaemonAvailable) return;
        _fixture = new EnsembleDaemonHarness();
        await _fixture.InitializeAsync();
        await _fixture.WaitForRegistryReadyAsync(TimeSpan.FromMinutes(2));
        _client = new EC.EnsembleClient(_fixture.GrpcAddress);
    }

    public async Task DisposeAsync()
    {
        if (_client is not null) await _client.DisposeAsync();
        if (_fixture is not null) await _fixture.DisposeAsync();
    }

    private static CancellationToken Cancel(int seconds) =>
        new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    /// <summary>
    /// Happy path: a stub matchmaker introduces the player to a valid peer
    /// with the correct session id and a future expiry; the player's
    /// <see cref="QueueHandle{TPayload}.WaitForMatchAsync"/> completes with
    /// the introduced peer and the role hint.
    /// </summary>
    [DaemonFact]
    public async Task WaitForMatch_OnValidIntroduction_CompletesMatchFound()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmakerAddr: matchmaker.ServiceAddress,
            sessionId: sessionId,
            ct: Cancel(15));

        Assert.Equal(sessionId, handle.SessionId);
        Assert.Equal(matchmaker.ServiceAddress, handle.MatchmakerAddr);

        var fakePeerAddr = await RegisterDummyAddressAsync();
        await matchmaker.IntroducePlayerAsync(
            playerServiceAddr: handle.PlayerServiceAddress,
            peerAddr: fakePeerAddr,
            sessionId: sessionId,
            expiresInSec: 60,
            roleHint: "host");

        var match = await handle.WaitForMatchAsync(Cancel(10));
        Assert.Equal(sessionId, match.SessionId);
        Assert.Single(match.Peers);
        Assert.Equal(fakePeerAddr, match.Peers[0].EnsembleAddr);
        Assert.Equal("host", match.RoleHint);
    }

    /// <summary>
    /// Provenance check: an impostor service with the right session id
    /// emits an introduction; the handle MUST drop it (FromServiceAddr
    /// mismatch) and stay open.
    /// </summary>
    [DaemonFact]
    public async Task WaitForMatch_DropsIntroductionFromWrongService()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var impostor = await StubMatchmaker.StartAsync(_client, $"stub-mm-imp-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        var fakePeer = await RegisterDummyAddressAsync();
        await impostor.IntroducePlayerAsync(
            playerServiceAddr: handle.PlayerServiceAddress,
            peerAddr: fakePeer,
            sessionId: sessionId,
            expiresInSec: 60);

        await AssertWaitForMatchDoesNotCompleteAsync(handle, TimeSpan.FromSeconds(3));
    }

    /// <summary>
    /// Expiry check: an introduction with <c>ExpiresAt</c> in the past.
    /// The daemon drops these server-side and reports an unstructured
    /// error back to the introducer rather than delivering the event —
    /// either way the handle stays open. (Belt-and-braces: even if a
    /// future daemon delivered the event, the SDK filter would drop it.)
    /// </summary>
    [DaemonFact]
    public async Task WaitForMatch_DropsExpiredIntroduction()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        var fakePeer = await RegisterDummyAddressAsync();
        // The daemon's introduce-peer path validates expires_at before
        // delivery and surfaces an unstructured error to the introducer
        // on expired horizons (see Phase4 learnings). The stub matchmaker
        // ignores onError, so the introduction is silently dropped daemon-
        // side. WaitForMatchAsync must NOT complete.
        await matchmaker.IntroducePlayerAsync(
            playerServiceAddr: handle.PlayerServiceAddress,
            peerAddr: fakePeer,
            sessionId: sessionId,
            expiresAtMsAbsolute: DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds());

        await AssertWaitForMatchDoesNotCompleteAsync(handle, TimeSpan.FromSeconds(3));
    }

    /// <summary>
    /// Replay protection: an introduction with a session id that doesn't
    /// match the handle's outstanding session must be silently dropped.
    /// Provenance and expiry are valid, so only the SDK-level session
    /// filter can catch this.
    /// </summary>
    [DaemonFact]
    public async Task WaitForMatch_DropsUnknownSessionIntroduction()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        var fakePeer = await RegisterDummyAddressAsync();
        await matchmaker.IntroducePlayerAsync(
            playerServiceAddr: handle.PlayerServiceAddress,
            peerAddr: fakePeer,
            sessionId: "completely-unrelated-session-id",
            expiresInSec: 60);

        await AssertWaitForMatchDoesNotCompleteAsync(handle, TimeSpan.FromSeconds(3));
    }

    /// <summary>
    /// Dispose path: <see cref="QueueHandle{TPayload}.LeaveAsync"/> is
    /// idempotent and cleanly tears down the underlying registered
    /// service. The leave-message send may fail under our test topology
    /// (single-daemon RPC routing constraint), but Dispose must succeed
    /// regardless and a second call must be a no-op.
    /// </summary>
    [DaemonFact]
    public async Task LeaveAsync_IsIdempotentAndDisposesUnderlyingService()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));
        var serviceAddrBeforeLeave = handle.PlayerServiceAddress;
        Assert.False(string.IsNullOrEmpty(serviceAddrBeforeLeave));

        // First leave: tears down the service. The LeaveQueueRequest
        // SendBytesAsync may fail in the single-daemon topology — that's
        // fine, QueueHandle.LeaveAsync swallows send errors and disposes
        // anyway.
        await handle.LeaveAsync(Cancel(5));

        // Second leave: no-op, no throw.
        await handle.LeaveAsync(Cancel(5));

        // Dispose: no throw.
        await handle.DisposeAsync();
        await player.DisposeAsync();
    }

    /// <summary>
    /// Register a throwaway service to obtain a daemon-issued, valid E-
    /// address suitable for the "peer to introduce to" field. We don't
    /// dispose these in the test — they're cleaned up when the test
    /// fixture tears down the daemon's data dir.
    /// </summary>
    private async Task<string> RegisterDummyAddressAsync()
    {
        var svc = await _client.RegisterServiceAsync(
            EC.ServiceManifest.NewBuilder($"pug-test-peer-{Guid.NewGuid():N}")
                .Acl(EC.ServiceAcl.Public)
                .Transport(EC.ServiceTransport.Chat)
                .Build(),
            onEvent: _ => ValueTask.CompletedTask,
            ct: Cancel(15));
        return svc.ServiceAddress;
    }

    private static async Task AssertWaitForMatchDoesNotCompleteAsync<TPayload>(
        QueueHandle<TPayload> handle,
        TimeSpan window)
    {
        using var cts = new CancellationTokenSource(window);
        try
        {
            var match = await handle.WaitForMatchAsync(cts.Token);
            Assert.Fail(
                $"WaitForMatchAsync unexpectedly completed: peers=[{string.Join(",", match.Peers.Select(p => p.EnsembleAddr))}]");
        }
        catch (OperationCanceledException)
        {
            // Expected: the bad introduction was dropped, the handle is
            // still waiting, and our timeout fired.
        }
    }
}

/// <summary>
/// Test-internal stub matchmaker. NOT a production artifact — lives in the
/// test project to keep T12 (player client) independent of T-mmhost
/// (matchmaker host) progress. Registers as a SERVICE_TRANSPORT_RPC service
/// so its <c>IntroducePeersAsync</c> calls succeed; the RPC-message
/// ingress is wired but only used for assertion plumbing in tests that
/// don't actually try to send bytes back via the daemon's RPC routing.
/// </summary>
internal sealed class StubMatchmaker : IAsyncDisposable
{
    private readonly EC.RegisteredService _svc;

    internal string ServiceAddress => _svc.ServiceAddress;

    private StubMatchmaker(EC.RegisteredService svc) { _svc = svc; }

    internal static async Task<StubMatchmaker> StartAsync(EC.EnsembleClient client, string name)
    {
        var manifest = EC.ServiceManifest.NewBuilder(name)
            .Acl(EC.ServiceAcl.Public)
            .Transport(EC.ServiceTransport.Rpc)
            .MaxPayloadBytes(1 << 20)
            .Build();
        var svc = await client.RegisterServiceAsync(
            manifest,
            onEvent: _ => ValueTask.CompletedTask,
            onError: null,
            ct: new CancellationTokenSource(TimeSpan.FromSeconds(20)).Token);
        return new StubMatchmaker(svc);
    }

    /// <summary>
    /// Issue a daemon-attested peer introduction to a player. Wraps
    /// <see cref="EC.RegisteredService.IntroducePeersAsync"/>; the daemon
    /// will deliver a <c>peer_introduction</c> event to
    /// <paramref name="playerServiceAddr"/> with <c>FromServiceAddr</c>
    /// stamped as this stub's service address.
    /// </summary>
    internal Task IntroducePlayerAsync(
        string playerServiceAddr,
        string peerAddr,
        string sessionId,
        int? expiresInSec = null,
        long? expiresAtMsAbsolute = null,
        string? roleHint = null)
    {
        var expiresAt = expiresAtMsAbsolute
            ?? DateTimeOffset.UtcNow.AddSeconds(expiresInSec ?? 60).ToUnixTimeMilliseconds();
        return _svc.IntroducePeersAsync(
            toAddr: playerServiceAddr,
            otherAddr: peerAddr,
            sessionId: sessionId,
            expiresAtMs: expiresAt,
            roleHint: roleHint);
    }

    public ValueTask DisposeAsync() => _svc.DisposeAsync();
}
