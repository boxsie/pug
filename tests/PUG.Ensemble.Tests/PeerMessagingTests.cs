using System.Text;
using EC = Ensemble.Client;
using Ensemble.Client.Testing;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Coverage for <see cref="QueueHandle{TPayload}.SendToPeerAsync"/> and
/// <see cref="QueueHandle{TPayload}.PeerMessages"/> — the post-match P2P
/// surface added by ticket <c>dc68e6ed</c>.
///
/// <para>
/// <b>Topology note.</b> Same constraint as
/// <see cref="EnsemblePlayerClientTests"/>: <c>rpc.Service.Send</c> has no
/// same-daemon local fast path, so a real-wire test of two players sending
/// bytes between their services on one daemon is structurally impossible.
/// These tests therefore exercise the channel routing + lifecycle + dispose
/// + API-gate behaviour via the <c>internal InjectPeerMessageAsync</c> test
/// seam, mirroring the pattern that already covers the matchmaker host.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class PeerMessagingTests : IAsyncLifetime
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
    /// Happy path: an injected peer RpcMessage surfaces through
    /// <see cref="QueueHandle{TPayload}.PeerMessages"/> with the correct
    /// <c>FromAddr</c>, <c>Bytes</c>, and a wall-clock <c>Arrived</c> stamp.
    /// </summary>
    [DaemonFact]
    public async Task PeerMessages_DeliversInjectedMessage()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        var peerAddr = $"E{Guid.NewGuid():N}";
        var payload = Encoding.UTF8.GetBytes("hello peer");
        var before = DateTimeOffset.UtcNow;
        await handle.InjectPeerMessageAsync(peerAddr, payload);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await foreach (var msg in handle.PeerMessages(cts.Token))
        {
            Assert.Equal(peerAddr, msg.FromAddr);
            Assert.Equal(payload, msg.Bytes.ToArray());
            Assert.InRange(msg.Arrived, before, DateTimeOffset.UtcNow);
            return;
        }
        Assert.Fail("PeerMessages enumerator yielded no items before the cancellation window expired.");
    }

    /// <summary>
    /// API gate: <see cref="QueueHandle{TPayload}.SendToPeerAsync"/> before
    /// <see cref="QueueHandle{TPayload}.WaitForMatchAsync"/> has succeeded
    /// throws <see cref="InvalidOperationException"/>. The send path must
    /// not silently succeed against a still-queueing handle — the peer
    /// hasn't been introduced yet and the daemon has no routing for the
    /// bytes.
    /// </summary>
    [DaemonFact]
    public async Task SendToPeerAsync_BeforeMatch_Throws()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handle.SendToPeerAsync(
                peerAddr: $"E{Guid.NewGuid():N}",
                bytes: new byte[] { 1, 2, 3 },
                ct: Cancel(5)));
    }

    /// <summary>
    /// Dispose guard: <see cref="QueueHandle{TPayload}.SendToPeerAsync"/>
    /// on a disposed handle throws <see cref="ObjectDisposedException"/>.
    /// The dispose check runs before the match-required gate, so it doesn't
    /// matter whether a match was ever completed.
    /// </summary>
    [DaemonFact]
    public async Task SendToPeerAsync_AfterDispose_Throws()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));
        await handle.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            handle.SendToPeerAsync(
                peerAddr: $"E{Guid.NewGuid():N}",
                bytes: new byte[] { 1, 2, 3 },
                ct: Cancel(5)));
    }

    /// <summary>
    /// Dispose-while-enumerating: a consumer awaiting
    /// <see cref="QueueHandle{TPayload}.PeerMessages"/> terminates cleanly
    /// when the handle is disposed. No exception leaks; the enumerator
    /// completes.
    /// </summary>
    [DaemonFact]
    public async Task PeerMessages_TerminatesCleanlyOnDispose()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        // Start enumerating on a background task — there's nothing in the
        // channel yet, so the consumer will block on WaitToReadAsync.
        var consumer = Task.Run(async () =>
        {
            var seen = 0;
            await foreach (var _ in handle.PeerMessages(CancellationToken.None))
            {
                seen++;
            }
            return seen;
        });

        // Give the consumer a moment to reach the WaitToReadAsync block.
        await Task.Delay(100);
        await handle.DisposeAsync();

        var seenCount = await consumer.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, seenCount);
    }

    /// <summary>
    /// Contract guard: the <c>InjectPeerMessageAsync</c> test seam refuses
    /// the matchmaker's own address — peer messages by definition come
    /// from a non-matchmaker sender, and feeding the matchmaker addr
    /// through this path would test a code shape that doesn't exist in
    /// production. The corresponding production dispatch case (matchmaker
    /// vs peer) is enforced by the <c>switch</c> arm ordering in
    /// <see cref="EnsemblePlayerClient"/>'s onEvent callback.
    /// </summary>
    [DaemonFact]
    public async Task InjectPeerMessageAsync_RejectsMatchmakerAddr()
    {
        await using var matchmaker = await StubMatchmaker.StartAsync(_client, $"stub-mm-{Guid.NewGuid():N}");
        await using var player = new EnsemblePlayerClient(_client);

        var sessionId = $"sess-{Guid.NewGuid():N}";
        await using var handle = await player.CreatePreQueuedHandleAsync<string>(
            matchmaker.ServiceAddress, sessionId, Cancel(15));

        await Assert.ThrowsAsync<ArgumentException>(() =>
            handle.InjectPeerMessageAsync(matchmaker.ServiceAddress, new byte[] { 9 }));
    }
}
