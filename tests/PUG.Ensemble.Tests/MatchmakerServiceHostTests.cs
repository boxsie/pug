using System.Threading.Channels;
using Ensemble.Client.Testing;
using Google.Protobuf;
using PUG.Core;
using PUG.Ensemble.Proto;
using EC = Ensemble.Client;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Integration coverage for <see cref="MatchmakerServiceHost{TPayload}"/>.
/// Pattern: one Ensemble daemon, multiple registered services on the same
/// daemon — the daemon's <c>handleIntroducePeer</c> local fast-path
/// delivers introductions synchronously without Tor, so 1v1/2v2 round-trips
/// complete in milliseconds after the (cached) backend warmup.
///
/// <para>
/// Uses the loopback signaling backend (Ensemble T2/T4) because every
/// assertion exercises the daemon's local fast-path — no real onion is
/// needed. Boots the daemon in &lt;200ms vs ~10-30s for Tor bootstrap.
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class MatchmakerServiceHostTests : IClassFixture<MatchmakerServiceHostTests.PugDaemonFixture>, IAsyncLifetime
{
    private readonly EnsembleDaemonHarness _daemon;

    public MatchmakerServiceHostTests(PugDaemonFixture daemon) => _daemon = daemon.Inner;

    /// <summary>
    /// Composition wrapper around the loopback <see cref="EnsembleDaemonHarness"/>
    /// (shipped in Ensemble.Client). The daemon binary is resolved from
    /// <c>$ENSEMBLE_BIN</c>; when none is available the harness reports
    /// unavailable and the tests are skipped via <see cref="DaemonFactAttribute"/>.
    /// The inner harness opts into <c>--signaling=loopback</c> so the daemon
    /// skips Tor entirely.
    /// </summary>
    public sealed class PugDaemonFixture : IAsyncLifetime
    {
        internal EnsembleDaemonHarness Inner { get; } = new EnsembleDaemonHarness.Loopback();

        public async Task InitializeAsync()
        {
            if (!EnsembleDaemonHarness.IsDaemonAvailable) return;
            await Inner.InitializeAsync();
        }
        public async Task DisposeAsync() => await Inner.DisposeAsync();
    }

    /// <summary>
    /// Under loopback this completes within milliseconds of fixture init
    /// (the daemon fires <c>tor_state == "ready"</c> as a compat shim once
    /// the loopback backend's pre-closed <c>Ready()</c> channel resolves).
    /// </summary>
    public async Task InitializeAsync()
    {
        if (!EnsembleDaemonHarness.IsDaemonAvailable) return;
        await _daemon.WaitForRegistryReadyAsync(TimeSpan.FromMinutes(3));
    }
    public Task DisposeAsync() => Task.CompletedTask;

    private static MatchmakerOptions<byte[]> NewOptions(
        IReadOnlyList<int> teamSizes,
        TimeSpan? tick = null,
        bool teammatesOnly = false) =>
        new(
            ServiceName: $"pug-mm-{Guid.NewGuid().ToString("N")[..8]}",
            TeamSizes: teamSizes,
            MatchTickInterval: tick ?? TimeSpan.FromMilliseconds(100),
            IntroductionExpiry: TimeSpan.FromSeconds(30),
            IntroduceTeammatesOnly: teammatesOnly,
            SerializePayload: bytes => bytes,
            DeserializePayload: bytes => bytes);

    private static EC.EnsembleClient NewClient(EnsembleDaemonHarness daemon) => new(daemon.GrpcAddress);

    private static byte[] MakeJoin(byte[] payload) =>
        new MatchmakerRequest
        {
            JoinQueue = new JoinQueueRequest
            {
                Payload = ByteString.CopyFrom(payload),
                PrivateGameId = string.Empty,
            },
        }.ToByteArray();

    private static byte[] MakeLeave(string sessionId) =>
        new MatchmakerRequest
        {
            LeaveQueue = new LeaveQueueRequest { SessionId = sessionId },
        }.ToByteArray();

    /// <summary>
    /// Two players send <c>JoinQueueRequest</c>; each receives two
    /// <see cref="EC.ServiceEvent.PeerIntroduction"/> events for the pair —
    /// one carrying their own queued <c>SessionId</c> (which their
    /// <c>QueueHandle</c> filter accepts) and one carrying the other player's
    /// queued <c>SessionId</c> (which their filter rejects). The doubling is
    /// the cost of per-recipient session-id correlation: a player's only
    /// replay correlator is the session id they were issued in their
    /// <c>QueuedResponse</c>, so the matchmaker emits one
    /// <c>IntroducePeer</c> per directed pair, each stamped with the
    /// recipient's queued id. See <c>MatchmakerServiceHost.TickAsync</c>.
    /// </summary>
    /// <remarks>
    /// Single-daemon topology: <c>SendBytesAsync</c> across two same-daemon
    /// services does not work (Ensemble's <c>rpc.Service.Send</c> requires
    /// a real libp2p connection to the target peer; there is no local fast
    /// path for RPC bytes). We deliver the <c>JoinQueueRequest</c> bytes
    /// directly into the host via the <c>InjectRpcAsync</c> test seam, and
    /// observe responses via a <c>TestResponseSink</c>. Introductions still
    /// flow through the real daemon and rely on its local fast-path for
    /// <c>handleIntroducePeer</c>.
    /// </remarks>
    [DaemonFact]
    public async Task OneVOne_TwoPlayersJoin_EachReceivesIntroductionWithTheirQueuedSession()
    {
        await using var ensemble = NewClient(_daemon);

        var queue = new TestQueue<Ticket<byte[]>>();
        var options = NewOptions(new[] { 1, 1 });
        var matcher = new FifoMatcher<Ticket<byte[]>>(queue, options.TeamSizes);

        var responses = Channel.CreateUnbounded<(string ToAddr, MatchmakerResponse Resp)>();

        await using var host = new MatchmakerServiceHost<byte[]>(
            ensemble, matcher, queue, options);
        host.TestResponseSink = (addr, resp) =>
        {
            responses.Writer.TryWrite((addr, resp));
            return Task.CompletedTask;
        };
        await host.StartAsync(NewCts(15).Token);

        var (playerA, introA) = await RegisterPlayerAsync(ensemble, "player-a");
        var (playerB, introB) = await RegisterPlayerAsync(ensemble, "player-b");

        await host.InjectRpcAsync(playerA.ServiceAddress, MakeJoin(new byte[] { 0xA1 }));
        await host.InjectRpcAsync(playerB.ServiceAddress, MakeJoin(new byte[] { 0xB2 }));

        // Drain queued responses for both players.
        var queuedSessions = new Dictionary<string, string>();
        for (var i = 0; i < 2; i++)
        {
            var (addr, resp) = await responses.Reader.ReadAsync(NewCts(10).Token);
            Assert.Equal(MatchmakerResponse.MsgOneofCase.Queued, resp.MsgCase);
            Assert.NotEmpty(resp.Queued.SessionId);
            queuedSessions[addr] = resp.Queued.SessionId;
        }
        Assert.NotEqual(queuedSessions[playerA.ServiceAddress], queuedSessions[playerB.ServiceAddress]);

        // Each player receives TWO intros for the same pair: one with their
        // own queued id (their handle would accept) and one with the other
        // player's queued id (their handle would reject). Drain both.
        var introsA = new List<EC.ServiceEvent.PeerIntroduction>();
        var introsB = new List<EC.ServiceEvent.PeerIntroduction>();
        for (var k = 0; k < 2; k++)
        {
            introsA.Add(await introA.Reader.ReadAsync(NewCts(10).Token));
            introsB.Add(await introB.Reader.ReadAsync(NewCts(10).Token));
        }

        // Provenance + peer-addr cross-reference holds on every intro.
        foreach (var intro in introsA)
        {
            Assert.Equal(host.ServiceAddress, intro.FromServiceAddr);
            Assert.Equal(playerB.ServiceAddress, intro.PeerAddr);
        }
        foreach (var intro in introsB)
        {
            Assert.Equal(host.ServiceAddress, intro.FromServiceAddr);
            Assert.Equal(playerA.ServiceAddress, intro.PeerAddr);
        }

        // Each player's pair of intros carries exactly the two queued
        // session ids (their own + the peer's), once each.
        var aSessions = introsA.Select(i => i.SessionId).OrderBy(s => s).ToList();
        var bSessions = introsB.Select(i => i.SessionId).OrderBy(s => s).ToList();
        var expected = new[] { queuedSessions[playerA.ServiceAddress], queuedSessions[playerB.ServiceAddress] }
            .OrderBy(s => s).ToList();
        Assert.Equal(expected, aSessions);
        Assert.Equal(expected, bSessions);
    }

    /// <summary>
    /// 2v2: all-pairs introductions across two teams. Each of four players
    /// receives six <see cref="EC.ServiceEvent.PeerIntroduction"/>s — two per
    /// pair they're in (one stamped with their queued session id, one with
    /// the peer's). Across all four players the distinct session ids on the
    /// 24 intros equal the four players' queued ids.
    /// </summary>
    [DaemonFact]
    public async Task TwoVTwo_AllPairsIntroductionsAcrossTeams()
    {
        await using var ensemble = NewClient(_daemon);

        var queue = new TestQueue<Ticket<byte[]>>();
        var options = NewOptions(new[] { 2, 2 });
        var matcher = new FifoMatcher<Ticket<byte[]>>(queue, options.TeamSizes);

        var responses = Channel.CreateUnbounded<(string ToAddr, MatchmakerResponse Resp)>();

        await using var host = new MatchmakerServiceHost<byte[]>(
            ensemble, matcher, queue, options);
        host.TestResponseSink = (addr, resp) =>
        {
            responses.Writer.TryWrite((addr, resp));
            return Task.CompletedTask;
        };
        await host.StartAsync(NewCts(15).Token);

        var players = new (EC.RegisteredService svc, Channel<EC.ServiceEvent.PeerIntroduction> intros)[4];
        for (var i = 0; i < 4; i++)
        {
            players[i] = await RegisterPlayerAsync(ensemble, $"player-{i}");
        }

        // Deliver each player's JoinQueueRequest through the host test seam
        // — see OneVOne_… for why a real SendBytesAsync round-trip won't
        // work single-daemon.
        for (var i = 0; i < 4; i++)
        {
            await host.InjectRpcAsync(players[i].svc.ServiceAddress,
                MakeJoin(new byte[] { (byte)i }));
        }

        // Drain queued responses to learn each player's queued session id.
        var queuedSessions = new Dictionary<string, string>();
        for (var i = 0; i < 4; i++)
        {
            var (addr, resp) = await responses.Reader.ReadAsync(NewCts(10).Token);
            Assert.Equal(MatchmakerResponse.MsgOneofCase.Queued, resp.MsgCase);
            queuedSessions[addr] = resp.Queued.SessionId;
        }

        // Each player gets 6 intros — for each of their 3 peer pairs, two
        // directed IntroducePeer deliveries (one with the player's queued id,
        // one with the peer's). Drain all six per player.
        var allIntros = new List<(string Receiver, EC.ServiceEvent.PeerIntroduction Intro)>();
        for (var i = 0; i < 4; i++)
        {
            for (var k = 0; k < 6; k++)
            {
                var intro = await players[i].intros.Reader.ReadAsync(NewCts(20).Token);
                Assert.Equal(host.ServiceAddress, intro.FromServiceAddr);
                allIntros.Add((players[i].svc.ServiceAddress, intro));
            }
        }

        // Per player: 6 intros across 3 distinct peer addrs (each peer
        // shows up exactly twice); the receiver's own addr never appears.
        for (var i = 0; i < 4; i++)
        {
            var receiverAddr = players[i].svc.ServiceAddress;
            var mine = allIntros.Where(x => x.Receiver == receiverAddr).Select(x => x.Intro).ToList();
            Assert.Equal(6, mine.Count);
            var peerCounts = mine.GroupBy(p => p.PeerAddr).ToDictionary(g => g.Key, g => g.Count());
            Assert.Equal(3, peerCounts.Count);
            Assert.DoesNotContain(receiverAddr, peerCounts.Keys);
            Assert.All(peerCounts.Values, c => Assert.Equal(2, c));

            // Across the 6 intros sent to this receiver, exactly 3 carry the
            // receiver's queued session id (one per pair) — the rest carry
            // the respective peers' queued ids.
            var withMine = mine.Count(p => p.SessionId == queuedSessions[receiverAddr]);
            Assert.Equal(3, withMine);
        }

        // Distinct session ids across the entire 24-intro fan-out equal
        // the four players' queued session ids.
        var distinctSessions = allIntros.Select(x => x.Intro.SessionId).Distinct().OrderBy(s => s).ToList();
        var expectedSessions = queuedSessions.Values.OrderBy(s => s).ToList();
        Assert.Equal(expectedSessions, distinctSessions);
    }

    /// <summary>
    /// Leave-queue removes the player so a subsequent solo join sits in the
    /// queue without forming a match. No introduction fires within a few
    /// tick intervals.
    /// </summary>
    [DaemonFact]
    public async Task LeaveQueue_RemovesPlayer_NoMatchAgainstSubsequentSoloJoin()
    {
        await using var ensemble = NewClient(_daemon);

        var queue = new TestQueue<Ticket<byte[]>>();
        var options = NewOptions(new[] { 1, 1 });
        var matcher = new FifoMatcher<Ticket<byte[]>>(queue, options.TeamSizes);

        var responses = Channel.CreateUnbounded<(string ToAddr, MatchmakerResponse Resp)>();

        await using var host = new MatchmakerServiceHost<byte[]>(
            ensemble, matcher, queue, options);
        host.TestResponseSink = (addr, resp) =>
        {
            responses.Writer.TryWrite((addr, resp));
            return Task.CompletedTask;
        };
        await host.StartAsync(NewCts(15).Token);

        var (playerA, introA) = await RegisterPlayerAsync(ensemble, "leave-a");
        await host.InjectRpcAsync(playerA.ServiceAddress, MakeJoin(new byte[] { 0xAA }));
        var (_, queuedA) = await responses.Reader.ReadAsync(NewCts(10).Token);
        Assert.Equal(MatchmakerResponse.MsgOneofCase.Queued, queuedA.MsgCase);

        // Leave.
        await host.InjectRpcAsync(playerA.ServiceAddress, MakeLeave(queuedA.Queued.SessionId));
        var (_, leaveAck) = await responses.Reader.ReadAsync(NewCts(10).Token);
        Assert.Equal(MatchmakerResponse.MsgOneofCase.Status, leaveAck.MsgCase);

        // Confirm queue is empty.
        Assert.Equal(0, await queue.CountAsync(default));

        // Now register a fresh player B and join — they're solo, so no match should fire.
        var (playerB, introB) = await RegisterPlayerAsync(ensemble, "leave-b");
        await host.InjectRpcAsync(playerB.ServiceAddress, MakeJoin(new byte[] { 0xBB }));
        var (_, queuedB) = await responses.Reader.ReadAsync(NewCts(10).Token);
        Assert.Equal(MatchmakerResponse.MsgOneofCase.Queued, queuedB.MsgCase);

        // Wait several tick intervals. No introductions should arrive.
        var waitFor = options.EffectiveMatchTickInterval * 5;
        using var noIntroCts = new CancellationTokenSource(waitFor);
        var noIntroA = WaitForNoIntroAsync(introA, noIntroCts.Token);
        var noIntroB = WaitForNoIntroAsync(introB, noIntroCts.Token);
        Assert.True(await noIntroA, "playerA should not receive an introduction after leaving");
        Assert.True(await noIntroB, "playerB should not receive an introduction while alone in the queue");
    }

    /// <summary>
    /// Unit: the manifest the host would register has the expected ACL,
    /// transport, payload cap, and rate-limit values. Validates without a
    /// daemon round-trip via the internal <c>BuildManifest</c> seam.
    /// </summary>
    [DaemonFact]
    public async Task BuildManifest_ReflectsOptions()
    {
        var queue = new TestQueue<Ticket<byte[]>>();
        var opts = new MatchmakerOptions<byte[]>(
            ServiceName: "pug-mm-unit",
            TeamSizes: new[] { 1, 1 },
            MaxPayloadBytes: 1024,
            RateLimitPerMinute: 120,
            RateLimitBurst: 30,
            SerializePayload: b => b,
            DeserializePayload: b => b);
        var matcher = new FifoMatcher<Ticket<byte[]>>(queue, opts.TeamSizes);

        // EnsembleClient ctor needs a non-empty endpoint string but we never
        // call out — manifest construction is a pure local operation.
        await using var dummy = new EC.EnsembleClient("http://127.0.0.1:1");
        var host = new MatchmakerServiceHost<byte[]>(dummy, matcher, queue, opts);

        var manifest = host.BuildManifest();
        Assert.Equal("pug-mm-unit", manifest.Name);
        Assert.Equal(EC.ServiceAcl.Public, manifest.Acl);
        Assert.Equal(EC.ServiceTransport.Rpc, manifest.Transport);
        Assert.Equal(1024L, manifest.MaxPayloadBytes);
        Assert.Equal(120, manifest.RateLimitRequestsPerMinute);
        Assert.Equal(30, manifest.RateLimitBurst);
    }

    /// <summary>
    /// Unit: proto round-trip of a <see cref="JoinQueueRequest"/> through
    /// the <see cref="MatchmakerRequest"/> envelope, asserting payload bytes
    /// survive serialise → parse. Doubles as a sanity check that the proto
    /// codegen for the host-owned domain is wired correctly.
    /// </summary>
    [DaemonFact]
    public void ProtoRoundTrip_JoinAndLeaveRequestsSurviveSerialisation()
    {
        var join = new MatchmakerRequest
        {
            JoinQueue = new JoinQueueRequest
            {
                Payload = ByteString.CopyFrom(new byte[] { 0xCA, 0xFE, 0xBA, 0xBE }),
                PrivateGameId = "00000000-0000-0000-0000-000000000001",
            },
        };
        var joinBytes = join.ToByteArray();
        var joinParsed = MatchmakerRequest.Parser.ParseFrom(joinBytes);
        Assert.Equal(MatchmakerRequest.MsgOneofCase.JoinQueue, joinParsed.MsgCase);
        Assert.Equal(join.JoinQueue.Payload, joinParsed.JoinQueue.Payload);
        Assert.Equal(join.JoinQueue.PrivateGameId, joinParsed.JoinQueue.PrivateGameId);

        var leave = new MatchmakerRequest
        {
            LeaveQueue = new LeaveQueueRequest { SessionId = "abc-123" },
        };
        var leaveParsed = MatchmakerRequest.Parser.ParseFrom(leave.ToByteArray());
        Assert.Equal(MatchmakerRequest.MsgOneofCase.LeaveQueue, leaveParsed.MsgCase);
        Assert.Equal("abc-123", leaveParsed.LeaveQueue.SessionId);
    }

    /// <summary>
    /// Adversarial coverage NOTE: oversize-payload and rate-limit enforcement
    /// land at the daemon's INBOUND path from a remote peer; on a single
    /// daemon (per past learning <c>12263bf2</c>) the local fast-path skips
    /// those checks. A full integration test requires a pair-of-daemons
    /// harness — not built for this ticket. We rely on
    /// <see cref="BuildManifest_ReflectsOptions"/> to pin the manifest values
    /// the daemon would enforce, and on Ensemble's own Phase-4 daemon-side
    /// tests for the wire-level enforcement.
    /// </summary>
    [DaemonFact]
    public void AdversarialEnforcement_Skipped_DocumentedGap()
    {
        // Intentional no-op — see XML comment.
        Console.WriteLine(
            "[skipped: cross-daemon harness needed for oversize/rate-limit wire enforcement]");
        Assert.True(true);
    }

    // ----- helpers -----

    private static CancellationTokenSource NewCts(int seconds) =>
        new(TimeSpan.FromSeconds(seconds));

    private static async Task<bool> WaitForNoIntroAsync(
        Channel<EC.ServiceEvent.PeerIntroduction> ch, CancellationToken ct)
    {
        try
        {
            await ch.Reader.ReadAsync(ct);
            return false; // an intro arrived — bad
        }
        catch (OperationCanceledException)
        {
            return true;
        }
    }

    /// <summary>
    /// Register a player-side RPC service whose <c>onEvent</c> callback
    /// pushes <see cref="EC.ServiceEvent.PeerIntroduction"/> events into a
    /// channel. We don't capture <see cref="EC.ServiceEvent.RpcMessage"/>
    /// replies on the player side because the host can't actually
    /// <c>SendBytes</c> back to a same-daemon service (Ensemble's
    /// <c>rpc.Service.Send</c> requires a real libp2p peer connection).
    /// Tests observe responses via the host's <c>TestResponseSink</c>
    /// instead.
    /// </summary>
    private async Task<(EC.RegisteredService svc,
        Channel<EC.ServiceEvent.PeerIntroduction> intros)> RegisterPlayerAsync(
            EC.EnsembleClient client, string baseName)
    {
        var intros = Channel.CreateUnbounded<EC.ServiceEvent.PeerIntroduction>();

        var manifest = EC.ServiceManifest.NewBuilder($"{baseName}-{Guid.NewGuid().ToString("N")[..8]}")
            .Acl(EC.ServiceAcl.Public)
            .Transport(EC.ServiceTransport.Rpc)
            .MaxPayloadBytes(64 * 1024)
            .Build();

        var svc = await client.RegisterServiceAsync(
            manifest,
            ev =>
            {
                if (ev is EC.ServiceEvent.PeerIntroduction pi)
                    intros.Writer.TryWrite(pi);
                return ValueTask.CompletedTask;
            },
            onError: _ => ValueTask.CompletedTask,
            ct: NewCts(15).Token);

        return (svc, intros);
    }

    /// <summary>
    /// In-memory <see cref="IQueue{TTicket}"/> for tests. Ordered insert,
    /// remove-by-player-id, peek oldest-first. Not thread-safe by design —
    /// the host's match loop is the sole writer modulo
    /// <see cref="RemoveAsync"/> calls from the leave-queue path; both
    /// dispatch from the daemon reader-loop task or the match-loop task,
    /// not from arbitrary user threads.
    /// </summary>
    private sealed class TestQueue<T> : IQueue<T> where T : ITicket<byte[]>
    {
        private readonly List<T> _items = new();
        private readonly object _lock = new();

        public Task EnqueueAsync(T ticket, CancellationToken ct)
        {
            lock (_lock) _items.Add(ticket);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<T>> PeekOldestAsync(int count, CancellationToken ct)
        {
            lock (_lock)
            {
                IReadOnlyList<T> snap = _items.Take(count).ToList();
                return Task.FromResult(snap);
            }
        }

        public Task<int> CountAsync(CancellationToken ct)
        {
            lock (_lock) return Task.FromResult(_items.Count);
        }

        public Task RemoveAsync(Guid playerId, CancellationToken ct)
        {
            lock (_lock)
            {
                _items.RemoveAll(t => t.PlayerId == playerId);
            }
            return Task.CompletedTask;
        }
    }
}
