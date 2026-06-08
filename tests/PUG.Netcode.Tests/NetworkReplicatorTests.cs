using System.Buffers;
using System.Buffers.Binary;

namespace PUG.Netcode.Tests;

/// <summary>
/// Tier B1 entity state replication: two <see cref="NetworkReplicator"/>s over a
/// <see cref="FakePeerLink"/> + the A1 KeepLatest channel. Proves full-world
/// snapshots reconstruct authority→client by set-diff (spawn / apply / despawn),
/// self-heal through loss, and surface snapshot-age in diagnostics.
/// </summary>
public class NetworkReplicatorTests
{
    private const byte Snap = 1;
    private static readonly ChannelSpec[] Specs = { new(Snap, ChannelMode.KeepLatest) };

    [Fact]
    public async Task Client_SpawnsTheRightKinds_AndStateRoundTrips()
    {
        await using var harness = Harness.Create();

        var paddle = new FakeEntity(kind: 10) { X = 100, Y = -50 };
        var ball = new FakeEntity(kind: 20) { X = 7, Y = 7 };
        var paddleId = harness.Authority.Register(paddle);
        var ballId = harness.Authority.Register(ball);

        await harness.Authority.CaptureAndBroadcastAsync(tick: 5);
        await harness.PumpUntil(() => harness.Client.Entities.Count == 2, "both entities replicated");

        var replicatedPaddle = (FakeEntity)harness.Client.Entities[paddleId].State;
        var replicatedBall = (FakeEntity)harness.Client.Entities[ballId].State;

        Assert.Equal(10, harness.Client.Entities[paddleId].Kind);
        Assert.Equal(20, harness.Client.Entities[ballId].Kind);
        Assert.Equal((100, -50), (replicatedPaddle.X, replicatedPaddle.Y));
        Assert.Equal((7, 7), (replicatedBall.X, replicatedBall.Y));

        // owner is reserved in B1 — every entity is authority-owned (PeerId 0).
        Assert.Equal(PeerId.Authority, harness.Client.Entities[paddleId].Owner);
    }

    [Fact]
    public async Task EntityDespawnedOnAuthority_VanishesOnClient_ViaSetDiff()
    {
        var despawned = new List<ushort>();
        await using var harness = Harness.Create(onDespawn: (id, _) => despawned.Add(id));

        var keepId = harness.Authority.Register(new FakeEntity(1) { X = 1 });
        var goneId = harness.Authority.Register(new FakeEntity(2) { X = 2 });
        await harness.Authority.CaptureAndBroadcastAsync(tick: 1);
        await harness.PumpUntil(() => harness.Client.Entities.Count == 2, "both spawned");

        Assert.True(harness.Authority.Despawn(goneId));
        await harness.Authority.CaptureAndBroadcastAsync(tick: 2);
        await harness.PumpUntil(() => harness.Client.Entities.Count == 1, "one despawned");

        Assert.True(harness.Client.Entities.ContainsKey(keepId));
        Assert.False(harness.Client.Entities.ContainsKey(goneId));
        Assert.Equal(new[] { goneId }, despawned); // set-diff fired the despawn callback
    }

    [Fact]
    public async Task World_SelfHealsThroughLoss_ToTheNewestFullSnapshot()
    {
        // 50% loss — the spawn snapshot itself may be dropped repeatedly. Because
        // every snapshot is a full world, re-sending eventually reconstructs it:
        // there is no "spawn lost ⇒ ghost forever" failure mode to recover from.
        await using var harness = Harness.Create(new FakeLinkOptions
        {
            LossRate = 0.5,
            Seed = 1,
            Capabilities = new PeerLinkCapabilities(PeerLinkGuarantees.None),
        });

        var entity = new FakeEntity(kind: 7) { X = 999, Y = 111 };
        var id = harness.Authority.Register(entity);

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        var healed = false;
        while (!healed && DateTime.UtcNow < deadline)
        {
            await harness.Authority.CaptureAndBroadcastAsync(tick: 100);
            await Task.Delay(5);
            harness.Client.Apply();
            healed = harness.Client.Entities.TryGetValue(id, out var re)
                && ((FakeEntity)re.State) is { X: 999, Y: 111 };
        }

        Assert.True(healed, "repeated full snapshots reconstructed the world despite 50% loss");
    }

    [Fact]
    public async Task SnapshotAge_IsTrackedAndSurfacedThroughDiagnostics()
    {
        var diag = new NetDiagnostics();
        // Local clock sits 5 ticks ahead of the snapshot we'll send.
        await using var harness = Harness.Create(localTick: () => 15);

        harness.Authority.Register(new FakeEntity(1) { X = 1 });
        await harness.Authority.CaptureAndBroadcastAsync(tick: 10);
        await harness.PumpUntil(() => harness.Client.Entities.Count == 1, "entity replicated");

        Assert.Equal(5, harness.Client.Stats.SnapshotAgeTicks); // 15 − 10

        diag.RegisterReplicator("world", harness.Client);
        var rep = Assert.Single(diag.Snapshot().Replicators);
        Assert.Equal("world", rep.Label);
        Assert.False(rep.Stats.IsAuthority);
        Assert.Equal(5, rep.Stats.SnapshotAgeTicks);
        Assert.Equal(10u, rep.Stats.LastSnapshotTick);
        Assert.Contains("replicator", diag.Describe());
    }

    [Fact]
    public async Task RoleGuards_RejectCrossRoleCalls()
    {
        await using var harness = Harness.Create();

        // Client can't register; authority can't apply.
        Assert.Throws<InvalidOperationException>(() => harness.Client.Register(new FakeEntity(1)));
        Assert.Throws<InvalidOperationException>(() => harness.Authority.Apply());

        // And the factories insist on the matching session role.
        var (link, _) = FakePeerLink.CreatePair();
        await using var clientSession = NetSession.CreateClient(link, Specs, new PeerId(1));
        Assert.Throws<ArgumentException>(() => NetworkReplicator.CreateAuthority(clientSession, Snap));
    }

    /// <summary>A minimal quantized entity: a kind + two int16 coordinates.</summary>
    private sealed class FakeEntity : INetEntityState
    {
        public FakeEntity(byte kind) => Kind = kind;

        public byte Kind { get; }
        public short X { get; set; }
        public short Y { get; set; }

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(4);
            BinaryPrimitives.WriteInt16BigEndian(span, X);
            BinaryPrimitives.WriteInt16BigEndian(span.Slice(2), Y);
            writer.Advance(4);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            X = BinaryPrimitives.ReadInt16BigEndian(state);
            Y = BinaryPrimitives.ReadInt16BigEndian(state.Slice(2));
        }
    }

    /// <summary>Wires an authority + client replicator over a FakePeerLink pair and
    /// owns their sessions' disposal.</summary>
    private sealed class Harness : IAsyncDisposable
    {
        private readonly NetSession _authoritySession;
        private readonly NetSession _clientSession;

        private Harness(NetSession authoritySession, NetSession clientSession, NetworkReplicator authority, NetworkReplicator client)
        {
            _authoritySession = authoritySession;
            _clientSession = clientSession;
            Authority = authority;
            Client = client;
        }

        public NetworkReplicator Authority { get; }
        public NetworkReplicator Client { get; }

        public static Harness Create(
            FakeLinkOptions? link = null,
            Action<ushort, INetEntityState>? onDespawn = null,
            Func<uint>? localTick = null)
        {
            var (a, b) = FakePeerLink.CreatePair(link);
            var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
            var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
            var authority = NetworkReplicator.CreateAuthority(authoritySession, Snap);
            var client = NetworkReplicator.CreateClient(
                clientSession, Snap, kind => new FakeEntity(kind), onDespawn: onDespawn, localTick: localTick);
            return new Harness(authoritySession, clientSession, authority, client);
        }

        /// <summary>Pump the client (drain + apply) until <paramref name="done"/>,
        /// racing-free: the predicate itself applies, so it only sees what landed.</summary>
        public Task PumpUntil(Func<bool> done, string what) =>
            TestPolling.WaitUntilAsync(() => { Client.Apply(); return done(); }, what);

        public async ValueTask DisposeAsync()
        {
            await _authoritySession.DisposeAsync();
            await _clientSession.DisposeAsync();
        }
    }
}
