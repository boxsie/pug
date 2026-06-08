namespace PUG.Netcode.Tests;

/// <summary>
/// Wave-3 substrate end-to-end: this is the "actually try it" test. It wires the
/// whole tier-A substrate together over a single link — a <see cref="TickClock"/>
/// driving a fixed-step loop, a <see cref="ChannelMux"/> carrying a KeepLatest
/// snapshot channel and an Ordered event channel each step, and
/// <see cref="NetDiagnostics"/> observing the receiver — and proves the pieces
/// compose. There's no entity/snapshot model yet (that's Tier B), so this asserts
/// the <i>plumbing</i>: stepped sends arrive, ordered events keep their order, the
/// latest snapshot lands, and diagnostics sees it all.
/// </summary>
public class SubstrateIntegrationTests
{
    private const byte SnapshotChannel = 1; // KeepLatest — newest wins
    private const byte EventChannel = 2;    // Ordered — every one, in order

    [Fact]
    public async Task ClockDrivenSends_ArriveSequenced_AndAreObservedByDiagnostics()
    {
        var specs = new[]
        {
            new ChannelSpec(SnapshotChannel, ChannelMode.KeepLatest),
            new ChannelSpec(EventChannel, ChannelMode.Ordered),
        };

        // Ensemble's profile: reliable, but unordered across sends — the mux resequences.
        var (clientLink, serverLink) = FakePeerLink.CreatePair(
            new FakeLinkOptions { Capabilities = new PeerLinkCapabilities(PeerLinkGuarantees.Reliable) });

        await using var clientMux = new ChannelMux(clientLink, specs);
        await using var serverMux = new ChannelMux(serverLink, specs);

        var serverDiag = new NetDiagnostics();
        var logs = new List<string>();
        serverDiag.LogSink = (level, message) =>
        {
            lock (logs)
            {
                logs.Add($"{level}:{message}");
            }
        };
        serverDiag.RegisterMux("client→server", serverMux);
        serverDiag.Info("session up");

        // The client pumps a 60 Hz clock and emits one snapshot + one event per
        // fixed step. Feeding 3·dt per "frame" over 10 frames yields exactly 30
        // steps — the clock, not the frame count, decides how many sends happen.
        var clock = new TickClock(tickHz: 60);
        const int frames = 10;
        var sent = 0;
        for (var f = 0; f < frames; f++)
        {
            var steps = clock.Advance(clock.Delta * 3);
            for (var s = 0; s < steps; s++)
            {
                await clientMux.SendAsync(EventChannel, new[] { (byte)sent });
                await clientMux.SendAsync(SnapshotChannel, new[] { (byte)sent });
                sent++;
            }
        }

        Assert.Equal(30, sent);
        Assert.Equal(30ul, clock.CurrentTick);

        // Server: every ordered event arrives, in send order.
        await TestPolling.WaitUntilAsync(
            () => Received(serverMux, EventChannel) >= 30, "30 ordered events");

        var events = new List<ReadOnlyMemory<byte>>();
        serverMux.DrainInto(EventChannel, events);
        Assert.Equal(Enumerable.Range(0, 30).Select(i => (byte)i), events.Select(m => m.Span[0]));

        // Snapshot channel: the freshest value lands (it's the last drained item).
        var snapshots = new List<ReadOnlyMemory<byte>>();
        serverMux.DrainInto(SnapshotChannel, snapshots);
        Assert.NotEmpty(snapshots);
        Assert.Equal(29, snapshots[^1].Span[0]);

        // Diagnostics saw the whole thing through one polled surface + one log sink.
        var snap = serverDiag.Snapshot();
        var mux = Assert.Single(snap.Muxes);
        Assert.Equal("client→server", mux.Label);
        Assert.Equal(30, mux.Stats.Channels.First(c => c.ChannelId == EventChannel).PacketsReceived);
        Assert.Equal(30, mux.Stats.Channels.First(c => c.ChannelId == SnapshotChannel).PacketsReceived);
        Assert.Equal(0, mux.Stats.MalformedPackets);
        Assert.Equal(0, mux.Stats.UnknownChannelPackets);

        lock (logs)
        {
            Assert.Contains("Info:session up", logs);
        }
    }

    private static long Received(ChannelMux mux, byte channelId)
    {
        return mux.Stats.Channels.First(c => c.ChannelId == channelId).PacketsReceived;
    }
}
