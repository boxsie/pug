namespace PUG.Netcode.Tests;

/// <summary>The observability surface: the log-sink seam and the pull-model stats
/// snapshot, exercised over a real <see cref="ChannelMux"/> on a
/// <see cref="FakePeerLink"/>.</summary>
public class NetDiagnosticsTests
{
    private static readonly PeerLinkCapabilities ReliableUnordered = new(PeerLinkGuarantees.Reliable);

    [Fact]
    public void LogSink_DefaultsToNoOp_AndNeverThrows()
    {
        var diag = new NetDiagnostics();

        diag.Info("nobody is listening");
        diag.Log(NetLogLevel.Error, "still fine");
        // No sink wired, no exception — the deliberate void, not a null-ref trap.
    }

    [Fact]
    public void LogSink_ReceivesEmittedEventsWithLevels()
    {
        var diag = new NetDiagnostics();
        var captured = new List<(NetLogLevel Level, string Message)>();
        diag.LogSink = (level, message) => captured.Add((level, message));

        diag.Trace("t");
        diag.Debug("d");
        diag.Info("i");
        diag.Warn("w");
        diag.Error("e");
        diag.Log(NetLogLevel.Info, "direct");

        Assert.Equal(
            new[]
            {
                (NetLogLevel.Trace, "t"),
                (NetLogLevel.Debug, "d"),
                (NetLogLevel.Info, "i"),
                (NetLogLevel.Warn, "w"),
                (NetLogLevel.Error, "e"),
                (NetLogLevel.Info, "direct"),
            },
            captured);
    }

    [Fact]
    public void LogSink_SetToNull_RestoresNoOp()
    {
        var diag = new NetDiagnostics { LogSink = (_, _) => throw new InvalidOperationException("should be replaced") };

        diag.LogSink = null!;

        diag.Info("safe again"); // would throw if the old sink were still wired
    }

    [Fact]
    public async Task Snapshot_ReflectsTrafficThroughARegisteredMux()
    {
        const byte ch = 1;
        var (sendLink, recvLink) = FakePeerLink.CreatePair(new FakeLinkOptions { Capabilities = ReliableUnordered });
        await using var sender = new ChannelMux(sendLink, new[] { new ChannelSpec(ch, ChannelMode.Unreliable) });
        await using var receiver = new ChannelMux(recvLink, new[] { new ChannelSpec(ch, ChannelMode.Unreliable) });

        var diag = new NetDiagnostics();
        diag.RegisterMux("peer-A", receiver);

        for (byte i = 0; i < 4; i++)
        {
            await sender.SendAsync(ch, new byte[] { i, i });
        }

        await TestPolling.WaitUntilAsync(
            () => diag.Snapshot().Muxes[0].Stats.Channels[0].PacketsReceived >= 4, "4 packets received");

        var snap = diag.Snapshot();
        var mux = Assert.Single(snap.Muxes);
        Assert.Equal("peer-A", mux.Label);
        var chStats = Assert.Single(mux.Stats.Channels);
        Assert.Equal(ch, chStats.ChannelId);
        Assert.Equal(4, chStats.PacketsReceived);
        Assert.Equal(4 * (ChannelMux.HeaderBytes + 2), chStats.BytesReceived); // 3-byte header + 2 payload
    }

    [Fact]
    public async Task Describe_FormatsLabelAndChannelCounters()
    {
        const byte ch = 5;
        var (sendLink, recvLink) = FakePeerLink.CreatePair(new FakeLinkOptions { Capabilities = ReliableUnordered });
        await using var sender = new ChannelMux(sendLink, new[] { new ChannelSpec(ch, ChannelMode.KeepLatest) });
        await using var receiver = new ChannelMux(recvLink, new[] { new ChannelSpec(ch, ChannelMode.KeepLatest) });

        var diag = new NetDiagnostics();
        diag.RegisterMux("client", receiver);
        await sender.SendAsync(ch, new byte[] { 1 });

        await TestPolling.WaitUntilAsync(
            () => diag.Snapshot().Muxes[0].Stats.Channels[0].PacketsReceived >= 1, "1 received");

        var dump = diag.Describe();

        Assert.Contains("client", dump);
        Assert.Contains("ch5", dump);
        Assert.Contains("KeepLatest", dump);
    }

    [Fact]
    public async Task RegisterMux_RejectsBlankLabelOrNullMux()
    {
        var diag = new NetDiagnostics();
        var (link, _) = FakePeerLink.CreatePair();
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec((byte)1, ChannelMode.Unreliable) });

        Assert.Throws<ArgumentException>(() => diag.RegisterMux("  ", mux));
        Assert.Throws<ArgumentNullException>(() => diag.RegisterMux("ok", null!));
    }

    [Fact]
    public async Task Snapshot_ListsMultipleMuxesInRegistrationOrder()
    {
        var diag = new NetDiagnostics();
        var (a, b) = FakePeerLink.CreatePair();
        await using var muxA = new ChannelMux(a, new[] { new ChannelSpec((byte)1, ChannelMode.Unreliable) });
        await using var muxB = new ChannelMux(b, new[] { new ChannelSpec((byte)1, ChannelMode.Unreliable) });

        diag.RegisterMux("first", muxA);
        diag.RegisterMux("second", muxB);

        var labels = diag.Snapshot().Muxes.Select(m => m.Label).ToArray();
        Assert.Equal(new[] { "first", "second" }, labels);
    }
}
