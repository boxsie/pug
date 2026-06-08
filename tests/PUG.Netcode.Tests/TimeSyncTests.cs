namespace PUG.Netcode.Tests;

/// <summary>
/// RTT + tick-offset estimation over a link. The RTT test rides real injected
/// latency on a <see cref="FakePeerLink"/>; the offset test freezes the timestamp
/// source so the tick math is exact; and the smoothing test drives the
/// <see cref="RttWindow"/> directly to prove the sliding minimum ignores a spike.
/// </summary>
public class TimeSyncTests
{
    private static readonly ChannelSpec[] SyncChannel = { new(0, ChannelMode.Unreliable) };

    [Fact]
    public async Task Rtt_ConvergesNearTwiceTheOneWayLatency()
    {
        var oneWay = TimeSpan.FromMilliseconds(30);
        var (clientLink, authorityLink) = FakePeerLink.CreatePair(new FakeLinkOptions { Latency = oneWay });
        await using var clientMux = new ChannelMux(clientLink, SyncChannel);
        await using var authorityMux = new ChannelMux(authorityLink, SyncChannel);

        var client = new TimeSync(clientMux, 0, new TickClock(60), new TimeSyncOptions { PingInterval = TimeSpan.FromMilliseconds(10) });
        var authority = new TimeSync(authorityMux, 0, new TickClock(60), new TimeSyncOptions { AutoPing = false });

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (client.SampleCount < 6 && DateTime.UtcNow < deadline)
        {
            await client.UpdateAsync();
            await authority.UpdateAsync();
            await Task.Delay(5);
        }

        Assert.True(client.SampleCount >= 6, $"only got {client.SampleCount} samples");
        // ~2×30 = 60ms; Task.Delay never returns early so the floor is firm, the
        // ceiling generous for scheduler slack. The sliding-min keeps it near 60.
        Assert.InRange(client.Rtt.TotalMilliseconds, 40, 160);
    }

    [Fact]
    public async Task Offset_RecoversAFixedTickSkew()
    {
        var (clientLink, authorityLink) = FakePeerLink.CreatePair();
        await using var clientMux = new ChannelMux(clientLink, SyncChannel);
        await using var authorityMux = new ChannelMux(authorityLink, SyncChannel);

        var clientClock = new TickClock(60);
        var authorityClock = new TickClock(60);
        const int skew = 100;
        // Step it tick-by-tick: one big Advance would hit the spiral-of-death cap
        // and drop the backlog. The authority ends up 100 ticks ahead of the client.
        for (var i = 0; i < skew; i++)
        {
            authorityClock.Advance(authorityClock.Delta);
        }

        Assert.Equal((ulong)skew, authorityClock.CurrentTick);

        // Freeze time so rtt ≡ 0: the offset is then the raw tick skew, no estimate.
        var frozen = new TimeSyncOptions { AutoPing = false, TimestampTicks = () => 1000, TimestampFrequency = 1 };
        var client = new TimeSync(clientMux, 0, clientClock, frozen);
        var authority = new TimeSync(authorityMux, 0, authorityClock, frozen);

        await client.PingAsync();
        await TestPolling.WaitUntilAsync(() => Received(authorityMux) >= 1, "authority got the ping");
        await authority.UpdateAsync(); // echoes a pong stamped at authority tick 100
        await TestPolling.WaitUntilAsync(() => Received(clientMux) >= 1, "client got the pong");
        await client.UpdateAsync(); // folds it in

        Assert.Equal(1, client.SampleCount);
        Assert.Equal((long)skew, client.TickOffset);
        Assert.Equal(TimeSpan.Zero, client.Rtt);

        // …and it surfaces through diagnostics.
        var diag = new NetDiagnostics();
        diag.RegisterTimeSync("authority", client);
        var ts = Assert.Single(diag.Snapshot().TimeSyncs);
        Assert.Equal("authority", ts.Label);
        Assert.Equal((long)skew, ts.Stats.TickOffset);
        Assert.Contains("timesync", diag.Describe());
    }

    [Fact]
    public void SlidingMin_IgnoresASingleJitterSpike()
    {
        var window = new RttWindow(8);
        for (var i = 0; i < 5; i++)
        {
            window.Push(100, 10);
        }

        window.Push(9000, 999); // a lone latency spike, with a wildly wrong offset

        Assert.Equal(100L, window.BestRttTicks);
        Assert.Equal(10L, window.BestOffsetTicks); // the spike's offset is never chosen

        window.Push(80, 7); // a cleaner sample legitimately wins
        Assert.Equal(80L, window.BestRttTicks);
        Assert.Equal(7L, window.BestOffsetTicks);
    }

    [Fact]
    public void Window_EvictsOldestWhenFull_SoAStaleMinAgesOut()
    {
        var window = new RttWindow(3);
        window.Push(50, 1);
        window.Push(60, 2);
        window.Push(70, 3);
        Assert.Equal(50L, window.BestRttTicks);

        window.Push(80, 4); // evicts the 50 → the min ages out

        Assert.Equal(3, window.Count);
        Assert.Equal(60L, window.BestRttTicks);
        Assert.Equal(2L, window.BestOffsetTicks);
    }

    private static long Received(ChannelMux mux) =>
        mux.Stats.Channels.First(c => c.ChannelId == 0).PacketsReceived;
}
