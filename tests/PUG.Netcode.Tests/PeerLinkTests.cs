namespace PUG.Netcode.Tests;

public class PeerLinkCapabilitiesTests
{
    [Fact]
    public void ReliableOrderedStream_ReportsBothGuarantees_NoPayloadCap()
    {
        var caps = PeerLinkCapabilities.ReliableOrderedStream();

        Assert.True(caps.IsReliable);
        Assert.True(caps.IsOrdered);
        Assert.True(caps.IsReliableOrdered);
        Assert.Null(caps.MaxPayloadBytes);
    }

    [Fact]
    public void ReliableOrderedStream_CanCarryAPayloadCeiling()
    {
        // Models a reliable transport that still enforces a per-message limit
        // (e.g. Ensemble's RPC payload cap).
        var caps = PeerLinkCapabilities.ReliableOrderedStream(maxPayloadBytes: 65_536);

        Assert.True(caps.IsReliableOrdered);
        Assert.Equal(65_536, caps.MaxPayloadBytes);
    }

    [Fact]
    public void UnreliableDatagram_ReportsNoGuarantees_WithMtu()
    {
        var caps = PeerLinkCapabilities.UnreliableDatagram(maxPayloadBytes: 1200);

        Assert.False(caps.IsReliable);
        Assert.False(caps.IsOrdered);
        Assert.False(caps.IsReliableOrdered);
        Assert.Equal(1200, caps.MaxPayloadBytes);
    }
}

public class FakePeerLinkTests
{
    // Generous timeout: a present payload returns instantly on a zero-latency
    // link; the timeout only bounds the "nothing should arrive" (loss) cases.
    private static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public void Capabilities_AreSurfacedThroughTheLink()
    {
        var caps = PeerLinkCapabilities.UnreliableDatagram(1200);
        var (a, b) = FakePeerLink.CreatePair(new FakeLinkOptions { Capabilities = caps });

        Assert.Equal(caps, a.Capabilities);
        Assert.Equal(caps, b.Capabilities);
    }

    [Fact]
    public async Task Send_RoundTripsToPeer_BothDirections()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var disposeA = a;
        await using var disposeB = b;

        await a.SendAsync(new byte[] { 1, 2, 3 });
        Assert.Equal(new byte[] { 1, 2, 3 }, await ReadOneAsync(b, ReadTimeout));

        await b.SendAsync(new byte[] { 9, 8 });
        Assert.Equal(new byte[] { 9, 8 }, await ReadOneAsync(a, ReadTimeout));
    }

    [Fact]
    public async Task Send_CopiesPayload_CallerMayReuseBuffer()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var disposeA = a;
        await using var disposeB = b;

        var buffer = new byte[] { 1, 2, 3 };
        await a.SendAsync(buffer);
        buffer[0] = 99; // mutate after send — must not affect what the peer sees

        Assert.Equal(new byte[] { 1, 2, 3 }, await ReadOneAsync(b, ReadTimeout));
    }

    [Fact]
    public async Task Send_DefaultLink_PreservesOrder()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var disposeA = a;
        await using var disposeB = b;

        for (var i = 0; i < 10; i++)
            await a.SendAsync(new[] { (byte)i });

        var got = await ReadManyAsync(b, count: 10, ReadTimeout);
        Assert.Equal(Enumerable.Range(0, 10).Select(i => (byte)i), got.Select(p => p[0]));
    }

    [Fact]
    public async Task Send_FullLoss_DeliversNothing()
    {
        var (a, b) = FakePeerLink.CreatePair(new FakeLinkOptions
        {
            LossRate = 1.0,
            Capabilities = PeerLinkCapabilities.UnreliableDatagram(1200),
        });
        await using var disposeA = a;
        await using var disposeB = b;

        await a.SendAsync(new byte[] { 42 });

        // Short bound — we're asserting absence; nothing should ever arrive.
        Assert.Null(await ReadOneAsync(b, TimeSpan.FromMilliseconds(250)));
    }

    [Fact]
    public async Task Send_WithLatencyAndJitter_DeliversEveryPayload_OrderAside()
    {
        var (a, b) = FakePeerLink.CreatePair(new FakeLinkOptions
        {
            Latency = TimeSpan.FromMilliseconds(5),
            Jitter = TimeSpan.FromMilliseconds(40), // ⇒ reordering
            Capabilities = PeerLinkCapabilities.UnreliableDatagram(1200),
        });
        await using var disposeA = a;
        await using var disposeB = b;

        for (var i = 0; i < 20; i++)
            await a.SendAsync(new[] { (byte)i });

        var got = await ReadManyAsync(b, count: 20, TimeSpan.FromSeconds(5));

        // The fake may reorder under jitter, but it must not lose data with
        // LossRate=0. Assert on set membership, not sequence.
        Assert.Equal(
            Enumerable.Range(0, 20).Select(i => (byte)i).OrderBy(x => x),
            got.Select(p => p[0]).OrderBy(x => x));
    }

    [Fact]
    public async Task Dispose_CompletesTheReceiveStream()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var disposeA = a;

        await b.DisposeAsync();

        // Stream completed cleanly → enumeration ends with no item, not a hang.
        Assert.Null(await ReadOneAsync(b, ReadTimeout));
    }

    [Fact]
    public async Task Send_AfterDispose_Throws()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var disposeB = b;
        await a.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await a.SendAsync(new byte[] { 1 }));
    }

    private static async Task<byte[]?> ReadOneAsync(IPeerLink link, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var msg in link.ReceiveAsync(cts.Token).ConfigureAwait(false))
                return msg.ToArray();
        }
        catch (OperationCanceledException) { /* timed out → nothing arrived */ }
        return null;
    }

    private static async Task<List<byte[]>> ReadManyAsync(IPeerLink link, int count, TimeSpan timeout)
    {
        var items = new List<byte[]>(count);
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var msg in link.ReceiveAsync(cts.Token).ConfigureAwait(false))
            {
                items.Add(msg.ToArray());
                if (items.Count >= count) break;
            }
        }
        catch (OperationCanceledException) { /* timed out → return what we got */ }
        return items;
    }
}
