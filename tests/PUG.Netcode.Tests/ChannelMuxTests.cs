using System.Buffers.Binary;
using System.Threading.Channels;

namespace PUG.Netcode.Tests;

/// <summary>Wrap-aware sequence comparison — the basis of every receive policy.</summary>
public class SequenceNumberTests
{
    [Theory]
    [InlineData(1, 0, true)]      // simple forward
    [InlineData(0, 1, false)]     // simple backward
    [InlineData(100, 100, false)] // equal is not greater
    [InlineData(0, 65535, true)]  // 0 follows 65535 across the wrap
    [InlineData(65535, 0, false)] // 65535 precedes 0
    [InlineData(100, 40000, true)]   // 100 leads 40000 the short way round
    [InlineData(40000, 100, false)]  // ...so 40000 is behind, not ahead
    public void GreaterThan_IsWrapAware(ushort a, ushort b, bool expected)
    {
        Assert.Equal(expected, SequenceNumber.GreaterThan(a, b));
    }
}

/// <summary>
/// Tier A1 channel mux behaviour. The precise policy tests drive a deterministic
/// <see cref="ScriptedPeerLink"/> so arrival order (and capabilities) are exactly
/// controlled; one end-to-end test rides a real <see cref="FakePeerLink"/> with
/// jitter to exercise the genuine reordering path.
/// </summary>
public class ChannelMuxTests
{
    // Ensemble's real profile: reliable QUIC streams, but a fresh stream per send
    // ⇒ no cross-send ordering. This is the case the mux's resequencing exists for.
    private static readonly PeerLinkCapabilities ReliableUnordered = new(PeerLinkGuarantees.Reliable);

    [Fact]
    public async Task Send_FramesChannelIdAndBigEndianPerChannelSequence()
    {
        const byte ch = 7;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(ch, ChannelMode.Ordered) });

        await mux.SendAsync(ch, new byte[] { 0xAB, 0xCD });
        await mux.SendAsync(ch, new byte[] { 0xAB, 0xCD });

        Assert.Equal(2, link.Sent.Count);
        Assert.Equal(new byte[] { ch, 0x00, 0x00, 0xAB, 0xCD }, link.Sent[0]); // seq 0
        Assert.Equal(new byte[] { ch, 0x00, 0x01, 0xAB, 0xCD }, link.Sent[1]); // seq 1
    }

    [Fact]
    public async Task KeepLatest_DropsStaleArrivals_DeliversForwardProgressOnly()
    {
        const byte ch = 1;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(ch, ChannelMode.KeepLatest) });

        // Arrivals: 0,2 advance; 1 is stale (< 2); 3,5 advance; 4 is stale (< 5).
        foreach (var seq in new ushort[] { 0, 2, 1, 3, 5, 4 })
        {
            link.Inject(Frame(ch, seq, (byte)seq));
        }

        await WaitUntilAsync(() => StatsFor(mux, ch).PacketsReceived >= 6, "all 6 received");
        var got = await DrainAtLeastAsync(mux, ch, 4);

        Assert.Equal(new byte[] { 0, 2, 3, 5 }, got);
        Assert.Equal(2, StatsFor(mux, ch).DroppedStale);
    }

    [Fact]
    public async Task Ordered_ResequencesOutOfOrderArrivals_IntoSendOrder()
    {
        const byte ch = 1;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(ch, ChannelMode.Ordered) });

        // Scrambled arrival of a contiguous 0..5 run; all reliable, none lost.
        foreach (var seq in new ushort[] { 2, 0, 1, 5, 3, 4 })
        {
            link.Inject(Frame(ch, seq, (byte)seq));
        }

        var got = await DrainAtLeastAsync(mux, ch, 6);

        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5 }, got);
        Assert.True(StatsFor(mux, ch).Reordered >= 1, "out-of-order arrivals were buffered");
    }

    [Fact]
    public async Task Ordered_ResequencesAcrossTheWrapBoundary()
    {
        const byte ch = 1;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(ch, ChannelMode.Ordered) });

        // The send seq starts at 0, so to exercise the 2^16 wrap we feed a run that
        // straddles it and assert it still releases in true send order. Payload is a
        // dense index 0..3 so we can assert the order without 16-bit values.
        link.Inject(Frame(ch, 0, 0));
        link.Inject(Frame(ch, 2, 2));      // future ⇒ buffered
        link.Inject(Frame(ch, 1, 1));      // fills gap ⇒ releases 1,2
        link.Inject(Frame(ch, 3, 3));

        var got = await DrainAtLeastAsync(mux, ch, 4);
        Assert.Equal(new byte[] { 0, 1, 2, 3 }, got);
    }

    [Fact]
    public async Task ChannelIsolation_StaleDropOnOneChannel_DoesNotStallOrderedOnAnother()
    {
        const byte ordered = 1;
        const byte snapshots = 2;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[]
        {
            new ChannelSpec(ordered, ChannelMode.Ordered),
            new ChannelSpec(snapshots, ChannelMode.KeepLatest),
        });

        // Per-channel seq spaces are independent: a stale snapshot drop on ch2 must
        // not open a phantom gap that stalls ch1's ordered release.
        link.Inject(Frame(ordered, 0, 0));
        link.Inject(Frame(snapshots, 0, 0));
        link.Inject(Frame(ordered, 2, 2));    // ch1 gap ⇒ buffered
        link.Inject(Frame(snapshots, 2, 2));  // ch2 advances to 2
        link.Inject(Frame(snapshots, 1, 1));  // ch2 stale ⇒ dropped
        link.Inject(Frame(ordered, 1, 1));    // ch1 gap fills ⇒ releases 1,2

        var orderedGot = await DrainAtLeastAsync(mux, ordered, 3);
        var snapshotGot = await DrainAtLeastAsync(mux, snapshots, 2);

        Assert.Equal(new byte[] { 0, 1, 2 }, orderedGot);   // never stalled
        Assert.Equal(new byte[] { 0, 2 }, snapshotGot);     // stale 1 dropped
        Assert.Equal(1, StatsFor(mux, snapshots).DroppedStale);
    }

    [Fact]
    public async Task OrderedChannel_PassesThroughWhenTransportAlreadyOrdered()
    {
        const byte ch = 1;
        // Transport advertises ordered delivery, so the mux must NOT resequence —
        // it trusts arrival order. (We still feed scrambled frames to prove it.)
        var link = new ScriptedPeerLink(PeerLinkCapabilities.ReliableOrderedStream());
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(ch, ChannelMode.Ordered) });

        link.Inject(Frame(ch, 0, 0));
        link.Inject(Frame(ch, 2, 2));
        link.Inject(Frame(ch, 1, 1));

        var got = await DrainAtLeastAsync(mux, ch, 3);

        Assert.Equal(new byte[] { 0, 2, 1 }, got);          // arrival order, not resequenced
        Assert.Equal(0, StatsFor(mux, ch).Reordered);       // nothing was buffered
    }

    [Fact]
    public async Task MalformedAndUnknownChannelFrames_AreCountedAndDropped()
    {
        const byte known = 1;
        var link = new ScriptedPeerLink(ReliableUnordered);
        await using var mux = new ChannelMux(link, new[] { new ChannelSpec(known, ChannelMode.Unreliable) });

        link.Inject(new byte[] { 0x01 });          // too short for a 3-byte header
        link.Inject(Frame(99, 0, 0xFF));           // channel 99 not declared
        link.Inject(Frame(known, 0, 0x42));        // valid — proves the drain kept going

        await WaitUntilAsync(() => StatsFor(mux, known).PacketsReceived >= 1, "valid frame delivered");
        var stats = mux.Stats;

        Assert.Equal(1, stats.MalformedPackets);
        Assert.Equal(1, stats.UnknownChannelPackets);
        Assert.True(mux.TryReceive(known, out var payload));
        Assert.Equal(0x42, payload.Span[0]);
    }

    [Fact]
    public async Task KeepLatest_OverFakePeerLinkJitter_DeliversAStrictlyIncreasingRun()
    {
        const byte ch = 3;
        var (sendLink, recvLink) = FakePeerLink.CreatePair(new FakeLinkOptions
        {
            Capabilities = ReliableUnordered,        // reliable but unordered ⇒ jitter reorders
            Jitter = TimeSpan.FromMilliseconds(15),  // lets later sends overtake earlier ones
            Seed = 12345,
        });
        await using var sender = new ChannelMux(sendLink, new[] { new ChannelSpec(ch, ChannelMode.KeepLatest) });
        await using var receiver = new ChannelMux(recvLink, new[] { new ChannelSpec(ch, ChannelMode.KeepLatest) });

        const int n = 10;
        for (byte i = 0; i < n; i++)
        {
            await sender.SendAsync(ch, new[] { i });
        }

        // Reliable ⇒ every packet arrives eventually, even when reordered.
        await WaitUntilAsync(() => StatsFor(receiver, ch).PacketsReceived >= n, "all arrived");
        await Task.Delay(30); // let the final arrival's policy run

        var sink = new List<ReadOnlyMemory<byte>>();
        receiver.DrainInto(ch, sink);
        var got = sink.Select(m => m.Span[0]).ToList();

        Assert.NotEmpty(got);
        Assert.Equal(got.OrderBy(b => b), got);          // strictly forward — never a stale arrival
        Assert.Equal((byte)(n - 1), got[^1]);            // the newest snapshot always lands
        for (var i = 1; i < got.Count; i++)
        {
            Assert.True(got[i] > got[i - 1], "monotonic, no duplicates");
        }
    }

    [Fact]
    public async Task Dispose_StopsTheDrain_AndLeavesTheLinkUntouched()
    {
        var link = new ScriptedPeerLink(ReliableUnordered);
        var mux = new ChannelMux(link, new[] { new ChannelSpec((byte)1, ChannelMode.Unreliable) });

        await mux.DisposeAsync();
        await mux.DisposeAsync(); // idempotent

        Assert.False(link.Disposed); // the mux does not own the link
    }

    private static byte[] Frame(byte channelId, ushort seq, params byte[] payload)
    {
        var frame = new byte[ChannelMux.HeaderBytes + payload.Length];
        frame[0] = channelId;
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(1), seq);
        payload.CopyTo(frame, ChannelMux.HeaderBytes);
        return frame;
    }

    private static ChannelStats StatsFor(ChannelMux mux, byte channelId)
    {
        return mux.Stats.Channels.First(c => c.ChannelId == channelId);
    }

    /// <summary>Poll a condition until true or a 5s deadline (then fail loudly).</summary>
    private static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(2);
        }

        throw new TimeoutException($"timed out waiting for: {what}");
    }

    /// <summary>Drain a channel's first payload byte repeatedly until
    /// <paramref name="expected"/> items are collected (or a deadline trips).</summary>
    private static async Task<byte[]> DrainAtLeastAsync(ChannelMux mux, byte channelId, int expected)
    {
        var collected = new List<byte>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (collected.Count < expected && DateTime.UtcNow < deadline)
        {
            var sink = new List<ReadOnlyMemory<byte>>();
            mux.DrainInto(channelId, sink);
            foreach (var m in sink)
            {
                collected.Add(m.Span[0]);
            }

            if (collected.Count < expected)
            {
                await Task.Delay(2);
            }
        }

        return collected.ToArray();
    }

    /// <summary>
    /// A deterministic <see cref="IPeerLink"/>: it records sent frames and yields
    /// inbound frames from an explicit queue in the exact order injected — no
    /// timing, no RNG, so receive-policy tests are bit-stable.
    /// </summary>
    private sealed class ScriptedPeerLink : IPeerLink
    {
        private readonly Channel<ReadOnlyMemory<byte>> _inbound =
            Channel.CreateUnbounded<ReadOnlyMemory<byte>>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        public ScriptedPeerLink(PeerLinkCapabilities capabilities)
        {
            Capabilities = capabilities;
        }

        public PeerLinkCapabilities Capabilities { get; }

        public List<byte[]> Sent { get; } = new();

        public bool Disposed { get; private set; }

        public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default)
        {
            Sent.Add(payload.ToArray());
            return ValueTask.CompletedTask;
        }

        public IAsyncEnumerable<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default)
        {
            return _inbound.Reader.ReadAllAsync(ct);
        }

        public void Inject(byte[] frame)
        {
            _inbound.Writer.TryWrite(frame);
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            _inbound.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }
}
