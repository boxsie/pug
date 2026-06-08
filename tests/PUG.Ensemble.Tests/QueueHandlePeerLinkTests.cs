using System.Threading.Channels;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Unit coverage for the <see cref="QueueHandlePeerLink"/> netcode adapter.
/// Runs with NO daemon: the adapter depends on the narrow
/// <see cref="IPeerChannel"/> seam, so a in-memory fake stands in for a matched
/// <see cref="QueueHandle{TPayload}"/> (which would need a live Ensemble daemon
/// to construct). Not <c>[Category=Integration]</c> — these run everywhere.
/// </summary>
public class QueueHandlePeerLinkTests
{
    private const string PeerAddr = "Epeer";
    private const string OtherAddr = "Eother";

    [Fact]
    public void Capabilities_ReportReliableUnordered_WithDefaultPayloadCap()
    {
        var link = new QueueHandlePeerLink(new FakePeerChannel(), PeerAddr);

        // Ensemble = reliable QUIC streams, but a fresh stream per send ⇒ no
        // cross-send ordering. The adapter must NOT claim Ordered.
        Assert.True(link.Capabilities.IsReliable);
        Assert.False(link.Capabilities.IsOrdered);
        Assert.False(link.Capabilities.IsReliableOrdered);
        Assert.Equal(QueueHandlePeerLink.DefaultMaxPayloadBytes, link.Capabilities.MaxPayloadBytes);
    }

    [Fact]
    public void Capabilities_HonoursCustomPayloadCap()
    {
        var link = new QueueHandlePeerLink(new FakePeerChannel(), PeerAddr, maxPayloadBytes: 4 * 1024 * 1024);

        Assert.Equal(4 * 1024 * 1024, link.Capabilities.MaxPayloadBytes);
    }

    [Fact]
    public async Task SendAsync_ForwardsBytesToTheMatchedPeer()
    {
        var channel = new FakePeerChannel();
        var link = new QueueHandlePeerLink(channel, PeerAddr);

        await link.SendAsync(new byte[] { 1, 2, 3 });

        var sent = Assert.Single(channel.Sent);
        Assert.Equal(PeerAddr, sent.PeerAddr);
        Assert.Equal(new byte[] { 1, 2, 3 }, sent.Bytes);
    }

    [Fact]
    public async Task ReceiveAsync_DropsTrafficFromOtherPeers()
    {
        var channel = new FakePeerChannel();
        var link = new QueueHandlePeerLink(channel, PeerAddr);

        channel.Inject(OtherAddr, new byte[] { 9 }); // dropped — not our peer
        channel.Inject(PeerAddr, new byte[] { 1 });
        channel.Inject(OtherAddr, new byte[] { 8 }); // dropped
        channel.Inject(PeerAddr, new byte[] { 2 });
        channel.Complete();

        var got = new List<byte>();
        await foreach (var msg in link.ReceiveAsync())
            got.Add(msg.Span[0]);

        Assert.Equal(new byte[] { 1, 2 }, got);
    }

    [Fact]
    public async Task DisposeAsync_IsNoOp_LeavesUnderlyingChannelUsable()
    {
        var channel = new FakePeerChannel();
        var link = new QueueHandlePeerLink(channel, PeerAddr);

        await link.DisposeAsync();
        await link.DisposeAsync(); // idempotent

        // The adapter doesn't own the channel — it's still alive and usable.
        channel.Inject(PeerAddr, new byte[] { 7 });
        channel.Complete();

        var got = new List<byte>();
        await foreach (var msg in channel.PeerMessages())
            got.Add(msg.Bytes.Span[0]);

        Assert.Equal(new byte[] { 7 }, got);
    }

    [Fact]
    public void Constructor_RejectsBlankPeerAddr()
    {
        Assert.Throws<ArgumentException>(() => new QueueHandlePeerLink(new FakePeerChannel(), "  "));
    }

    [Fact]
    public void Constructor_RejectsNullChannel()
    {
        Assert.Throws<ArgumentNullException>(() => new QueueHandlePeerLink(null!, PeerAddr));
    }

    /// <summary>In-memory <see cref="IPeerChannel"/> — records sends, replays
    /// injected inbound messages. Stands in for a matched QueueHandle.</summary>
    private sealed class FakePeerChannel : IPeerChannel
    {
        private readonly Channel<PeerMessage> _inbound = Channel.CreateUnbounded<PeerMessage>();

        public List<(string PeerAddr, byte[] Bytes)> Sent { get; } = new();

        public Task SendToPeerAsync(string peerAddr, ReadOnlyMemory<byte> bytes, CancellationToken ct = default)
        {
            Sent.Add((peerAddr, bytes.ToArray()));
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<PeerMessage> PeerMessages(CancellationToken ct = default) =>
            _inbound.Reader.ReadAllAsync(ct);

        public void Inject(string fromAddr, byte[] bytes) =>
            _inbound.Writer.TryWrite(new PeerMessage(fromAddr, bytes, DateTimeOffset.UtcNow));

        public void Complete() => _inbound.Writer.TryComplete();
    }
}
