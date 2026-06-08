namespace PUG.Netcode.Tests;

/// <summary>
/// Tier B2 reliable game events over the A1 Ordered channel. Proves events arrive
/// reliably and in send order even when the link reorders them, that the tick
/// stamp round-trips, and that NetSession routing carries events both directions
/// with sender attribution.
/// </summary>
public class NetEventChannelTests
{
    private const byte Ev = 2;
    private static readonly ChannelSpec[] Specs = { new(Ev, ChannelMode.Ordered) };

    [Fact]
    public async Task EveryEvent_ArrivesInSendOrder_EvenUnderReorder()
    {
        // Reliable but unordered + jitter ⇒ the link delivers everything but shuffles
        // it; the A1 Ordered channel must resequence. (Ensemble's real profile.)
        var (a, b) = FakePeerLink.CreatePair(new FakeLinkOptions
        {
            Jitter = TimeSpan.FromMilliseconds(20),
            Seed = 3,
            Capabilities = new PeerLinkCapabilities(PeerLinkGuarantees.Reliable),
        });
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        await using var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
        var authorityEvents = new NetEventChannel(authoritySession, Ev);
        var clientEvents = new NetEventChannel(clientSession, Ev);

        const int n = 10;
        for (var i = 0; i < n; i++)
        {
            await authorityEvents.BroadcastAsync(tick: (uint)(1000 + i), new[] { (byte)i });
        }

        var received = new List<GameEvent>();
        await TestPolling.WaitUntilAsync(
            () => { clientEvents.Drain(received); return received.Count >= n; }, "all events delivered");

        Assert.Equal(n, received.Count);
        for (var i = 0; i < n; i++)
        {
            Assert.Equal(i, received[i].Payload.Span[0]);          // in send order
            Assert.Equal((uint)(1000 + i), received[i].Tick);      // tick stamp round-trips
            Assert.Equal(PeerId.Authority, received[i].From);      // from the authority
        }

        Assert.Equal(n, authorityEvents.Stats.EventsSent);
        Assert.Equal(n, clientEvents.Stats.EventsReceived);
    }

    [Fact]
    public async Task ClientToAuthority_ReachesTheAuthority_AttributedToTheClient()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        await using var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
        var authorityEvents = new NetEventChannel(authoritySession, Ev);
        var clientEvents = new NetEventChannel(clientSession, Ev);

        await clientEvents.SendToAuthorityAsync(tick: 42, new byte[] { 0xAB });

        var received = new List<GameEvent>();
        await TestPolling.WaitUntilAsync(
            () => { authorityEvents.Drain(received); return received.Count >= 1; }, "authority got the event");

        var ev = Assert.Single(received);
        Assert.Equal(new PeerId(1), ev.From); // link identity = the sending client
        Assert.Equal(42u, ev.Tick);
        Assert.Equal(0xAB, ev.Payload.Span[0]);
        Assert.Equal(1, clientEvents.Stats.EventsSent);
    }

    [Fact]
    public async Task Broadcast_ReachesEveryClientLink()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a1, a2 });
        await using var client1Session = NetSession.CreateClient(b1, Specs, new PeerId(1));
        await using var client2Session = NetSession.CreateClient(b2, Specs, new PeerId(2));
        var authorityEvents = new NetEventChannel(authoritySession, Ev);
        var client1 = new NetEventChannel(client1Session, Ev);
        var client2 = new NetEventChannel(client2Session, Ev);

        await authorityEvents.BroadcastAsync(tick: 7, new byte[] { 0x55 });

        var r1 = new List<GameEvent>();
        var r2 = new List<GameEvent>();
        await TestPolling.WaitUntilAsync(
            () =>
            {
                client1.Drain(r1);
                client2.Drain(r2);
                return r1.Count >= 1 && r2.Count >= 1;
            },
            "both clients got the broadcast");

        Assert.Equal(0x55, Assert.Single(r1).Payload.Span[0]);
        Assert.Equal(0x55, Assert.Single(r2).Payload.Span[0]);
    }
}
