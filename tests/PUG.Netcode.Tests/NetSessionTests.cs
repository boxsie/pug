namespace PUG.Netcode.Tests;

/// <summary>
/// The topology + authority seam. Exercises the client shape (1 link, remote
/// authority, injected self-id) and the server shape (N links, self authority,
/// authority-assigned peer-ids, broadcast fan-out, per-peer inbound attribution)
/// — all over <see cref="FakePeerLink"/> pairs, no daemon.
/// </summary>
public class NetSessionTests
{
    private static readonly ChannelSpec[] OneChannel = { new(1, ChannelMode.Unreliable) };

    [Fact]
    public void Client_HasRemoteAuthority_AndLearnsItsOwnId()
    {
        var (clientLink, _) = FakePeerLink.CreatePair();
        var session = NetSession.CreateClient(clientLink, OneChannel, selfId: new PeerId(7));

        Assert.False(session.IsAuthority);
        Assert.Equal(new PeerId(7), session.SelfId);

        var authority = Assert.Single(session.Peers);
        Assert.Same(authority, session.AuthorityPeer);
        Assert.Equal(PeerId.Authority, authority.Id);
        Assert.True(authority.Id.IsAuthority);
    }

    [Fact]
    public void Client_RejectsReservedSelfId()
    {
        var (clientLink, _) = FakePeerLink.CreatePair();
        Assert.Throws<ArgumentException>(
            () => NetSession.CreateClient(clientLink, OneChannel, selfId: PeerId.Authority));
    }

    [Fact]
    public void Authority_IsSelfAuthority_AndAssignsStableIdsFromOne()
    {
        var (a1, _) = FakePeerLink.CreatePair();
        var (a2, _) = FakePeerLink.CreatePair();
        var session = NetSession.CreateAuthority(OneChannel, new[] { a1, a2 });

        Assert.True(session.IsAuthority);
        Assert.Equal(PeerId.Authority, session.SelfId);
        Assert.Null(session.AuthorityPeer); // it IS the authority — no upstream one

        var peers = session.Peers;
        Assert.Equal(new PeerId(1), peers[0].Id);
        Assert.Equal(new PeerId(2), peers[1].Id);

        var (a3, _) = FakePeerLink.CreatePair();
        var attached = session.AttachClient(a3);
        Assert.Equal(new PeerId(3), attached.Id); // ids stay monotonic across later joins
    }

    [Fact]
    public void AttachClient_OnAClientSession_Throws()
    {
        var (clientLink, _) = FakePeerLink.CreatePair();
        var session = NetSession.CreateClient(clientLink, OneChannel, selfId: new PeerId(1));
        var (other, _) = FakePeerLink.CreatePair();

        Assert.Throws<InvalidOperationException>(() => session.AttachClient(other));
    }

    [Fact]
    public async Task SendToAuthority_OnAnAuthoritySession_Throws()
    {
        var session = NetSession.CreateAuthority(OneChannel);
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.SendToAuthorityAsync(1, new byte[] { 0 }));
    }

    [Fact]
    public async Task Authority_BroadcastReachesEveryClient()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authority = NetSession.CreateAuthority(OneChannel, new[] { a1, a2 });
        await using var client1 = new ChannelMux(b1, OneChannel);
        await using var client2 = new ChannelMux(b2, OneChannel);

        await authority.BroadcastAsync(1, new byte[] { 42 });

        await TestPolling.WaitUntilAsync(
            () => Received(client1, 1) >= 1 && Received(client2, 1) >= 1, "both clients got the broadcast");

        Assert.Equal(42, DrainFirst(client1, 1));
        Assert.Equal(42, DrainFirst(client2, 1));
    }

    [Fact]
    public async Task Authority_DrainAttributesInboundToTheSendingPeer()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authority = NetSession.CreateAuthority(OneChannel, new[] { a1, a2 });
        await using var client1 = new ChannelMux(b1, OneChannel); // becomes peer#1
        await using var client2 = new ChannelMux(b2, OneChannel); // becomes peer#2

        await client1.SendAsync(1, new byte[] { 0x11 });
        await client2.SendAsync(1, new byte[] { 0x22 });

        await TestPolling.WaitUntilAsync(
            () => Received(authority.Peers[0].Mux, 1) >= 1 && Received(authority.Peers[1].Mux, 1) >= 1,
            "authority received from both clients");

        var inbound = new List<PeerInbound>();
        authority.DrainInto(1, inbound);

        Assert.Equal(0x11, Single(inbound, new PeerId(1)));
        Assert.Equal(0x22, Single(inbound, new PeerId(2)));
    }

    [Fact]
    public async Task Client_BroadcastAndSendToAuthority_BothReachTheServer()
    {
        var (clientLink, serverLink) = FakePeerLink.CreatePair();
        await using var client = NetSession.CreateClient(clientLink, OneChannel, selfId: new PeerId(1));
        await using var serverMux = new ChannelMux(serverLink, OneChannel);

        await client.BroadcastAsync(1, new byte[] { 1 });        // N=1 generalization
        await client.SendToAuthorityAsync(1, new byte[] { 2 });  // reads as what it is

        await TestPolling.WaitUntilAsync(() => Received(serverMux, 1) >= 2, "server got both client sends");
    }

    [Fact]
    public async Task SendTo_TargetsOnePeerById()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authority = NetSession.CreateAuthority(OneChannel, new[] { a1, a2 });
        await using var client1 = new ChannelMux(b1, OneChannel);
        await using var client2 = new ChannelMux(b2, OneChannel);

        await authority.SendToAsync(new PeerId(2), 1, new byte[] { 99 });

        await TestPolling.WaitUntilAsync(() => Received(client2, 1) >= 1, "only client2 addressed");
        Assert.Equal(0, Received(client1, 1)); // peer#1 untouched
        Assert.Equal(99, DrainFirst(client2, 1));
    }

    [Fact]
    public async Task Dispose_TearsDownTheOwnedLinks()
    {
        var (a1, _) = FakePeerLink.CreatePair();
        var session = NetSession.CreateAuthority(OneChannel, new[] { a1 });

        await session.DisposeAsync();

        // The session owns its links — after disposal the link is dead.
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await a1.SendAsync(new byte[] { 0 }));
    }

    private static long Received(ChannelMux mux, byte channelId) =>
        mux.Stats.Channels.First(c => c.ChannelId == channelId).PacketsReceived;

    private static int DrainFirst(ChannelMux mux, byte channelId)
    {
        var sink = new List<ReadOnlyMemory<byte>>();
        mux.DrainInto(channelId, sink);
        return sink[0].Span[0];
    }

    private static int Single(IEnumerable<PeerInbound> inbound, PeerId from) =>
        inbound.Single(p => p.From == from).Payload.Span[0];
}
