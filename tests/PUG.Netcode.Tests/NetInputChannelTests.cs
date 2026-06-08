namespace PUG.Netcode.Tests;

/// <summary>
/// Tier B3 client input up the KeepLatest channel via <see cref="NetInputChannel"/>.
/// Proves a client's input reaches the authority attributed to the right peer, the
/// tick stamp round-trips, the authority keeps only the freshest input per peer, and
/// two clients' inputs are held separately.
/// </summary>
public class NetInputChannelTests
{
    private const byte In = 2;
    private static readonly ChannelSpec[] Specs = { new(In, ChannelMode.KeepLatest) };

    [Fact]
    public async Task ClientInput_ReachesAuthority_AttributedToTheClient()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        await using var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
        var authorityInput = new NetInputChannel(authoritySession, In);
        var clientInput = new NetInputChannel(clientSession, In);

        await clientInput.SendToAuthorityAsync(tick: 42, new byte[] { 0xAB, 0xCD });

        await TestPolling.WaitUntilAsync(
            () => { authorityInput.Drain(); return authorityInput.Latest.Count >= 1; }, "authority got the input");

        Assert.True(authorityInput.TryGetLatest(new PeerId(1), out var input));
        Assert.Equal(new PeerId(1), input.Peer); // link identity = the sending client
        Assert.Equal(42u, input.Tick);
        Assert.Equal(new byte[] { 0xAB, 0xCD }, input.Payload.ToArray());
        Assert.Equal(1, clientInput.Stats.InputsSent);
        Assert.Equal(1, authorityInput.Stats.InputsReceived);
    }

    [Fact]
    public async Task Authority_KeepsOnlyTheFreshestInputPerPeer()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        await using var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
        var authorityInput = new NetInputChannel(authoritySession, In);
        var clientInput = new NetInputChannel(clientSession, In);

        // Stream a run of inputs; KeepLatest + the per-peer tick fold must converge
        // on the newest, never a stale earlier tick.
        const int n = 8;
        for (var i = 1; i <= n; i++)
        {
            await clientInput.SendToAuthorityAsync(tick: (uint)i, new[] { (byte)i });
        }

        await TestPolling.WaitUntilAsync(
            () =>
            {
                authorityInput.Drain();
                return authorityInput.TryGetLatest(new PeerId(1), out var cur) && cur.Tick == n;
            },
            "authority converged on the newest input");

        Assert.True(authorityInput.TryGetLatest(new PeerId(1), out var latest));
        Assert.Equal((uint)n, latest.Tick);
        Assert.Equal((byte)n, latest.Payload.Span[0]);
    }

    [Fact]
    public async Task TwoClients_InputsHeldSeparatelyPerPeer()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a1, a2 });
        await using var client1Session = NetSession.CreateClient(b1, Specs, new PeerId(1));
        await using var client2Session = NetSession.CreateClient(b2, Specs, new PeerId(2));
        var authorityInput = new NetInputChannel(authoritySession, In);
        var client1 = new NetInputChannel(client1Session, In);
        var client2 = new NetInputChannel(client2Session, In);

        await client1.SendToAuthorityAsync(tick: 5, new byte[] { 0x11 });
        await client2.SendToAuthorityAsync(tick: 9, new byte[] { 0x22 });

        await TestPolling.WaitUntilAsync(
            () => { authorityInput.Drain(); return authorityInput.Latest.Count >= 2; }, "both clients' inputs held");

        Assert.True(authorityInput.TryGetLatest(new PeerId(1), out var i1));
        Assert.True(authorityInput.TryGetLatest(new PeerId(2), out var i2));
        Assert.Equal((5u, (byte)0x11), (i1.Tick, i1.Payload.Span[0]));
        Assert.Equal((9u, (byte)0x22), (i2.Tick, i2.Payload.Span[0]));
    }

    [Fact]
    public async Task SendInput_OnAuthority_Throws()
    {
        var (a, _) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        var authorityInput = new NetInputChannel(authoritySession, In);

        // The authority has no upstream authority to send input to.
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await authorityInput.SendToAuthorityAsync(tick: 1, new byte[] { 0x00 }));
    }
}
