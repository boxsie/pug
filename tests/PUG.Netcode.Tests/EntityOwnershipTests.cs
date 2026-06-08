using System.Buffers;

namespace PUG.Netcode.Tests;

/// <summary>
/// Tier B3 entity ownership: the authority tags each registered entity with a
/// controlling <see cref="PeerId"/>, that owner round-trips through the B1 snapshot,
/// and a client identifies "mine" by comparing the owner to its own
/// <see cref="NetSession.SelfId"/>. Proven for one client and for two clients each
/// seeing its own entity as owned and the other's as remote.
/// </summary>
public class EntityOwnershipTests
{
    private const byte Snap = 1;
    private static readonly ChannelSpec[] Specs = { new(Snap, ChannelMode.KeepLatest) };

    [Fact]
    public async Task Owner_RoundTrips_AndClientIdentifiesOwnEntity()
    {
        var (a, b) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a });
        await using var clientSession = NetSession.CreateClient(b, Specs, new PeerId(1));
        var authority = NetworkReplicator.CreateAuthority(authoritySession, Snap);
        var client = NetworkReplicator.CreateClient(clientSession, Snap, kind => new OwnedEntity(kind));

        // The world owns the ball; the client (peer 1) owns its paddle.
        var ballId = authority.Register(new OwnedEntity(kind: 20));
        var paddleId = authority.Register(new OwnedEntity(kind: 10), new PeerId(1));

        await authority.CaptureAndBroadcastAsync(tick: 1);
        await TestPolling.WaitUntilAsync(
            () => { client.Apply(); return client.Entities.Count == 2; }, "both entities replicated");

        // Owner round-trips: ball → authority, paddle → peer 1.
        Assert.Equal(PeerId.Authority, client.Entities[ballId].Owner);
        Assert.Equal(new PeerId(1), client.Entities[paddleId].Owner);

        // "Which is mine" via SelfId: exactly the paddle.
        var mine = client.EntitiesOwnedBy(clientSession.SelfId).ToList();
        Assert.Equal(paddleId, Assert.Single(mine).Id);
        Assert.DoesNotContain(mine, e => e.Owner == PeerId.Authority);
    }

    [Fact]
    public async Task TwoClients_EachOwnsItsOwn_AndSeesTheOtherAsRemote()
    {
        var (a1, b1) = FakePeerLink.CreatePair();
        var (a2, b2) = FakePeerLink.CreatePair();
        await using var authoritySession = NetSession.CreateAuthority(Specs, new[] { a1, a2 });
        await using var client1Session = NetSession.CreateClient(b1, Specs, new PeerId(1));
        await using var client2Session = NetSession.CreateClient(b2, Specs, new PeerId(2));
        var authority = NetworkReplicator.CreateAuthority(authoritySession, Snap);
        var client1 = NetworkReplicator.CreateClient(client1Session, Snap, kind => new OwnedEntity(kind));
        var client2 = NetworkReplicator.CreateClient(client2Session, Snap, kind => new OwnedEntity(kind));

        var paddle1Id = authority.Register(new OwnedEntity(kind: 10), new PeerId(1));
        var paddle2Id = authority.Register(new OwnedEntity(kind: 10), new PeerId(2));

        await authority.CaptureAndBroadcastAsync(tick: 1);
        await TestPolling.WaitUntilAsync(
            () =>
            {
                client1.Apply();
                client2.Apply();
                return client1.Entities.Count == 2 && client2.Entities.Count == 2;
            },
            "both paddles replicated to both clients");

        // Client 1: paddle1 is mine, paddle2 is remote.
        Assert.Equal(paddle1Id, Assert.Single(client1.EntitiesOwnedBy(client1Session.SelfId)).Id);
        Assert.Equal(new PeerId(2), client1.Entities[paddle2Id].Owner);

        // Client 2: paddle2 is mine, paddle1 is remote.
        Assert.Equal(paddle2Id, Assert.Single(client2.EntitiesOwnedBy(client2Session.SelfId)).Id);
        Assert.Equal(new PeerId(1), client2.Entities[paddle1Id].Owner);
    }

    /// <summary>A trivial entity that carries only its kind — ownership is the
    /// replicator's concern, not the state's, so no payload is needed.</summary>
    private sealed class OwnedEntity : INetEntityState
    {
        public OwnedEntity(byte kind) => Kind = kind;

        public byte Kind { get; }

        public void WriteState(IBufferWriter<byte> writer)
        {
            // One placeholder byte: a zero-length state is legal, but a byte keeps
            // the wire shape obviously non-empty.
            var span = writer.GetSpan(1);
            span[0] = 0;
            writer.Advance(1);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            // Nothing to read — this entity is about ownership, not state.
        }
    }
}
