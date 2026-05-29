namespace PUG.Core.Tests;

public sealed class FifoMatcherTests
{
    private static Ticket<object?> MakeTicket(int seed, Guid? privateGameId = null) =>
        new(
            PlayerId: Guid.NewGuid(),
            EnqueuedAt: new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seed),
            Payload: null,
            PrivateGameId: privateGameId);

    private static async Task RemoveMatchedAsync(InMemoryQueue<Ticket<object?>> q, MatchResult<Ticket<object?>> match)
    {
        foreach (var team in match.Teams)
        {
            foreach (var ticket in team.Members)
            {
                await q.RemoveAsync(ticket.PlayerId, CancellationToken.None);
            }
        }
    }

    private static async Task<InMemoryQueue<Ticket<object?>>> SeedAsync(int n)
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        for (var i = 0; i < n; i++)
        {
            await q.EnqueueAsync(MakeTicket(i), CancellationToken.None);
        }

        return q;
    }

    [Fact]
    public async Task EmptyQueue_ReturnsNull()
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1 });

        Assert.Null(await m.TryMatchAsync(CancellationToken.None));
    }

    [Fact]
    public async Task QueueBelowRequired_ReturnsNull()
    {
        var q = await SeedAsync(3);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });

        Assert.Null(await m.TryMatchAsync(CancellationToken.None));
    }

    [Fact]
    public async Task OneVOne_PairsOldestIntoTeamZero()
    {
        var q = await SeedAsync(2);
        var enqueued = await q.PeekOldestAsync(2, CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1 });

        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Teams.Count);
        Assert.Equal(0, result.Teams[0].Index);
        Assert.Equal(1, result.Teams[1].Index);
        Assert.Equal(enqueued[0], result.Teams[0].Members.Single());
        Assert.Equal(enqueued[1], result.Teams[1].Members.Single());
    }

    [Fact]
    public async Task TwoVTwo_RoundRobinDistribution()
    {
        var q = await SeedAsync(4);
        var enqueued = await q.PeekOldestAsync(4, CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });

        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(new[] { enqueued[0], enqueued[2] }, result!.Teams[0].Members);
        Assert.Equal(new[] { enqueued[1], enqueued[3] }, result.Teams[1].Members);
    }

    [Fact]
    public async Task FiveVFive_RoundRobinDistribution()
    {
        var q = await SeedAsync(10);
        var enqueued = await q.PeekOldestAsync(10, CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 5, 5 });

        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(new[] { enqueued[0], enqueued[2], enqueued[4], enqueued[6], enqueued[8] }, result!.Teams[0].Members);
        Assert.Equal(new[] { enqueued[1], enqueued[3], enqueued[5], enqueued[7], enqueued[9] }, result.Teams[1].Members);
    }

    [Fact]
    public async Task CoopSingleTeam_AllPlayersInOneTeam()
    {
        var q = await SeedAsync(4);
        var enqueued = await q.PeekOldestAsync(4, CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 4 });

        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result!.Teams);
        Assert.Equal(enqueued, result.Teams[0].Members);
    }

    [Fact]
    public async Task FfaSoloTeams_OnePlayerPerTeam()
    {
        var q = await SeedAsync(4);
        var enqueued = await q.PeekOldestAsync(4, CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1, 1, 1 });

        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(4, result!.Teams.Count);
        for (var i = 0; i < 4; i++)
        {
            Assert.Equal(enqueued[i], result.Teams[i].Members.Single());
        }
    }

    [Fact]
    public async Task AfterMatch_QueueIsUnchanged()
    {
        var q = await SeedAsync(4);
        var before = await q.CountAsync(CancellationToken.None);
        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });

        _ = await m.TryMatchAsync(CancellationToken.None);

        var after = await q.CountAsync(CancellationToken.None);
        Assert.Equal(before, after);
    }

    [Fact]
    public void Ctor_NullQueue_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => new FifoMatcher<Ticket<object?>>(null!, new[] { 1, 1 }));
    }

    [Fact]
    public void Ctor_NullTeamSizes_Throws()
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        Assert.Throws<ArgumentNullException>(
            () => new FifoMatcher<Ticket<object?>>(q, null!));
    }

    [Fact]
    public void Ctor_EmptyTeamSizes_Throws()
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        Assert.Throws<ArgumentException>(
            () => new FifoMatcher<Ticket<object?>>(q, Array.Empty<int>()));
    }

    [Fact]
    public void Ctor_NonPositiveTeamSize_Throws()
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new FifoMatcher<Ticket<object?>>(q, new[] { 0, 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new FifoMatcher<Ticket<object?>>(q, new[] { 2, -1 }));
    }

    [Fact]
    public void Ctor_AsymmetricTeamSizes_Throws()
    {
        var q = new InMemoryQueue<Ticket<object?>>();
        Assert.Throws<ArgumentException>(
            () => new FifoMatcher<Ticket<object?>>(q, new[] { 2, 3 }));
    }

    [Fact]
    public async Task PrivatePartition_FormsMatchWithinSharedGameId()
    {
        var gid = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();
        var a = MakeTicket(seed: 0, privateGameId: gid);
        var public1 = MakeTicket(seed: 1);
        var b = MakeTicket(seed: 2, privateGameId: gid);
        var public2 = MakeTicket(seed: 3);
        foreach (var t in new[] { a, public1, b, public2 })
        {
            await q.EnqueueAsync(t, CancellationToken.None);
        }

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1 });
        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        var matchedPlayers = result!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet();
        Assert.Equal(new[] { a.PlayerId, b.PlayerId }.ToHashSet(), matchedPlayers);
    }

    [Fact]
    public async Task PrivatePartition_BelowRequired_DoesNotConsumePublicTickets()
    {
        // 3 private (gid1) + 1 public, teamSizes = [2, 2]. Neither partition
        // is large enough on its own to satisfy _required = 4, so the matcher
        // must NOT bridge them into a mixed match.
        var gid = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();
        for (var i = 0; i < 3; i++)
        {
            await q.EnqueueAsync(MakeTicket(seed: i, privateGameId: gid), CancellationToken.None);
        }
        await q.EnqueueAsync(MakeTicket(seed: 3), CancellationToken.None);

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });

        Assert.Null(await m.TryMatchAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PublicPartition_FormsMatch_WhenPrivateGroupIsStarved()
    {
        // 3 private (gid1) waiting indefinitely + 4 public. The public partition
        // forms; the private group remains in the queue untouched.
        var gid = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();
        var privates = new List<Ticket<object?>>();
        for (var i = 0; i < 3; i++)
        {
            var t = MakeTicket(seed: i, privateGameId: gid);
            privates.Add(t);
            await q.EnqueueAsync(t, CancellationToken.None);
        }
        var publics = new List<Ticket<object?>>();
        for (var i = 3; i < 7; i++)
        {
            var t = MakeTicket(seed: i);
            publics.Add(t);
            await q.EnqueueAsync(t, CancellationToken.None);
        }

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });
        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        var matchedPlayers = result!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet();
        Assert.Equal(publics.Select(p => p.PlayerId).ToHashSet(), matchedPlayers);
        Assert.DoesNotContain(privates[0].PlayerId, matchedPlayers);
    }

    [Fact]
    public async Task OlderPrivatePartition_FormsBeforeYoungerPrivate()
    {
        // Two private partitions both ready (each has _required tickets). The
        // older partition (lower oldest EnqueuedAt) must form first; the
        // younger one waits for the next tick.
        var older = Guid.NewGuid();
        var younger = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();

        // Interleave so neither partition trivially appears first in the queue.
        var olderA = MakeTicket(seed: 0, privateGameId: older);
        var youngerA = MakeTicket(seed: 5, privateGameId: younger);
        var olderB = MakeTicket(seed: 10, privateGameId: older);
        var youngerB = MakeTicket(seed: 6, privateGameId: younger);
        foreach (var t in new[] { olderA, youngerA, olderB, youngerB })
        {
            await q.EnqueueAsync(t, CancellationToken.None);
        }

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1 });
        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        var matched = result!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet();
        Assert.Equal(new[] { olderA.PlayerId, olderB.PlayerId }.ToHashSet(), matched);
    }

    [Fact]
    public async Task SuccessiveTicks_DrainPrivateThenPublic()
    {
        // Two private partitions interleaved with public tickets. Driving the
        // matcher through successive ticks (with the host removing matched
        // tickets between calls) drains each private partition first, then
        // the public partition.
        var gid1 = Guid.NewGuid();
        var gid2 = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();
        var a1 = MakeTicket(seed: 0, privateGameId: gid1);
        var a2 = MakeTicket(seed: 1, privateGameId: gid2);
        var b1 = MakeTicket(seed: 2, privateGameId: gid1);
        var p1 = MakeTicket(seed: 3);
        var b2 = MakeTicket(seed: 4, privateGameId: gid2);
        var p2 = MakeTicket(seed: 5);
        foreach (var t in new[] { a1, a2, b1, p1, b2, p2 })
        {
            await q.EnqueueAsync(t, CancellationToken.None);
        }

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 1, 1 });

        // Tick 1: gid1 partition (older oldest-waiter — a1 at seed 0).
        var m1 = await m.TryMatchAsync(CancellationToken.None);
        Assert.NotNull(m1);
        Assert.Equal(
            new[] { a1.PlayerId, b1.PlayerId }.ToHashSet(),
            m1!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet());
        await RemoveMatchedAsync(q, m1);

        // Tick 2: gid2 partition (next oldest oldest-waiter — a2 at seed 1).
        var m2 = await m.TryMatchAsync(CancellationToken.None);
        Assert.NotNull(m2);
        Assert.Equal(
            new[] { a2.PlayerId, b2.PlayerId }.ToHashSet(),
            m2!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet());
        await RemoveMatchedAsync(q, m2);

        // Tick 3: public partition (the only one left).
        var m3 = await m.TryMatchAsync(CancellationToken.None);
        Assert.NotNull(m3);
        Assert.Equal(
            new[] { p1.PlayerId, p2.PlayerId }.ToHashSet(),
            m3!.Teams.SelectMany(t => t.Members).Select(t => t.PlayerId).ToHashSet());
    }

    [Fact]
    public async Task PrivatePartition_RoundRobinsWithinTeams()
    {
        // Four tickets sharing one PrivateGameId form a 2v2 — the round-robin
        // distribution within the partition matches the public-queue behaviour.
        var gid = Guid.NewGuid();
        var q = new InMemoryQueue<Ticket<object?>>();
        var enqueued = new List<Ticket<object?>>();
        for (var i = 0; i < 4; i++)
        {
            var t = MakeTicket(seed: i, privateGameId: gid);
            enqueued.Add(t);
            await q.EnqueueAsync(t, CancellationToken.None);
        }

        var m = new FifoMatcher<Ticket<object?>>(q, new[] { 2, 2 });
        var result = await m.TryMatchAsync(CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(new[] { enqueued[0], enqueued[2] }, result!.Teams[0].Members);
        Assert.Equal(new[] { enqueued[1], enqueued[3] }, result.Teams[1].Members);
    }
}
