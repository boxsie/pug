using PUG.Core;

namespace PUG.Redis.Tests;

[Collection(RedisCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RedisQueueTests
{
    private readonly RedisFixture _fixture;

    public RedisQueueTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    private RedisQueue<Ticket<object?>> NewQueue(string name) => new(_fixture.Multiplexer, name);

    private static Ticket<object?> Make(DateTime enqueuedAt) =>
        new(Guid.NewGuid(), enqueuedAt, Payload: null);

    private static Ticket<object?> MakeFor(Guid playerId, DateTime enqueuedAt) =>
        new(playerId, enqueuedAt, Payload: null);

    [Fact]
    public async Task Enqueue_PeekOldest_ReturnsInScoreOrder()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("peek-order");
        var t0 = Make(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var t1 = Make(new DateTime(2026, 1, 1, 0, 0, 5, DateTimeKind.Utc));
        var t2 = Make(new DateTime(2026, 1, 1, 0, 0, 10, DateTimeKind.Utc));

        await q.EnqueueAsync(t2, default);
        await q.EnqueueAsync(t0, default);
        await q.EnqueueAsync(t1, default);

        var oldest = await q.PeekOldestAsync(3, default);

        Assert.Equal(new[] { t0.PlayerId, t1.PlayerId, t2.PlayerId }, oldest.Select(t => t.PlayerId));
    }

    [Fact]
    public async Task PeekOldest_CountSmallerThanQueue_TruncatesToOldestN()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("peek-count");
        for (var i = 0; i < 5; i++)
        {
            await q.EnqueueAsync(Make(new DateTime(2026, 1, 1, 0, 0, i, DateTimeKind.Utc)), default);
        }

        var two = await q.PeekOldestAsync(2, default);
        Assert.Equal(2, two.Count);
    }

    [Fact]
    public async Task PeekTimedOut_ReturnsOnlyEntriesOlderThanThreshold()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("timeout");
        var now = DateTime.UtcNow;

        var old1 = Make(now.AddMinutes(-3));
        var old2 = Make(now.AddMinutes(-2));
        var fresh = Make(now.AddSeconds(-10));

        await q.EnqueueAsync(old1, default);
        await q.EnqueueAsync(old2, default);
        await q.EnqueueAsync(fresh, default);

        var timedOut = await q.PeekTimedOutAsync(TimeSpan.FromMinutes(1), default);

        Assert.Equal(2, timedOut.Count);
        Assert.Contains(timedOut, t => t.PlayerId == old1.PlayerId);
        Assert.Contains(timedOut, t => t.PlayerId == old2.PlayerId);
        Assert.DoesNotContain(timedOut, t => t.PlayerId == fresh.PlayerId);
    }

    [Fact]
    public async Task Remove_DropsBothZsetAndSideHash()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("remove");
        var t = Make(DateTime.UtcNow);
        await q.EnqueueAsync(t, default);

        await q.RemoveAsync(t.PlayerId, default);

        Assert.Equal(0, await q.CountAsync(default));
        var db = _fixture.Multiplexer.GetDatabase();
        Assert.False(await db.HashExistsAsync("pug:queue:remove:tickets", t.PlayerId.ToString("N")));
    }

    [Fact]
    public async Task Remove_UnknownPlayer_NoThrow()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("remove-unknown");
        await q.RemoveAsync(Guid.NewGuid(), default);
    }

    [Fact]
    public async Task Enqueue_SamePlayerTwice_OverwritesInPlace()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("dup-player");
        var playerId = Guid.NewGuid();
        var first = MakeFor(playerId, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var second = MakeFor(playerId, new DateTime(2026, 1, 1, 0, 0, 30, DateTimeKind.Utc));

        await q.EnqueueAsync(first, default);
        await q.EnqueueAsync(second, default);

        Assert.Equal(1, await q.CountAsync(default));
        var stats = await q.GetStatsAsync(default);
        // Second enqueue refreshed the score; oldest wait reflects EnqueuedAt of `second`.
        Assert.Equal(1, stats.Count);
    }

    [Fact]
    public async Task Stats_AccurateAcrossEnqueueAndRemove()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("stats");

        var empty = await q.GetStatsAsync(default);
        Assert.Equal(0, empty.Count);
        Assert.Null(empty.OldestWait);

        var oldEnqueuedAt = DateTime.UtcNow.AddSeconds(-30);
        var oldTicket = Make(oldEnqueuedAt);
        await q.EnqueueAsync(oldTicket, default);
        await q.EnqueueAsync(Make(DateTime.UtcNow), default);

        var stats = await q.GetStatsAsync(default);
        Assert.Equal(2, stats.Count);
        Assert.NotNull(stats.OldestWait);
        Assert.InRange(stats.OldestWait!.Value.TotalSeconds, 25, 45);

        await q.RemoveAsync(oldTicket.PlayerId, default);
        var afterRemove = await q.GetStatsAsync(default);
        Assert.Equal(1, afterRemove.Count);
        Assert.True(afterRemove.OldestWait!.Value < TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task ConcurrentEnqueues_DoNotCorruptOrdering()
    {
        await _fixture.ResetAsync();
        var q = NewQueue("concurrent");
        var baseTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        var tasks = new List<Task>();
        for (var i = 0; i < 50; i++)
        {
            var t = Make(baseTime.AddSeconds(i));
            tasks.Add(q.EnqueueAsync(t, default));
        }
        await Task.WhenAll(tasks);

        Assert.Equal(50, await q.CountAsync(default));
        var ordered = await q.PeekOldestAsync(50, default);
        for (var i = 1; i < ordered.Count; i++)
        {
            Assert.True(ordered[i - 1].EnqueuedAt <= ordered[i].EnqueuedAt,
                $"At index {i}: previous={ordered[i - 1].EnqueuedAt:O}, current={ordered[i].EnqueuedAt:O}");
        }
    }
}
