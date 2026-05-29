namespace PUG.Redis.Tests;

[Collection(RedisCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RedisSessionStoreTests
{
    private readonly RedisFixture _fixture;

    public RedisSessionStoreTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    public sealed class GameSession : IVersioned
    {
        public string Id { get; set; } = "";
        public int Version { get; set; }
        public int Score { get; set; }
    }

    private RedisSessionStore<GameSession> NewStore(TimeSpan? ttl = null)
    {
        var lockSvc = new RedisDistributedLock(_fixture.Multiplexer);
        return new RedisSessionStore<GameSession>(_fixture.Multiplexer, lockSvc, ttl: ttl);
    }

    [Fact]
    public async Task Save_Get_RoundTrips()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        var session = new GameSession { Id = "g1", Version = 0, Score = 7 };

        await store.SaveAsync(session, default);
        var read = await store.GetAsync("g1", default);

        Assert.NotNull(read);
        Assert.Equal("g1", read!.Id);
        Assert.Equal(7, read.Score);
        Assert.Equal(0, read.Version);
    }

    [Fact]
    public async Task GetAsync_Missing_ReturnsNull()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        Assert.Null(await store.GetAsync("ghost", default));
    }

    [Fact]
    public async Task Update_PersistsAndBumpsVersion()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        await store.SaveAsync(new GameSession { Id = "g1", Version = 0, Score = 1 }, default);

        var updated = await store.UpdateAsync(
            "g1",
            session =>
            {
                session.Score = 5;
                return Task.FromResult(true);
            },
            default);

        Assert.NotNull(updated);
        Assert.Equal(5, updated!.Score);
        Assert.Equal(1, updated.Version);

        var fresh = await store.GetAsync("g1", default);
        Assert.Equal(5, fresh!.Score);
        Assert.Equal(1, fresh.Version);
    }

    [Fact]
    public async Task Update_AbortReturnsFalse_NoSaveNoVersionBump()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        await store.SaveAsync(new GameSession { Id = "g1", Version = 3, Score = 10 }, default);

        var result = await store.UpdateAsync(
            "g1",
            session =>
            {
                session.Score = 999; // mutated locally but won't persist
                return Task.FromResult(false);
            },
            default);

        Assert.NotNull(result);
        // The store returns the (locally-mutated) session that wasn't persisted.
        Assert.Equal(999, result!.Score);

        var fresh = await store.GetAsync("g1", default);
        Assert.Equal(10, fresh!.Score);
        Assert.Equal(3, fresh.Version);
    }

    [Fact]
    public async Task Update_MissingId_ReturnsNull()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        var result = await store.UpdateAsync(
            "absent",
            _ => Task.FromResult(true),
            default);

        Assert.Null(result);
    }

    [Fact]
    public async Task ConcurrentUpdates_SerialiseViaLock()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        await store.SaveAsync(new GameSession { Id = "g1", Version = 0, Score = 0 }, default);

        // 10 parallel +1 updates; without the lock they'd race and lose
        // increments (read-1+1 → write-1, read-1+1 → write-1). With the lock,
        // every update reads the latest score then bumps; final score == 10.
        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ =>
            store.UpdateAsync(
                "g1",
                session =>
                {
                    session.Score += 1;
                    return Task.FromResult(true);
                },
                default)));

        var fresh = await store.GetAsync("g1", default);
        Assert.Equal(10, fresh!.Score);
        Assert.Equal(10, fresh.Version);
    }

    [Fact]
    public async Task TtlRefreshedOnSave()
    {
        await _fixture.ResetAsync();
        var store = NewStore(TimeSpan.FromSeconds(2));
        var db = _fixture.Multiplexer.GetDatabase();
        var key = "pug:session:GameSession:g1";

        await store.SaveAsync(new GameSession { Id = "g1", Version = 0, Score = 1 }, default);
        await Task.Delay(800);

        var beforeTtl = await db.KeyTimeToLiveAsync(key);
        Assert.True(beforeTtl!.Value < TimeSpan.FromMilliseconds(1500),
            $"Expected TTL to have decayed below 1500ms; got {beforeTtl}.");

        // Re-save refreshes the TTL.
        await store.SaveAsync(new GameSession { Id = "g1", Version = 0, Score = 1 }, default);
        var afterTtl = await db.KeyTimeToLiveAsync(key);
        Assert.True(afterTtl!.Value > TimeSpan.FromMilliseconds(1500),
            $"Expected TTL to be refreshed near 2s; got {afterTtl}.");
    }

    [Fact]
    public async Task Remove_DeletesSessionKey()
    {
        await _fixture.ResetAsync();
        var store = NewStore();
        await store.SaveAsync(new GameSession { Id = "g1", Version = 0, Score = 1 }, default);

        await store.RemoveAsync("g1", default);

        Assert.Null(await store.GetAsync("g1", default));
        var db = _fixture.Multiplexer.GetDatabase();
        Assert.False(await db.KeyExistsAsync("pug:session:GameSession:g1"));
    }
}
