using StackExchange.Redis;

namespace PUG.Redis.Tests;

[Collection(RedisCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RedisDistributedLockTests
{
    private readonly RedisFixture _fixture;

    public RedisDistributedLockTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    private RedisDistributedLock NewLock() => new(_fixture.Multiplexer);

    private static string KeyOf(string suffix) => "pug:lock:" + suffix;

    [Fact]
    public async Task HappyPath_AcquiresRunsReleases()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var db = _fixture.Multiplexer.GetDatabase();

        var result = await sut.ExecuteAsync(
            "happy",
            () => Task.FromResult("done"));

        Assert.Equal("done", result);
        Assert.False(await db.KeyExistsAsync(KeyOf("happy")));
    }

    [Fact]
    public async Task Contention_SecondCallerBlocksUntilFirstReleases()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var order = new List<string>();
        var orderLock = new object();
        void Append(string s) { lock (orderLock) { order.Add(s); } }

        var holder = Task.Run(async () =>
        {
            await sut.ExecuteAsync<object?>(
                "shared",
                async () =>
                {
                    Append("A:in");
                    await Task.Delay(400);
                    Append("A:out");
                    return null;
                });
        });

        await Task.Delay(50);

        var contender = Task.Run(async () =>
        {
            await sut.ExecuteAsync<object?>(
                "shared",
                () =>
                {
                    Append("B:in");
                    return Task.FromResult<object?>(null);
                },
                timeout: TimeSpan.FromSeconds(2),
                retryCount: 20,
                retryDelayMs: 100);
        });

        await Task.WhenAll(holder, contender);

        Assert.Equal(new[] { "A:in", "A:out", "B:in" }, order);
    }

    [Fact]
    public async Task LockExpiresOnTimeout_FreshAcquireSucceeds()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var db = _fixture.Multiplexer.GetDatabase();
        var key = "expires";

        // Plant a lock that out-lives our action — we manually SET to control
        // the TTL precisely; ExecuteAsync would refresh on its own loop.
        var foreignToken = Guid.NewGuid().ToString("N");
        await db.StringSetAsync(KeyOf(key), foreignToken, expiry: TimeSpan.FromMilliseconds(200), when: When.NotExists);

        // Wait past the TTL so Redis evicts the key.
        await Task.Delay(350);

        var result = await sut.ExecuteAsync(
            key,
            () => Task.FromResult("acquired"),
            timeout: TimeSpan.FromSeconds(5),
            retryCount: 0);

        Assert.Equal("acquired", result);
    }

    [Fact]
    public async Task ActionThrows_LockStillReleased()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var db = _fixture.Multiplexer.GetDatabase();
        var key = "throws";

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await sut.ExecuteAsync<object?>(
                key,
                () => throw new InvalidOperationException("boom"));
        });

        Assert.False(await db.KeyExistsAsync(KeyOf(key)));
    }

    [Fact]
    public async Task TryReleaseAsync_OwnerMismatch_LeavesForeignLockIntact()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var db = _fixture.Multiplexer.GetDatabase();
        var key = "mismatch";

        // Plant a foreign lock.
        await db.StringSetAsync(KeyOf(key), "alice", expiry: TimeSpan.FromMinutes(1), when: When.NotExists);

        var released = await sut.TryReleaseAsync(key, "bob");

        Assert.False(released);
        Assert.Equal("alice", (string?)await db.StringGetAsync(KeyOf(key)));
    }

    [Fact]
    public async Task AcquireBudgetExhausted_ReturnsDefault()
    {
        await _fixture.ResetAsync();
        var sut = NewLock();
        var db = _fixture.Multiplexer.GetDatabase();
        var key = "budget";

        // Plant a foreign lock that out-lives the contender's retry budget.
        await db.StringSetAsync(KeyOf(key), "alice", expiry: TimeSpan.FromSeconds(5), when: When.NotExists);

        var result = await sut.ExecuteAsync<string>(
            key,
            () => Task.FromResult("should-not-run"),
            timeout: TimeSpan.FromSeconds(5),
            retryCount: 2,
            retryDelayMs: 50);

        Assert.Null(result);
    }
}
