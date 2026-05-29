namespace PUG.Redis.Tests;

[Collection(RedisCollection.Name)]
[Trait("Category", "Integration")]
public sealed class RedisConnectionMapTests
{
    private readonly RedisFixture _fixture;

    public RedisConnectionMapTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    private RedisConnectionMap NewMap(TimeSpan? ttl = null) => new(_fixture.Multiplexer, ttl);

    [Fact]
    public async Task Add_Get_RoundTripsSingleConnection()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var playerId = Guid.NewGuid();

        await map.AddAsync(playerId, "conn-1", default);

        var conns = await map.GetAsync(playerId, default);
        Assert.Equal(new[] { "conn-1" }, conns);
    }

    [Fact]
    public async Task MultipleConnections_BothReturned()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var playerId = Guid.NewGuid();

        await map.AddAsync(playerId, "phone", default);
        await map.AddAsync(playerId, "pc", default);

        var conns = await map.GetAsync(playerId, default);
        Assert.Equal(2, conns.Count);
        Assert.Contains("phone", conns);
        Assert.Contains("pc", conns);
    }

    [Fact]
    public async Task RemoveSingleConnection_LeavesOthers()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var playerId = Guid.NewGuid();
        await map.AddAsync(playerId, "phone", default);
        await map.AddAsync(playerId, "pc", default);

        await map.RemoveAsync(playerId, "phone", default);

        var conns = await map.GetAsync(playerId, default);
        Assert.Equal(new[] { "pc" }, conns);
    }

    [Fact]
    public async Task RemoveLastConnection_EvictsKey()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var playerId = Guid.NewGuid();
        await map.AddAsync(playerId, "only", default);

        await map.RemoveAsync(playerId, "only", default);

        Assert.False(await map.IsOnlineAsync(playerId, default));
        var db = _fixture.Multiplexer.GetDatabase();
        Assert.False(await db.KeyExistsAsync("pug:conn:" + playerId.ToString("N")));
    }

    [Fact]
    public async Task IsOnline_ReturnsTrueWhenAnyConnectionPresent()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var playerId = Guid.NewGuid();

        Assert.False(await map.IsOnlineAsync(playerId, default));

        await map.AddAsync(playerId, "c", default);
        Assert.True(await map.IsOnlineAsync(playerId, default));

        await map.RemoveAsync(playerId, "c", default);
        Assert.False(await map.IsOnlineAsync(playerId, default));
    }

    [Fact]
    public async Task Ttl_EvictsStaleConnections()
    {
        await _fixture.ResetAsync();
        var map = NewMap(TimeSpan.FromMilliseconds(300));
        var playerId = Guid.NewGuid();

        await map.AddAsync(playerId, "stale", default);
        Assert.True(await map.IsOnlineAsync(playerId, default));

        await Task.Delay(500);

        Assert.False(await map.IsOnlineAsync(playerId, default));
    }

    [Fact]
    public async Task AddAsync_EmptyConnectionId_Throws()
    {
        var map = NewMap();
        await Assert.ThrowsAsync<ArgumentException>(
            () => map.AddAsync(Guid.NewGuid(), "", default));
    }

    [Fact]
    public async Task GetAsync_NoConnections_ReturnsEmpty()
    {
        await _fixture.ResetAsync();
        var map = NewMap();
        var conns = await map.GetAsync(Guid.NewGuid(), default);
        Assert.Empty(conns);
    }
}
