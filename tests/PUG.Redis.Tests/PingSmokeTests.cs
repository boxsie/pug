namespace PUG.Redis.Tests;

[Collection(RedisCollection.Name)]
[Trait("Category", "Integration")]
public sealed class PingSmokeTests
{
    private readonly RedisFixture _fixture;

    public PingSmokeTests(RedisFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Ping_RoundTrips()
    {
        await _fixture.ResetAsync();
        var db = _fixture.Multiplexer.GetDatabase();

        var latency = await db.PingAsync();

        Assert.True(latency >= TimeSpan.Zero);
    }
}
