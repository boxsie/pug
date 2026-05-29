using StackExchange.Redis;
using Testcontainers.Redis;

namespace PUG.Redis.Tests;

/// <summary>
/// xunit collection fixture that spins up a Redis 7 container via Testcontainers
/// and exposes an <see cref="IConnectionMultiplexer"/> shared across the
/// collection's tests. Each test class should call <see cref="ResetAsync"/>
/// in its constructor (or per-test) to ensure a clean keyspace.
/// </summary>
/// <remarks>
/// <para>
/// On boxsie, Docker Desktop is typically inactive but Podman is installed
/// with a user-mode socket. Testcontainers honours <c>DOCKER_HOST</c>; if
/// neither runtime is reachable, the fixture's <see cref="InitializeAsync"/>
/// throws and tests in the collection fail with a clear "container start
/// failed" error rather than masquerading as production-code failures.
/// </para>
/// <para>
/// Tests that consume this fixture should be tagged
/// <c>[Trait("Category", "Integration")]</c> so unit-only runs can filter
/// them out via <c>--filter "Category!=Integration"</c>.
/// </para>
/// </remarks>
public sealed class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    public IConnectionMultiplexer Multiplexer { get; private set; } = null!;

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        var config = ConfigurationOptions.Parse(ConnectionString);
        // Tests must not block on transient Redis startup hiccups; the
        // container is healthy before we touch it, but the multiplexer's
        // default 5s connect timeout is plenty.
        config.AbortOnConnectFail = false;
        // FLUSHDB in ResetAsync is an admin command and is refused by the
        // multiplexer unless we opt in here. Production callers must NOT
        // enable this; it's a test-fixture-only choice.
        config.AllowAdmin = true;
        Multiplexer = await ConnectionMultiplexer.ConnectAsync(config);
    }

    public async Task DisposeAsync()
    {
        if (Multiplexer is not null)
        {
            await Multiplexer.CloseAsync();
            await Multiplexer.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    /// <summary>FLUSHDB so subsequent tests start with an empty keyspace.</summary>
    public async Task ResetAsync()
    {
        var endpoint = Multiplexer.GetEndPoints().Single();
        var server = Multiplexer.GetServer(endpoint);
        await server.FlushDatabaseAsync();
    }
}

/// <summary>
/// Tags tests that share <see cref="RedisFixture"/>. xunit serialises
/// collection-tagged classes so we don't pay for multiple containers in
/// parallel — Testcontainers startup is the slowest part of the run.
/// </summary>
[CollectionDefinition(Name)]
public sealed class RedisCollection : ICollectionFixture<RedisFixture>
{
    public const string Name = "Redis";
}
