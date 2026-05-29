using StackExchange.Redis;

namespace PUG.Redis;

/// <summary>
/// Maps a player id to their currently-active transport connection ids
/// (typically SignalR connection ids, but the contract is opaque). One player
/// may hold multiple connections at once — a player can be in a lobby on
/// their phone while the game is running on PC, or have a second browser
/// tab open — so the mapping is one-to-many.
/// </summary>
/// <remarks>
/// <para>
/// Backed by Redis SETs (one set per player id, key
/// <c>pug:conn:{playerId}</c>). The set is sized at a configurable TTL so a
/// matchmaker crash mid-shutdown can't leave stale connection ids in Redis
/// forever — the set will evict itself within the TTL window. Production
/// callers should still emit explicit <see cref="RemoveAsync"/> calls on
/// disconnect; the TTL is a defence in depth, not the primary cleanup.
/// </para>
/// <para>
/// When the last connection for a player is removed, the entire key is
/// deleted — keeps Redis' keyspace tidy and prevents an empty-set ghost
/// from holding a TTL.
/// </para>
/// </remarks>
public sealed class RedisConnectionMap
{
    private const string KeyPrefix = "pug:conn:";

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly TimeSpan? _ttl;

    /// <summary>Build a connection map.</summary>
    /// <param name="multiplexer">Connection to Redis.</param>
    /// <param name="ttl">Optional TTL on the per-player set. <c>null</c>
    ///   disables auto-eviction (production usually wants 1 hour or so;
    ///   tests use null or a short value for determinism).</param>
    public RedisConnectionMap(IConnectionMultiplexer multiplexer, TimeSpan? ttl = null)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        _multiplexer = multiplexer;
        _ttl = ttl;
    }

    /// <summary>Record that <paramref name="connectionId"/> belongs to <paramref name="playerId"/>.</summary>
    public async Task AddAsync(Guid playerId, string connectionId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(connectionId);
        if (connectionId.Length == 0)
        {
            throw new ArgumentException("Connection id must be non-empty.", nameof(connectionId));
        }

        var key = KeyOf(playerId);
        var db = _multiplexer.GetDatabase();

        // SADD then (optional) EXPIRE. Two round-trips; cheap and consistent —
        // if the EXPIRE drops because the connection vanishes between calls,
        // the next AddAsync re-stamps the TTL.
        await db.SetAddAsync(key, connectionId).ConfigureAwait(false);
        if (_ttl is { } ttl)
        {
            await db.KeyExpireAsync(key, ttl).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Remove <paramref name="connectionId"/> from <paramref name="playerId"/>.
    /// If the set becomes empty, the key is deleted entirely.
    /// </summary>
    public async Task RemoveAsync(Guid playerId, string connectionId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(connectionId);

        var key = KeyOf(playerId);
        var db = _multiplexer.GetDatabase();
        await db.SetRemoveAsync(key, connectionId).ConfigureAwait(false);

        if (await db.SetLengthAsync(key).ConfigureAwait(false) == 0)
        {
            await db.KeyDeleteAsync(key).ConfigureAwait(false);
        }
    }

    /// <summary>Return every connection currently held for <paramref name="playerId"/>.</summary>
    public async Task<IReadOnlyList<string>> GetAsync(Guid playerId, CancellationToken ct)
    {
        var key = KeyOf(playerId);
        var db = _multiplexer.GetDatabase();
        var members = await db.SetMembersAsync(key).ConfigureAwait(false);
        return members.Select(m => (string)m!).ToList();
    }

    /// <summary><c>true</c> iff <paramref name="playerId"/> has at least one active connection.</summary>
    public async Task<bool> IsOnlineAsync(Guid playerId, CancellationToken ct)
    {
        var key = KeyOf(playerId);
        var db = _multiplexer.GetDatabase();
        return await db.SetLengthAsync(key).ConfigureAwait(false) > 0;
    }

    private static string KeyOf(Guid playerId) => KeyPrefix + playerId.ToString("N");
}
