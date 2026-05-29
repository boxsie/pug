using System.Text.Json;
using PUG.Core;
using StackExchange.Redis;

namespace PUG.Redis;

/// <summary>
/// Redis-backed <see cref="ISessionStore{TSession}"/> with versioned JSON
/// payloads, expiring keys, and lock-wrapped read-modify-write updates.
/// </summary>
/// <remarks>
/// <para>
/// Key format <c>pug:session:{typeof(T).Name}:{session.Id}</c> — sharing a
/// Redis instance across multiple session types is safe because the type
/// name partitions the keyspace. The lock key sits in a sibling namespace
/// (<c>pug:session-lock:{typeName}:{id}</c>) so it survives the session's
/// own removal cleanly.
/// </para>
/// <para>
/// The <c>where T : class, IVersioned, new()</c> constraint is intentional:
/// </para>
/// <list type="bullet">
///   <item><description><c>IVersioned</c> exposes <c>Id</c> (so SaveAsync
///     doesn't need a separate id parameter) and <c>Version</c> (bumped
///     by <see cref="UpdateAsync"/> on every successful update).</description></item>
///   <item><description><c>new()</c> isn't currently used but is the cheap
///     default if a future Get-or-create overload lands.</description></item>
///   <item><description><c>class</c> sidesteps the value-type quirks of
///     JSON deserialisation defaults.</description></item>
/// </list>
/// </remarks>
public sealed class RedisSessionStore<T> : ISessionStore<T>
    where T : class, IVersioned, new()
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDistributedLock _lock;
    private readonly TimeSpan _ttl;
    private readonly string _typeName;

    /// <summary>Build a session store.</summary>
    /// <param name="multiplexer">Connection to Redis.</param>
    /// <param name="distributedLock">Lock used to serialise concurrent
    ///   <see cref="UpdateAsync"/> calls on the same session id.</param>
    /// <param name="ttl">Time-to-live applied (and refreshed) on every
    ///   save. Defaults to 2 hours.</param>
    /// <param name="typeNameOverride">Optional override for the key
    ///   namespace; defaults to <c>typeof(T).Name</c>.</param>
    public RedisSessionStore(
        IConnectionMultiplexer multiplexer,
        IDistributedLock distributedLock,
        TimeSpan? ttl = null,
        string? typeNameOverride = null)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        ArgumentNullException.ThrowIfNull(distributedLock);

        _multiplexer = multiplexer;
        _lock = distributedLock;
        _ttl = ttl ?? TimeSpan.FromHours(2);
        _typeName = typeNameOverride ?? typeof(T).Name;
    }

    /// <inheritdoc/>
    public async Task<T?> GetAsync(string id, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(id);
        var key = SessionKey(id);
        var db = _multiplexer.GetDatabase();
        var payload = await db.StringGetAsync(key).ConfigureAwait(false);
        if (payload.IsNullOrEmpty)
        {
            return null;
        }

        return JsonSerializer.Deserialize<T>((byte[])payload!, JsonOptions);
    }

    /// <inheritdoc/>
    public async Task SaveAsync(T session, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (string.IsNullOrEmpty(session.Id))
        {
            throw new ArgumentException("Session.Id must be non-empty.", nameof(session));
        }

        var key = SessionKey(session.Id);
        var payload = JsonSerializer.SerializeToUtf8Bytes(session, JsonOptions);
        var db = _multiplexer.GetDatabase();
        await db.StringSetAsync(key, payload, expiry: _ttl).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<T?> UpdateAsync(string id, Func<T, Task<bool>> update, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(id);
        ArgumentNullException.ThrowIfNull(update);

        return await _lock.ExecuteAsync<T?>(
            LockKey(id),
            async () =>
            {
                var session = await GetAsync(id, ct).ConfigureAwait(false);
                if (session is null)
                {
                    return null;
                }

                var shouldPersist = await update(session).ConfigureAwait(false);
                if (!shouldPersist)
                {
                    return session;
                }

                session.Version += 1;
                await SaveAsync(session, ct).ConfigureAwait(false);
                return session;
            }).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task RemoveAsync(string id, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(id);
        // A batch (rather than a transaction) is right here: the only key
        // we own is the session key, but we batch in case a future change
        // attaches side keys (per-session counters, indexes). Batch is
        // single round-trip; the keys are independent so MULTI/EXEC's
        // atomicity isn't needed.
        var db = _multiplexer.GetDatabase();
        await db.KeyDeleteAsync(new RedisKey[] { SessionKey(id) }).ConfigureAwait(false);
    }

    private string SessionKey(string id) => $"pug:session:{_typeName}:{id}";

    private string LockKey(string id) => $"session-lock:{_typeName}:{id}";
}
