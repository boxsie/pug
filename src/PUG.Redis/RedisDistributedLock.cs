using PUG.Core;
using StackExchange.Redis;

namespace PUG.Redis;

/// <summary>
/// Redis-backed <see cref="IDistributedLock"/>. <c>SET NX EX</c> acquire with
/// per-attempt jitter on retry, owner-checked atomic release via a Lua script.
/// </summary>
/// <remarks>
/// <para>
/// Acquire uses <c>SET key &lt;owner-token&gt; NX EX &lt;timeout-seconds&gt;</c>
/// where <c>owner-token</c> is a fresh per-acquire <see cref="Guid"/>. Release
/// is an atomic Lua script (<c>get</c>-then-<c>del</c>-if-equal) so we never
/// delete a lock we no longer own — between our acquire and our release, the
/// lock's TTL might have expired server-side and a different process might
/// have legitimately re-acquired the same key. A blind <c>DEL</c> would steal
/// their lock; the Lua check-then-delete prevents that.
/// </para>
/// <para>
/// On unsuccessful acquire the loop sleeps for
/// <c>retryDelayMs + Random.Shared.Next(0, 50)</c> milliseconds (jitter) and
/// retries. The total budget is <c>retryCount + 1</c> attempts (one initial
/// plus <c>retryCount</c> retries) — matches the <see cref="IDistributedLock"/>
/// contract's "retries" interpretation.
/// </para>
/// <para>
/// If the budget is exhausted without acquiring, <see cref="ExecuteAsync"/>
/// returns <c>default(T?)</c>; it does NOT throw. Callers distinguish "no
/// match yet" / "value not found" / "we couldn't get the lock" via the
/// nullable result.
/// </para>
/// </remarks>
public sealed class RedisDistributedLock : IDistributedLock
{
    private const string KeyPrefix = "pug:lock:";

    // Atomic owner-checked release. KEYS[1] = lock key, ARGV[1] = owner token.
    // Returns 1 if the lock was held by us and was deleted; 0 otherwise.
    private const string ReleaseScript =
        "if redis.call('get', KEYS[1]) == ARGV[1] then " +
        "  return redis.call('del', KEYS[1]) " +
        "else " +
        "  return 0 " +
        "end";

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly TimeSpan _defaultTimeout;

    /// <summary>Build a Redis-backed lock against <paramref name="multiplexer"/>.</summary>
    /// <param name="multiplexer">Connection to the Redis cluster / instance.</param>
    /// <param name="defaultTimeout">Lock TTL when <see cref="ExecuteAsync"/> is
    ///   called with <c>timeout = null</c>. Defaults to 10 seconds.</param>
    public RedisDistributedLock(IConnectionMultiplexer multiplexer, TimeSpan? defaultTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        _multiplexer = multiplexer;
        _defaultTimeout = defaultTimeout ?? TimeSpan.FromSeconds(10);
    }

    /// <inheritdoc/>
    public async Task<T?> ExecuteAsync<T>(
        string key,
        Func<Task<T>> action,
        TimeSpan? timeout = null,
        int retryCount = 3,
        int retryDelayMs = 100)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(action);
        if (retryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryCount), retryCount, "retryCount must be >= 0.");
        }
        if (retryDelayMs < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryDelayMs), retryDelayMs, "retryDelayMs must be >= 0.");
        }

        var lockKey = KeyPrefix + key;
        var ownerToken = Guid.NewGuid().ToString("N");
        var ttl = timeout ?? _defaultTimeout;
        var db = _multiplexer.GetDatabase();

        var acquired = false;
        for (var attempt = 0; attempt <= retryCount; attempt++)
        {
            if (await db.StringSetAsync(lockKey, ownerToken, expiry: ttl, when: When.NotExists).ConfigureAwait(false))
            {
                acquired = true;
                break;
            }

            if (attempt < retryCount)
            {
                var jitter = Random.Shared.Next(0, 50);
                await Task.Delay(retryDelayMs + jitter).ConfigureAwait(false);
            }
        }

        if (!acquired)
        {
            return default;
        }

        try
        {
            return await action().ConfigureAwait(false);
        }
        finally
        {
            await TryReleaseAsync(key, ownerToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Release a lock only if the supplied <paramref name="ownerToken"/> still
    /// matches the value stored in Redis. Internal — exposed for adversarial
    /// tests that need to probe the owner-mismatch path. Returns <c>true</c>
    /// when the lock was held by us and was deleted, <c>false</c> otherwise
    /// (key absent, or held by a different owner).
    /// </summary>
    internal async Task<bool> TryReleaseAsync(string key, string ownerToken)
    {
        var lockKey = KeyPrefix + key;
        var db = _multiplexer.GetDatabase();

        var result = await db.ScriptEvaluateAsync(
            ReleaseScript,
            new RedisKey[] { lockKey },
            new RedisValue[] { ownerToken }).ConfigureAwait(false);

        return (long)result == 1;
    }
}
