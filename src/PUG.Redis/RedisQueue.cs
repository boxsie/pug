using System.Text.Json;
using PUG.Core;
using StackExchange.Redis;

namespace PUG.Redis;

/// <summary>
/// Redis-backed <see cref="IQueue{TTicket}"/> implemented as a ZSET (score =
/// <see cref="ITicket{TPayload}.EnqueuedAt"/> in Unix milliseconds, member =
/// player-id) plus a side hash holding the JSON-serialised tickets.
/// </summary>
/// <remarks>
/// <para>
/// Why ZSET, not list:
/// </para>
/// <list type="bullet">
///   <item><description>Time-windowed reads (timed-out sweeps, "oldest N")
///     are <c>ZRANGEBYSCORE</c> / <c>ZRANGE</c> — O(log N + window-size).</description></item>
///   <item><description>Indexed removes (<c>ZREM</c> by member) are O(log N);
///     a list would force scan-and-delete.</description></item>
///   <item><description>The score is the EnqueuedAt time so we don't have to
///     parse JSON to make ordering or timeout decisions.</description></item>
/// </list>
/// <para>
/// Enqueue and remove are wrapped in a single MULTI/EXEC transaction so the
/// ZSET and the side hash stay consistent. A crash between the two commands
/// is impossible — either both land or neither.
/// </para>
/// <para>
/// Member identity is <see cref="ITicket{TPayload}.PlayerId"/> rendered as the
/// <c>"N"</c> hex format (32 chars, no dashes). Core's ticket model is "one
/// in-flight ticket per player" — re-enqueueing the same player overwrites
/// their existing ticket in place (same member, new score / payload).
/// </para>
/// </remarks>
public sealed class RedisQueue<TTicket> : IQueue<TTicket>
    where TTicket : ITicket<object?>
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly string _queueKey;
    private readonly string _hashKey;

    /// <summary>Build a queue named <paramref name="queueName"/>.</summary>
    /// <param name="multiplexer">Connection to Redis.</param>
    /// <param name="queueName">Logical queue name. May embed region or game-mode
    ///   partitions (e.g. <c>"public:eu-west"</c>) — the queue does not
    ///   interpret the string.</param>
    public RedisQueue(IConnectionMultiplexer multiplexer, string queueName)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        if (string.IsNullOrWhiteSpace(queueName))
        {
            throw new ArgumentException("Queue name must be non-empty.", nameof(queueName));
        }

        _multiplexer = multiplexer;
        _queueKey = $"pug:queue:{queueName}";
        _hashKey = $"pug:queue:{queueName}:tickets";
    }

    /// <inheritdoc/>
    public async Task EnqueueAsync(TTicket ticket, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(ticket);

        var member = MemberKey(ticket.PlayerId);
        var score = ToUnixMs(ticket.EnqueuedAt);
        var payload = JsonSerializer.SerializeToUtf8Bytes(ticket, JsonOptions);

        var db = _multiplexer.GetDatabase();
        var tx = db.CreateTransaction();
        _ = tx.SortedSetAddAsync(_queueKey, member, score);
        _ = tx.HashSetAsync(_hashKey, member, payload);
        if (!await tx.ExecuteAsync().ConfigureAwait(false))
        {
            throw new InvalidOperationException(
                $"Redis MULTI/EXEC for enqueue on '{_queueKey}' did not execute. This usually " +
                "indicates a connection drop mid-transaction — retry the enqueue.");
        }
    }

    /// <inheritdoc/>
    public async Task<IReadOnlyList<TTicket>> PeekOldestAsync(int count, CancellationToken ct)
    {
        if (count < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "count must be >= 0.");
        }
        if (count == 0)
        {
            return Array.Empty<TTicket>();
        }

        var db = _multiplexer.GetDatabase();
        var members = await db.SortedSetRangeByRankAsync(_queueKey, 0, count - 1).ConfigureAwait(false);
        return await FetchPayloadsAsync(members).ConfigureAwait(false);
    }

    /// <summary>
    /// Peek every ticket whose <see cref="ITicket{TPayload}.EnqueuedAt"/> is
    /// older than <c>now - <paramref name="timeout"/></c>. Useful for sweep
    /// jobs that need to notify, re-route, or abandon long-waiters.
    /// </summary>
    public async Task<IReadOnlyList<TTicket>> PeekTimedOutAsync(TimeSpan timeout, CancellationToken ct)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), timeout, "timeout must be >= 0.");
        }

        var threshold = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)timeout.TotalMilliseconds;
        var db = _multiplexer.GetDatabase();
        var members = await db.SortedSetRangeByScoreAsync(
            _queueKey,
            start: double.NegativeInfinity,
            stop: threshold,
            exclude: Exclude.None).ConfigureAwait(false);

        return await FetchPayloadsAsync(members).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async Task<int> CountAsync(CancellationToken ct)
    {
        var db = _multiplexer.GetDatabase();
        return checked((int)await db.SortedSetLengthAsync(_queueKey).ConfigureAwait(false));
    }

    /// <inheritdoc/>
    public async Task RemoveAsync(Guid playerId, CancellationToken ct)
    {
        var member = MemberKey(playerId);
        var db = _multiplexer.GetDatabase();
        var tx = db.CreateTransaction();
        _ = tx.SortedSetRemoveAsync(_queueKey, member);
        _ = tx.HashDeleteAsync(_hashKey, member);
        // Don't fail if EXEC reports false (e.g. WATCH abort) — RemoveAsync's
        // contract is "no-op if absent", which covers the race too.
        _ = await tx.ExecuteAsync().ConfigureAwait(false);
    }

    /// <summary>Stats snapshot — count and oldest wait. Cheap to call.</summary>
    public async Task<QueueStats> GetStatsAsync(CancellationToken ct)
    {
        var db = _multiplexer.GetDatabase();
        var count = checked((int)await db.SortedSetLengthAsync(_queueKey).ConfigureAwait(false));

        if (count == 0)
        {
            return new QueueStats(0, OldestWait: null);
        }

        var oldest = await db.SortedSetRangeByRankWithScoresAsync(_queueKey, 0, 0).ConfigureAwait(false);
        if (oldest.Length == 0)
        {
            return new QueueStats(count, OldestWait: null);
        }

        var oldestEnqueuedMs = (long)oldest[0].Score;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var wait = TimeSpan.FromMilliseconds(Math.Max(0, nowMs - oldestEnqueuedMs));
        return new QueueStats(count, wait);
    }

    private async Task<IReadOnlyList<TTicket>> FetchPayloadsAsync(RedisValue[] members)
    {
        if (members.Length == 0)
        {
            return Array.Empty<TTicket>();
        }

        var db = _multiplexer.GetDatabase();
        var payloads = await db.HashGetAsync(_hashKey, members).ConfigureAwait(false);

        var result = new List<TTicket>(payloads.Length);
        foreach (var raw in payloads)
        {
            // A ZSET member without a side-hash payload is a torn-write artefact
            // we tolerate (e.g. concurrent remove from another caller observed
            // the ZREM landed but our HMGET ran before the matching HDEL).
            if (raw.IsNullOrEmpty)
            {
                continue;
            }

            var ticket = JsonSerializer.Deserialize<TTicket>((byte[])raw!, JsonOptions);
            if (ticket is not null)
            {
                result.Add(ticket);
            }
        }

        return result;
    }

    private static RedisValue MemberKey(Guid playerId) => playerId.ToString("N");

    private static double ToUnixMs(DateTime when)
    {
        var dto = when.Kind switch
        {
            DateTimeKind.Utc => new DateTimeOffset(when, TimeSpan.Zero),
            DateTimeKind.Local => new DateTimeOffset(when),
            DateTimeKind.Unspecified => new DateTimeOffset(DateTime.SpecifyKind(when, DateTimeKind.Utc), TimeSpan.Zero),
            _ => new DateTimeOffset(when),
        };
        return dto.ToUnixTimeMilliseconds();
    }
}
