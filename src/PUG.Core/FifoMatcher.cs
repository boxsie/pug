namespace PUG.Core;

/// <summary>
/// The reference <see cref="IMatcher{TTicket}"/> — pairs the oldest waiting
/// tickets into teams. Strategy-free: no rank, no region, no skill
/// consideration. The only matcher shipped in <c>PUG.Core</c>; ranked /
/// skill-aware matchers live in extension packages.
/// </summary>
/// <remarks>
/// <para>
/// FIFO ordering comes from <see cref="IQueue{TTicket}.PeekOldestAsync"/>,
/// which the implementation defines as oldest-first. The matcher itself is
/// purely read-only against the queue — the hosting service removes matched
/// tickets via <see cref="IQueue{TTicket}.RemoveAsync"/> after the result
/// is consumed.
/// </para>
/// <para>
/// Tickets carrying a non-null <see cref="ITicket{TPayload}.PrivateGameId"/>
/// are partitioned away from the public queue and from other private groups:
/// the matcher only pairs tickets that share the same <c>PrivateGameId</c>
/// (or all-null, for public tickets). Private partitions are tried first,
/// oldest-waiter first, so a code waiting 10 minutes doesn't get starved by
/// a public queue that forms every second.
/// </para>
/// <para>
/// v0.1 supports only <em>symmetric</em> team sizes (all entries in
/// <c>teamSizes</c> equal). Asymmetric NvM is on the v0.x roadmap and would
/// change the round-robin distribution rule. The constructor rejects
/// non-uniform team sizes outright so the failure mode is loud and early.
/// </para>
/// </remarks>
public sealed class FifoMatcher<TTicket> : IMatcher<TTicket>
    where TTicket : ITicket<object?>
{
    private readonly IQueue<TTicket> _queue;
    private readonly IReadOnlyList<int> _teamSizes;
    private readonly int _required;

    /// <summary>
    /// Build a FIFO matcher.
    /// </summary>
    /// <param name="queue">Source of waiting tickets. Implementations must return
    ///   tickets oldest-first from <see cref="IQueue{TTicket}.PeekOldestAsync"/>.</param>
    /// <param name="teamSizes">Per-team member counts. Must be non-empty, every
    ///   element ≥ 1, and (v0.1) all elements equal.</param>
    /// <exception cref="ArgumentNullException"><paramref name="queue"/> or
    ///   <paramref name="teamSizes"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="teamSizes"/> is empty
    ///   or contains non-uniform values.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element of
    ///   <paramref name="teamSizes"/> is less than 1.</exception>
    public FifoMatcher(IQueue<TTicket> queue, IReadOnlyList<int> teamSizes)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(teamSizes);

        if (teamSizes.Count == 0)
        {
            throw new ArgumentException("teamSizes must contain at least one team.", nameof(teamSizes));
        }

        var first = teamSizes[0];
        for (var i = 0; i < teamSizes.Count; i++)
        {
            if (teamSizes[i] < 1)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(teamSizes),
                    teamSizes[i],
                    "Every team size must be at least 1.");
            }

            if (teamSizes[i] != first)
            {
                throw new ArgumentException(
                    "v0.1 supports only symmetric team sizes (all entries equal). " +
                    "Asymmetric NvM is on the v0.x roadmap.",
                    nameof(teamSizes));
            }
        }

        _queue = queue;
        _teamSizes = teamSizes;
        _required = teamSizes.Sum();
    }

    /// <inheritdoc/>
    public async Task<MatchResult<TTicket>?> TryMatchAsync(CancellationToken ct)
    {
        var count = await _queue.CountAsync(ct).ConfigureAwait(false);
        if (count < _required)
        {
            return null;
        }

        // Peek the full queue so we can group tickets by PrivateGameId. A
        // partition's eligible tickets may be interleaved with other-game-id
        // and public tickets, so the cheap "peek the oldest _required" shape
        // doesn't work — we need the whole picture to partition correctly.
        // For an O(N) Redis-backed queue this is still one round-trip per
        // tick; the price is paid in bytes, not RTTs.
        var all = await _queue.PeekOldestAsync(count, ct).ConfigureAwait(false);
        // Defensive: a concurrent dequeue could have shrunk the queue.
        if (all.Count < _required)
        {
            return null;
        }

        // Partition by PrivateGameId. Each partition keeps the queue's
        // oldest-first ordering by construction (we iterate `all` in order).
        var privatePartitions = new Dictionary<Guid, List<TTicket>>();
        var publicPartition = new List<TTicket>();
        foreach (var ticket in all)
        {
            if (ticket.PrivateGameId is { } gameId)
            {
                if (!privatePartitions.TryGetValue(gameId, out var list))
                {
                    privatePartitions[gameId] = list = new List<TTicket>();
                }

                list.Add(ticket);
            }
            else
            {
                publicPartition.Add(ticket);
            }
        }

        // Try private partitions first, oldest-waiter-first so a long-waiting
        // private code isn't starved by a younger one that happens to fill up
        // first. The first eligible partition wins this tick; the hosting
        // service will call us again after removing matched tickets, at which
        // point any remaining partitions (including public) get their shot.
        foreach (var partition in privatePartitions.Values.OrderBy(p => p[0].EnqueuedAt))
        {
            if (partition.Count >= _required)
            {
                return BuildMatch(partition);
            }
        }

        if (publicPartition.Count >= _required)
        {
            return BuildMatch(publicPartition);
        }

        return null;
    }

    private MatchResult<TTicket> BuildMatch(IReadOnlyList<TTicket> picked)
    {
        var teamCount = _teamSizes.Count;
        var members = new List<TTicket>[teamCount];
        for (var t = 0; t < teamCount; t++)
        {
            members[t] = new List<TTicket>(_teamSizes[t]);
        }

        for (var i = 0; i < _required; i++)
        {
            members[i % teamCount].Add(picked[i]);
        }

        var teams = new Team<TTicket>[teamCount];
        for (var t = 0; t < teamCount; t++)
        {
            teams[t] = new Team<TTicket>(t, members[t]);
        }

        return new MatchResult<TTicket>(teams);
    }
}
