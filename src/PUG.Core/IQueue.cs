namespace PUG.Core;

/// <summary>
/// A pluggable ordered ticket store backing a matchmaker.
/// </summary>
/// <remarks>
/// <para>
/// Provided as a core abstraction (not just a matcher input) because Phase 2
/// ships a Redis ZSET-backed implementation that exposes the same operations
/// over a persistent store. Hosts mix in-memory and Redis implementations
/// behind this interface.
/// </para>
/// <para>
/// Implementations are expected to expose peek operations that don't mutate
/// the queue — the matcher reads tickets here; the hosting service removes
/// matched tickets via <see cref="RemoveAsync"/>.
/// </para>
/// </remarks>
public interface IQueue<TTicket>
{
    /// <summary>Append a ticket to the queue.</summary>
    Task EnqueueAsync(TTicket ticket, CancellationToken ct);

    /// <summary>Peek up to <paramref name="count"/> oldest tickets without removing them.</summary>
    /// <returns>Oldest-first; fewer than <paramref name="count"/> if the queue is smaller.</returns>
    Task<IReadOnlyList<TTicket>> PeekOldestAsync(int count, CancellationToken ct);

    /// <summary>Current queue size.</summary>
    Task<int> CountAsync(CancellationToken ct);

    /// <summary>Remove the ticket owned by <paramref name="playerId"/>, if present. No-op otherwise.</summary>
    Task RemoveAsync(Guid playerId, CancellationToken ct);
}
