namespace PUG.Core;

/// <summary>
/// A pluggable matchmaking strategy. Implementations decide which set of waiting
/// tickets forms a viable match and how they distribute into teams.
/// </summary>
/// <remarks>
/// <para>
/// Implementations should be safe to call from a single background loop on a
/// regular tick; they're not expected to be re-entrant across overlapping calls
/// on the same matcher instance.
/// </para>
/// <para>
/// A matcher is <em>read-only</em> against its underlying queue: it must not
/// dequeue matched tickets. The caller (typically a hosting service) removes
/// matched tickets after the result is consumed so failed downstream work doesn't
/// drop players from the queue.
/// </para>
/// </remarks>
public interface IMatcher<TTicket>
{
    /// <summary>
    /// Attempt to form a match.
    /// </summary>
    /// <returns>A populated <see cref="MatchResult{TTicket}"/> when a viable match
    ///   exists, or <c>null</c> when none can be formed this tick.</returns>
    Task<MatchResult<TTicket>?> TryMatchAsync(CancellationToken ct);
}
