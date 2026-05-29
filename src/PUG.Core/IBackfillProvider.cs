namespace PUG.Core;

/// <summary>
/// Optional hook: when a player has been waiting in queue past some threshold,
/// the host can offer them a non-matchmaker pairing (bot, in-progress match,
/// custom lobby). The matcher remains agnostic about what "backfill" means.
/// </summary>
public interface IBackfillProvider<TTicket>
{
    /// <summary>
    /// Try to satisfy <paramref name="waitingPlayer"/> outside the normal queue
    /// pipeline.
    /// </summary>
    /// <returns><c>true</c> if the player has been placed and the ticket can
    /// be removed from the queue; <c>false</c> if no backfill was available.</returns>
    Task<bool> TryBackfillAsync(TTicket waitingPlayer, CancellationToken ct);
}
