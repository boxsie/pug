namespace PUG.Core;

/// <summary>
/// A queued matchmaking request from a single player.
/// </summary>
/// <remarks>
/// Core is deliberately unopinionated about matching strategy:
/// <list type="bullet">
///   <item><description>No rank — ranked matchers live in <c>PUG.Matchmaking.Ranked</c>.</description></item>
///   <item><description>No region — region routing is matcher- or deployment-specific.</description></item>
///   <item><description>No payload schema — hosts ship whatever they need via <typeparamref name="TPayload"/>.</description></item>
/// </list>
/// </remarks>
/// <typeparam name="TPayload">Host-defined per-request payload (character picks, party
/// rosters, map preferences, etc.). Carried opaquely through the pipeline so hosts
/// avoid a downstream lookup at match time.</typeparam>
public interface ITicket<out TPayload>
{
    /// <summary>The player who enqueued this ticket. Stable across reconnects.</summary>
    Guid PlayerId { get; }

    /// <summary>When the player joined the queue. Used by FIFO matchers for oldest-first priority.</summary>
    DateTime EnqueuedAt { get; }

    /// <summary>Opaque host-supplied payload travelling with the ticket.</summary>
    TPayload Payload { get; }

    /// <summary>
    /// If set, this ticket can only be paired with other tickets that share the same
    /// private-game id. Set when joining a private lobby via a short code; null for
    /// public-queue tickets.
    /// </summary>
    Guid? PrivateGameId { get; }
}
