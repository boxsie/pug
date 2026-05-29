namespace PUG.Core;

/// <summary>
/// Validates an inbound matchmaking payload before it lands in the queue. Hosts
/// use this to reject malformed entries, enforce per-player limits, or check
/// signature on tamper-sensitive payloads (e.g. self-reported rank).
/// </summary>
public interface IPayloadVerifier<in TPayload>
{
    /// <summary>
    /// Decide whether the payload should be accepted for <paramref name="playerId"/>.
    /// </summary>
    /// <returns><c>true</c> to accept; <c>false</c> to reject the queue request.</returns>
    Task<bool> VerifyAsync(Guid playerId, TPayload payload, CancellationToken ct);
}
