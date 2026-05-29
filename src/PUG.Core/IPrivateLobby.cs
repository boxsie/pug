namespace PUG.Core;

/// <summary>
/// Generates and resolves short codes for private (invite-only) matches. The
/// matchmaker pairs only tickets that share a <see cref="ITicket{TPayload}.PrivateGameId"/>;
/// this interface maps from a human-shareable string to that id.
/// </summary>
/// <remarks>
/// Phase 1 ships an in-memory reference implementation;
/// Phase 2 ships a Redis-backed implementation for matchmakers that need
/// codes to survive process restarts.
/// </remarks>
public interface IPrivateLobby
{
    /// <summary>Generate a fresh code paired with a new private-game id.</summary>
    Task<(string Code, Guid PrivateGameId)> GenerateCodeAsync(CancellationToken ct);

    /// <summary>Resolve a code to its private-game id, or <c>null</c> if unknown / expired.</summary>
    Task<Guid?> ResolveCodeAsync(string code, CancellationToken ct);

    /// <summary>Drop a code so subsequent <see cref="ResolveCodeAsync"/> calls return <c>null</c>.</summary>
    Task ExpireCodeAsync(string code, CancellationToken ct);
}
