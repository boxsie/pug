namespace PUG.Ensemble;

/// <summary>
/// Outcome of a successful <see cref="QueueHandle{TPayload}.WaitForMatchAsync"/>.
/// Carries the matchmaker-issued session id and the set of introduced peers
/// the player should treat as match participants.
/// </summary>
/// <param name="SessionId">The matchmaker-issued session id this match
///   belongs to. Matches <see cref="QueueHandle{TPayload}.SessionId"/>.</param>
/// <param name="Peers">Per-peer dial outcomes. Each entry records the peer's
///   Ensemble service address and whether the SDK's eager service-identity
///   dial (<c>ConnectPeerAsync</c>) was enqueued on the daemon. Game code may
///   choose to retry connects for entries with
///   <see cref="PeerEndpoint.Connected"/>=<c>false</c>.</param>
/// <param name="RoleHint">Free-text role hint stamped by the matchmaker
///   (e.g. <c>"host"</c>, <c>"client"</c>). <c>null</c> when the matchmaker
///   left the hint empty.</param>
public sealed record MatchFound(
    string SessionId,
    IReadOnlyList<PeerEndpoint> Peers,
    string? RoleHint);

/// <summary>
/// A single peer the player was introduced to as part of a match. The SDK
/// eagerly dials each peer signed as this player service via
/// <c>RegisteredService.ConnectPeerAsync</c>; <see cref="Connected"/> reports
/// whether that dial was enqueued on the daemon (the handshake itself is
/// fire-and-forget). A <c>false</c> here is not fatal — game code can retry,
/// and the symmetric dial from the peer establishes the path either way.
/// </summary>
public sealed record PeerEndpoint(string EnsembleAddr, bool Connected);
