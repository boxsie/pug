namespace PUG.Core;

/// <summary>
/// A successful matcher output: a set of teams ready to play.
/// </summary>
/// <remarks>
/// Always team-shaped so 1v1, NvN, FFA (each player as a single-member team), and
/// co-op (one team of N) all fit the same result type. A matcher returns
/// <c>null</c> from <see cref="IMatcher{TTicket}.TryMatchAsync"/> instead of an
/// empty <see cref="MatchResult{TTicket}"/> when no match can be formed.
/// </remarks>
public sealed record MatchResult<TTicket>(IReadOnlyList<Team<TTicket>> Teams);

/// <summary>
/// One team within a <see cref="MatchResult{TTicket}"/>.
/// </summary>
/// <param name="Index">Zero-based team index. Matches the team's position in
///   <see cref="MatchResult{TTicket}.Teams"/>; useful for assigning roles, sides,
///   or spawn points downstream.</param>
/// <param name="Members">Tickets assigned to this team. The matcher is responsible
///   for putting <c>members.Count</c> players in line with the host's team-size config.</param>
public sealed record Team<TTicket>(int Index, IReadOnlyList<TTicket> Members);
