namespace PUG.Ensemble;

/// <summary>
/// An inbound game-time message from a matched peer. Surfaced through
/// <see cref="QueueHandle{TPayload}.PeerMessages"/>; constructed by the
/// SDK when the daemon delivers a <c>ServiceEvent.RpcMessage</c> whose
/// <c>FromAddr</c> is NOT the matchmaker the handle queued at.
/// </summary>
/// <param name="FromAddr">The peer's Ensemble service address — the same
///   string the caller used (or will use) with
///   <see cref="QueueHandle{TPayload}.SendToPeerAsync"/>.</param>
/// <param name="Bytes">Raw payload as the daemon delivered it. Wraps the
///   underlying <c>byte[]</c> without a copy. Consumers typically feed
///   this into a proto <c>Parser.ParseFrom(ReadOnlySpan&lt;byte&gt;)</c>
///   call.</param>
/// <param name="Arrived">Wall-clock time the SDK observed the inbound
///   event. Useful for game-time latency budgeting and stale-message
///   suppression.</param>
public sealed record PeerMessage(string FromAddr, ReadOnlyMemory<byte> Bytes, DateTimeOffset Arrived);
