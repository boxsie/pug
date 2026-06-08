namespace PUG.Netcode;

/// <summary>
/// The receiver-side sequence policy of a multiplexed channel. A channel is
/// defined <b>entirely</b> by what the receiver does with each packet's
/// sequence number — the sender always stamps a per-channel <c>seq</c> the same
/// way regardless of mode. See <see cref="ChannelMux"/>.
/// </summary>
public enum ChannelMode
{
    /// <summary>
    /// Deliver every packet as it arrives, in arrival order, no resequencing.
    /// For rare fire-and-forget traffic where ordering and staleness don't
    /// matter.
    /// </summary>
    Unreliable,

    /// <summary>
    /// Track the highest sequence seen and drop anything ≤ it; deliver any
    /// forward-progress packet immediately. The right policy for snapshots and
    /// inputs — an older snapshot arriving late is worthless, so it's discarded
    /// rather than delivered out of order.
    /// </summary>
    KeepLatest,

    /// <summary>
    /// Hold out-of-order arrivals in a small reorder buffer and release strictly
    /// in sequence order. For events that must not be reordered or lost —
    /// spawn/despawn, score changes, chat. On a transport that already orders
    /// (see <see cref="PeerLinkCapabilities.IsOrdered"/>) the buffer is bypassed.
    /// </summary>
    Ordered,
}
