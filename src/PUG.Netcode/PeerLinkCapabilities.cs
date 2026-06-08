namespace PUG.Netcode;

/// <summary>
/// Delivery guarantees a transport offers, as composable flags. The tier-A
/// channel layer reads these to decide how much of its own reliability /
/// sequencing machinery it needs to add — e.g. it can skip a redundant
/// reliable-ordered channel when the transport already provides one.
/// </summary>
[Flags]
public enum PeerLinkGuarantees
{
    /// <summary>Best-effort datagrams: may be dropped, duplicated, or reordered.</summary>
    None = 0,

    /// <summary>Every payload is delivered (eventually) or the link fails — no silent drops.</summary>
    Reliable = 1 << 0,

    /// <summary>Payloads are delivered in send order.</summary>
    Ordered = 1 << 1,
}

/// <summary>
/// Static traits of an <see cref="IPeerLink"/>'s underlying transport: what it
/// guarantees about delivery, and the largest payload a single
/// <see cref="IPeerLink.SendAsync"/> accepts. Deliberately holds <b>no live
/// metrics</b> — RTT and packet loss change moment to moment and are estimated
/// by the tier-A channel layer from its own acks, not surfaced here.
/// </summary>
/// <param name="Guarantees">The transport's delivery guarantees.</param>
/// <param name="MaxPayloadBytes">Largest payload a single send accepts, or
///   <c>null</c> when the transport imposes no practical per-message limit
///   (e.g. a reliable stream). The tier-A layer fragments above this.</param>
public readonly record struct PeerLinkCapabilities(
    PeerLinkGuarantees Guarantees,
    int? MaxPayloadBytes = null)
{
    /// <summary>True when the transport never silently drops a payload.</summary>
    public bool IsReliable => (Guarantees & PeerLinkGuarantees.Reliable) != 0;

    /// <summary>True when the transport preserves send order.</summary>
    public bool IsOrdered => (Guarantees & PeerLinkGuarantees.Ordered) != 0;

    /// <summary>True when the transport is both reliable and ordered (e.g. a QUIC stream).</summary>
    public bool IsReliableOrdered => IsReliable && IsOrdered;

    /// <summary>
    /// A reliable, in-order stream (e.g. QUIC/TLS). <paramref name="maxPayloadBytes"/>
    /// is usually <c>null</c> — streams don't frame — unless the transport enforces
    /// a per-message ceiling (Ensemble's RPC payload limit, say).
    /// </summary>
    public static PeerLinkCapabilities ReliableOrderedStream(int? maxPayloadBytes = null) =>
        new(PeerLinkGuarantees.Reliable | PeerLinkGuarantees.Ordered, maxPayloadBytes);

    /// <summary>
    /// Best-effort datagrams with a hard <paramref name="maxPayloadBytes"/> MTU
    /// (e.g. raw UDP at ~1200 bytes). No reliability, no ordering — tier A adds
    /// what the game needs.
    /// </summary>
    public static PeerLinkCapabilities UnreliableDatagram(int maxPayloadBytes) =>
        new(PeerLinkGuarantees.None, maxPayloadBytes);
}
