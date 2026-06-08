namespace PUG.Netcode;

/// <summary>
/// A point-in-time counter snapshot for one channel, taken from
/// <see cref="ChannelMux.Stats"/>. All counts are cumulative since the mux was
/// created. "Received" counts every packet that arrived on the channel off the
/// wire; <see cref="DroppedStale"/> and <see cref="Reordered"/> are subsets of
/// that describing what the receive policy did with them.
/// </summary>
/// <param name="ChannelId">The channel's wire id.</param>
/// <param name="Mode">The channel's receive policy.</param>
/// <param name="PacketsSent">Packets sent on this channel.</param>
/// <param name="BytesSent">Wire bytes sent (header + payload).</param>
/// <param name="PacketsReceived">Packets that arrived off the wire for this channel.</param>
/// <param name="BytesReceived">Wire bytes received (header + payload).</param>
/// <param name="DroppedStale">Arrivals discarded as stale — a
///   <see cref="ChannelMode.KeepLatest"/> packet ≤ the highest seen, or an
///   <see cref="ChannelMode.Ordered"/> duplicate already released.</param>
/// <param name="Reordered">Arrivals that came out of order and were buffered
///   (<see cref="ChannelMode.Ordered"/> only) before later release.</param>
public readonly record struct ChannelStats(
    byte ChannelId,
    ChannelMode Mode,
    long PacketsSent,
    long BytesSent,
    long PacketsReceived,
    long BytesReceived,
    long DroppedStale,
    long Reordered);

/// <summary>
/// A point-in-time snapshot of a whole <see cref="ChannelMux"/>'s counters. The
/// host polls <see cref="ChannelMux.Stats"/> each frame (or on demand) and reads
/// this; it's the mux's contribution to the wider <c>NetDiagnostics</c> surface
/// — diagnostics pulls these counters rather than the mux pushing them, so the
/// mux carries no logging dependency.
/// </summary>
/// <param name="Channels">Per-channel counters, one entry per declared channel,
///   in declaration order.</param>
/// <param name="MalformedPackets">Inbound frames too short to carry a header,
///   dropped before dispatch.</param>
/// <param name="UnknownChannelPackets">Inbound frames addressed to a channel id
///   that wasn't declared on this mux, dropped.</param>
public readonly record struct ChannelMuxStats(
    IReadOnlyList<ChannelStats> Channels,
    long MalformedPackets,
    long UnknownChannelPackets);
