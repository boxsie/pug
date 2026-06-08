namespace PUG.Netcode;

/// <summary>
/// Declares one channel on a <see cref="ChannelMux"/>: a wire id and the
/// receiver-side <see cref="ChannelMode"/> that governs its delivery. Both peers
/// must declare the same id→mode mapping; the id travels in every packet header
/// so the receiver knows which policy to apply.
/// </summary>
/// <param name="Id">The channel's wire identifier (0–255), unique within a mux.
///   Carried as the first header byte of every packet on this channel.</param>
/// <param name="Mode">The receiver-side sequence policy for this channel.</param>
public readonly record struct ChannelSpec(byte Id, ChannelMode Mode);
