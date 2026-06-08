namespace PUG.Netcode;

/// <summary>
/// A small, session-stable identifier the authority assigns to each connected
/// peer. It is the link between a wire <b>owner</b> tag and a participant: a
/// client reads an entity's owner field and compares it to its own
/// <see cref="NetSession.SelfId"/> to find the entity it controls.
///
/// <para>
/// <b><see cref="Authority"/> (0) is reserved.</b> It means "the authoritative
/// end / the world" — entities owned by the server (the ball, AI, scenery)
/// carry owner 0 and are predicted by nobody. Connected clients are numbered
/// from 1. This matches the Tier B1 wire format, whose per-entity
/// <c>owner: u8</c> field defaults to 0 ("unowned / server").
/// </para>
///
/// <para>
/// Backed by a <see cref="byte"/> so it fits that single owner byte directly —
/// 255 distinct clients is far more than any session PUG targets. Widen to
/// <see cref="ushort"/> here and in the wire format together if that ever bites.
/// </para>
/// </summary>
/// <param name="Value">The raw id. 0 is the reserved authority/world id.</param>
public readonly record struct PeerId(byte Value)
{
    /// <summary>The reserved id (0) for the authoritative end and anything it
    /// owns. Never assigned to a client.</summary>
    public static readonly PeerId Authority = new(0);

    /// <summary>True when this is the reserved authority/world id.</summary>
    public bool IsAuthority => Value == 0;

    /// <inheritdoc />
    public override string ToString() => IsAuthority ? "peer#0(authority)" : $"peer#{Value}";
}
