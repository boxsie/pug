namespace PUG.Netcode.Prediction;

/// <summary>
/// The opt-in contract a <i>remote</i> (non-owned) entity implements to be smoothed by
/// <see cref="InterpolatingApplyStrategy"/> instead of snapped to the newest snapshot.
/// It extends core's <see cref="INetEntityState"/> with one blend operation; an entity
/// that does not implement it falls back to snap-to-newest (parity with the core's
/// <c>ImmediateApply</c>).
///
/// <para>
/// This is the <b>interpolation lane</b> of the composable Tier C. Routing is structural:
/// remote entities implement <see cref="INetInterpolable"/> (smoothed here); an owned /
/// predicted entity implements <c>INetPredictable</c> instead (C2) and is never offered to
/// the interp lane — so "owned never interpolates" needs no runtime owner check, it is the
/// entity's interface that decides. An entity that implements neither just snaps.
/// </para>
///
/// <para>
/// As everywhere in PUG netcode, the bytes are opaque: PUG hands the game two authoritative
/// state blobs and a blend factor, and the <i>game</i> lerps its own fields. Netcode never
/// interprets the bytes — quantization, which fields blend linearly vs. need slerp, and
/// what "halfway" means are all the game's call.
/// </para>
/// </summary>
public interface INetInterpolable : INetEntityState
{
    /// <summary>
    /// Set this entity's visible state to the blend of two authoritative state blobs —
    /// <paramref name="from"/> at <paramref name="t"/>=0, <paramref name="to"/> at
    /// <paramref name="t"/>=1. Both spans are the exact bytes a matching
    /// <see cref="INetEntityState.WriteState"/> produced. <paramref name="t"/> is in
    /// [0,1]. The game reads the fields it cares about from each and writes the
    /// interpolated result onto itself.
    /// </summary>
    void ApplyInterpolated(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to, float t);
}
