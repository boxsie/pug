using System.Buffers;

namespace PUG.Netcode;

/// <summary>
/// The opt-in, content-agnostic contract a networked object exposes to Tier B.
/// PUG never knows what the object <i>is</i> — a paddle, a ball, a projectile —
/// only that it has an archetype <see cref="Kind"/> and can serialize / restore
/// its replicated state as bytes.
///
/// <para>
/// State goes out as raw bytes via an <see cref="IBufferWriter{T}"/>, which keeps
/// the core zero-dependency and — crucially — lets the game <b>quantize</b>: write
/// an <c>int16</c> where the simulation holds a <c>float</c>, the single biggest
/// lever on snapshot size. Tier B does not interpret the bytes; only the matching
/// <see cref="ApplyState"/> on the other end does.
/// </para>
///
/// <para>
/// Tier C will grow this contract with a <c>Simulate(state, input, dt)</c> for
/// client-side prediction; B1 needs only write/apply, so that stays out until
/// there's a predictor to use it.
/// </para>
/// </summary>
public interface INetEntityState
{
    /// <summary>The archetype id. A receiving peer maps it through its spawn
    /// factory to construct the right kind of object the first time this entity
    /// appears in a snapshot.</summary>
    byte Kind { get; }

    /// <summary>Serialize the current replicated state into <paramref name="writer"/>.
    /// Write only what peers must mirror, quantized as aggressively as the game can
    /// tolerate — every byte here ships every snapshot.</summary>
    void WriteState(IBufferWriter<byte> writer);

    /// <summary>Restore state from authoritative <paramref name="state"/> bytes —
    /// the exact span a peer's <see cref="WriteState"/> produced. Must mirror the
    /// write layout.</summary>
    void ApplyState(ReadOnlySpan<byte> state);
}
