namespace PUG.Netcode.Prediction;

/// <summary>
/// The opt-in contract an <i>owned</i> entity implements to be predicted locally by
/// <see cref="Predictor"/> — it advances under the player's input on the same frame the input
/// is sampled, instead of waiting a round-trip for the authority's snapshot. Extends core's
/// <see cref="INetEntityState"/> with one simulation step.
///
/// <para>
/// This is the <b>prediction lane</b> of the composable Tier C, the structural counterpart to
/// <see cref="INetInterpolable"/> (the interpolation lane). An entity implements one or the
/// other by role: the client's own entity is <see cref="INetPredictable"/> (predicted here and
/// excluded from interpolation); every other entity is <see cref="INetInterpolable"/>
/// (smoothed). That split is what keeps the interp lane from ever overwriting the predicted
/// entity with a stale authoritative sample.
/// </para>
/// </summary>
public interface INetPredictable : INetEntityState
{
    /// <summary>
    /// Advance this entity one step under <paramref name="input"/> over <paramref name="dt"/>,
    /// mutating it in place. <paramref name="input"/> is the game's opaque input bytes (the same
    /// blob sent to the authority); PUG never interprets them.
    ///
    /// <para>
    /// <b>Determinism is the contract.</b> This must be the <i>same</i> step the authority runs
    /// for this entity under the same input and dt, or prediction won't converge and C3's
    /// reconciliation will fight a permanent error. Keep it a pure function of
    /// (current state, input, dt) — no wall-clock, no RNG without a synced seed.
    /// </para>
    /// </summary>
    void Simulate(ReadOnlySpan<byte> input, TimeSpan dt);
}
