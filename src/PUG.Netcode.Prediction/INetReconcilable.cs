namespace PUG.Netcode.Prediction;

/// <summary>
/// The opt-in contract a predicted entity implements to have its corrections <i>smoothed</i> by
/// <see cref="Reconciler"/> instead of popped. Extends <see cref="INetPredictable"/> with one
/// blend operation: when the authoritative snapshot disagrees with the prediction, the reconciler
/// rewinds + replays to find the corrected state, then eases the visible entity toward it over
/// successive snapshots through <see cref="BlendCorrection"/> rather than snapping.
///
/// <para>
/// A predicted entity that does <i>not</i> implement this is still reconciled (rewound + replayed
/// to the accurate state) but the correction is applied hard — accurate, occasionally poppy.
/// Implement this when a visible pop on divergence would be jarring.
/// </para>
///
/// <para>
/// As with interpolation, the bytes stay opaque: the reconciler hands the game a target state and
/// a fraction, and the <i>game</i> moves its own fields toward it. PUG never interprets the bytes,
/// so it cannot compute "how far off" a prediction is — error magnitude is the game's to measure.
/// </para>
/// </summary>
public interface INetReconcilable : INetPredictable
{
    /// <summary>
    /// Ease this entity's visible state a fraction <paramref name="t"/> toward
    /// <paramref name="target"/> — the corrected state the reconciler computed by rewind + replay.
    /// <paramref name="t"/> is in [0,1]: a small value (e.g. 0.2) closes a bounded slice of the
    /// error this step so a large divergence decays in over several snapshots instead of popping;
    /// <paramref name="t"/>=1 snaps. <paramref name="target"/> is the exact bytes a matching
    /// <see cref="INetEntityState.WriteState"/> produced. The game lerps its fields from their
    /// current values toward the target's.
    /// </summary>
    void BlendCorrection(ReadOnlySpan<byte> target, float t);
}
