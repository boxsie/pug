using System.Buffers;

namespace PUG.Netcode.Prediction;

/// <summary>
/// The Tier C reconciliation step: when an authoritative snapshot disagrees with the client's
/// prediction of its own entity, correct it <b>without popping</b>. Closes the predict loop from
/// <see cref="Predictor"/> — it reuses that lane's input ring.
///
/// <para><b>The loop</b> (call <see cref="Reconcile"/> with the owned entity's authoritative state
/// each time a snapshot for it arrives — pull it from
/// <see cref="InterpolatingApplyStrategy.TryGetLatestAuthoritative"/>):</para>
/// <list type="number">
/// <item>Snapshot the current (predicted, visible) state.</item>
/// <item>Rewind: <see cref="INetEntityState.ApplyState"/> to the authoritative bytes at tick T.</item>
/// <item>Replay every still-unconfirmed local input (<c>tick &gt; T</c>) from the predictor's ring
///   via <see cref="INetPredictable.Simulate"/>, forward to the present — this is the new corrected
///   state.</item>
/// <item>If it diverged from step 1, ease the visible entity toward the corrected state by a bounded
///   fraction (<see cref="CorrectionRate"/>) via <see cref="INetReconcilable.BlendCorrection"/>, so a
///   large error decays in over several snapshots instead of snapping. Then ack the inputs through T.</item>
/// </list>
///
/// <para><b>The client replays its OWN inputs</b> — they are never echoed back over the wire. The
/// authoritative snapshot tick is the "authority has processed input up to ~here" marker; everything
/// after it the client re-simulates from its perfect local record.</para>
///
/// <para><b>KeepLatest input is sufficient</b> — reconciliation needs no Ordered/buffered input
/// channel, because it does not consume a received input stream. <b>Caveat:</b> the authority reads
/// each client's input on a lossy latest-wins channel (B3); if it drops an input tick its sim diverges
/// from the client's full-input prediction, producing constant small corrections. That is fine for
/// STATE-ish input (axes, a paddle target). IMPULSE input that must not be dropped (jump, fire) belongs
/// on the Ordered <c>NetEventChannel</c>, not the prediction path.</para>
///
/// <para><b>Pumped, single-threaded</b> — no locking. Implements <see cref="INetStatSource"/>; register
/// it with <c>NetDiagnostics.RegisterSource("reconcile", reconciler)</c>. Note PUG cannot surface
/// error <i>magnitude</i> (bytes are opaque) — it reports corrections, total replays, and last replay
/// depth.</para>
/// </summary>
public sealed class Reconciler : INetStatSource
{
    private readonly Predictor _predictor;
    private readonly TimeSpan _dt;
    private readonly ArrayBufferWriter<byte> _scratch = new();

    private long _corrections;
    private long _replaysTotal;
    private int _lastReplayCount;

    /// <summary>
    /// Create the reconciler over the <paramref name="predictor"/> whose input ring it replays.
    /// <paramref name="dt"/> is the per-step delta passed to <see cref="INetPredictable.Simulate"/>
    /// during replay (must match the prediction step). <paramref name="correctionRate"/> is the
    /// fraction of remaining error a smoothed correction closes per snapshot (0,1]; smaller = softer
    /// and slower, 1 = hard snap.
    /// </summary>
    public Reconciler(Predictor predictor, TimeSpan dt, float correctionRate = 0.2f)
    {
        ArgumentNullException.ThrowIfNull(predictor);
        if (correctionRate is <= 0f or > 1f)
        {
            throw new ArgumentOutOfRangeException(nameof(correctionRate), correctionRate, "Correction rate must be in (0, 1].");
        }

        _predictor = predictor;
        _dt = dt;
        CorrectionRate = correctionRate;
    }

    /// <summary>The bounded fraction of the prediction error a smoothed correction closes per
    /// snapshot. See the constructor.</summary>
    public float CorrectionRate { get; }

    /// <summary>Cumulative corrections applied (snapshots where prediction diverged from authority).</summary>
    public long Corrections => _corrections;

    /// <summary>
    /// Reconcile the owned <paramref name="entity"/> against its authoritative state
    /// (<paramref name="authoritativeState"/> at <paramref name="authoritativeTick"/>): rewind, replay
    /// the predictor's unconfirmed inputs, and — if prediction diverged — ease the visible entity
    /// toward the corrected state. Acks the predictor's inputs through the tick. A non-predictable
    /// entity is hard-snapped to the authoritative state (parity, no crash); a predictable but
    /// non-<see cref="INetReconcilable"/> entity is corrected accurately but without smoothing.
    /// </summary>
    public void Reconcile(INetEntityState entity, uint authoritativeTick, ReadOnlySpan<byte> authoritativeState)
    {
        ArgumentNullException.ThrowIfNull(entity);

        if (entity is not INetPredictable predictable)
        {
            entity.ApplyState(authoritativeState); // nothing to replay — just take authority
            return;
        }

        var preVisible = Capture(entity);

        entity.ApplyState(authoritativeState); // rewind to the confirmed state at tick T

        var replayCount = 0;
        foreach (var buffered in _predictor.BufferedInputs)
        {
            if (Predictor.IsAfter(buffered.Tick, authoritativeTick))
            {
                predictable.Simulate(buffered.Input.Span, _dt);
                replayCount++;
            }
        }

        var corrected = Capture(entity);
        _lastReplayCount = replayCount;
        _replaysTotal += replayCount;

        var diverged = !preVisible.AsSpan().SequenceEqual(corrected);
        if (diverged)
        {
            _corrections++;
            if (entity is INetReconcilable reconcilable)
            {
                // Undo the hard jump, then close a bounded slice of the error: the visible entity
                // eases toward the corrected state over successive snapshots instead of popping.
                entity.ApplyState(preVisible);
                reconcilable.BlendCorrection(corrected, CorrectionRate);
            }

            // else: leave the entity at the accurate corrected state (no smoothing available).
        }

        // else: prediction matched authority exactly — entity already sits at corrected==preVisible.
        _predictor.AckThrough(authoritativeTick);
    }

    /// <inheritdoc />
    public IReadOnlyList<NetStat> SampleStats() =>
    [
        new("corrections", _corrections),
        new("replaysTotal", _replaysTotal),
        new("lastReplayCount", _lastReplayCount),
    ];

    private byte[] Capture(INetEntityState entity)
    {
        _scratch.Clear();
        entity.WriteState(_scratch);
        return _scratch.WrittenSpan.ToArray();
    }
}
