namespace PUG.Netcode.Prediction;

/// <summary>
/// The Tier C interpolation lane: an <see cref="ISnapshotApplyStrategy"/> that renders
/// remote entities <i>between</i> buffered authoritative snapshots instead of snapping to
/// the newest. Pass it to <c>NetworkReplicator.CreateClient(..., applyStrategy)</c> and call
/// <see cref="Render"/> once per frame; the game keeps its own reference to the concrete
/// strategy so it can drive the render step. The core's <see cref="ISnapshotApplyStrategy"/>
/// is unchanged — this is the seam doing its job, not a wider core API.
///
/// <para><b>Arrival vs. render.</b> <see cref="Apply"/> fires when a snapshot <i>arrives</i>
/// and only buffers it (a copy — the span is borrowed). <see cref="Render"/> fires each frame
/// and blends toward a render time deliberately held in the past, so two samples always
/// bracket it.</para>
///
/// <para><b>Render in the past.</b> The caller renders at
/// <c>renderTick = AuthorityTickNow − <see cref="InterpDelayTicks"/></c>, where
/// <c>AuthorityTickNow = localTick + TimeSync.TickOffset</c>. Trailing the newest snapshot by
/// ≳ one snapshot interval is what guarantees a bracketing pair exists; TimeSync places that
/// trailing point consistently regardless of clock skew. <see cref="InterpDelayTicks"/> trades
/// latency for smoothness — bigger absorbs more jitter/loss but shows older state.</para>
///
/// <para><b>Structural routing.</b> Only entities implementing <see cref="INetInterpolable"/>
/// are buffered and blended; anything else (including an owned/predicted entity, which
/// implements <c>INetPredictable</c> in C2, not this) is snapped through
/// <see cref="INetEntityState.ApplyState"/> — exact parity with the core's
/// <c>ImmediateApply</c>. So "owned entities never interpolate" needs no owner check here.</para>
///
/// <para><b>Diagnostics.</b> Implements <see cref="INetStatSource"/>; register it with
/// <c>NetDiagnostics.RegisterSource("interp", strategy)</c> to surface buffer depth, cumulative
/// underruns, and the current delay through the core's generic out-of-assembly hook.</para>
///
/// <para><b>Pumped, single-threaded</b> like the replicator and clock it serves — no locking.</para>
/// </summary>
public sealed class InterpolatingApplyStrategy : ISnapshotApplyStrategy, INetStatSource
{
    private readonly int _bufferCapacity;
    private readonly Dictionary<INetInterpolable, EntityInterpBuffer> _buffers =
        new(ReferenceEqualityComparer.Instance);

    private long _underruns;
    private uint _lastRenderTick;

    /// <summary>
    /// Create the strategy. <paramref name="interpDelayTicks"/> is how far behind
    /// <c>AuthorityTickNow</c> the render point trails (default 4 ≈ 2–3 snapshots back at
    /// 30 Hz snaps on a 60 Hz tick); <paramref name="bufferCapacity"/> is the per-entity
    /// sample ring depth (must be ≥ 2 to bracket).
    /// </summary>
    public InterpolatingApplyStrategy(uint interpDelayTicks = 4, int bufferCapacity = 8)
    {
        if (bufferCapacity < 2)
        {
            throw new ArgumentOutOfRangeException(nameof(bufferCapacity), bufferCapacity, "Need ≥ 2 samples to bracket a render time.");
        }

        InterpDelayTicks = interpDelayTicks;
        _bufferCapacity = bufferCapacity;
    }

    /// <summary>
    /// How many ticks behind <c>AuthorityTickNow</c> the caller should render. Use it to
    /// compute the argument to <see cref="Render"/>:
    /// <c>strategy.Render(authorityTickNow - strategy.InterpDelayTicks)</c>.
    /// </summary>
    public uint InterpDelayTicks { get; }

    /// <summary>Cumulative count of per-entity frames that could not bracket the render
    /// time and held/snapped instead (buffer still filling, a long gap, or a stall).</summary>
    public long Underruns => _underruns;

    /// <inheritdoc />
    /// <remarks>Buffers interpolable entities (copying the borrowed span); snaps everything
    /// else through <see cref="INetEntityState.ApplyState"/>.</remarks>
    public void Apply(INetEntityState entity, uint snapshotTick, ReadOnlySpan<byte> state)
    {
        ArgumentNullException.ThrowIfNull(entity);

        if (entity is INetInterpolable interpolable)
        {
            if (!_buffers.TryGetValue(interpolable, out var buffer))
            {
                buffer = new EntityInterpBuffer(_bufferCapacity);
                _buffers[interpolable] = buffer;
            }

            buffer.Append(snapshotTick, state.ToArray());
            return;
        }

        // Non-interpolable (e.g. an owned/predicted entity, or one the game chose not to
        // smooth): snap, exactly like ImmediateApply.
        entity.ApplyState(state);
    }

    /// <summary>
    /// Blend every buffered interpolable entity toward <paramref name="renderTick"/> — the
    /// render point held in the past (<c>AuthorityTickNow − <see cref="InterpDelayTicks"/></c>).
    /// For each entity, find the two buffered samples straddling the tick and call
    /// <see cref="INetInterpolable.ApplyInterpolated"/>; if the tick falls outside the buffered
    /// range (still filling, a gap longer than the buffer, or a stall) hold the nearest sample
    /// and count an underrun. Call once per frame, after the replicator's <c>Apply()</c>.
    /// </summary>
    public void Render(uint renderTick)
    {
        _lastRenderTick = renderTick;

        foreach (var (entity, buffer) in _buffers)
        {
            var outcome = buffer.Resolve(renderTick);
            switch (outcome.Kind)
            {
                case RenderKind.Blend:
                    entity.ApplyInterpolated(outcome.From, outcome.To, outcome.T);
                    break;
                case RenderKind.Snap:
                    entity.ApplyState(outcome.From);
                    if (outcome.Underrun)
                    {
                        _underruns++;
                    }

                    break;
                case RenderKind.None:
                default:
                    break;
            }
        }
    }

    /// <summary>
    /// Stop tracking <paramref name="entity"/> and drop its buffered samples. Wire this to the
    /// replicator's <c>onDespawn</c> so a vanished entity's buffer doesn't leak or get rendered
    /// after it's gone.
    /// </summary>
    public void Forget(INetEntityState entity)
    {
        if (entity is INetInterpolable interpolable)
        {
            _buffers.Remove(interpolable);
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<NetStat> SampleStats() =>
    [
        new("entities", _buffers.Count),
        new("interpDelayTicks", InterpDelayTicks),
        new("underruns", _underruns),
        new("lastRenderTick", _lastRenderTick),
    ];
}

/// <summary>The two outcomes <see cref="EntityInterpBuffer.Resolve"/> can produce.</summary>
internal enum RenderKind
{
    /// <summary>No samples buffered yet — nothing to render.</summary>
    None,

    /// <summary>Hold a single sample (carried in <see cref="RenderOutcome.From"/>); an
    /// underrun if the render time was outside the buffered range.</summary>
    Snap,

    /// <summary>Interpolate between two samples.</summary>
    Blend,
}

/// <summary>The resolved render instruction for one entity at a render tick.</summary>
internal readonly struct RenderOutcome
{
    private RenderOutcome(RenderKind kind, byte[] from, byte[] to, float t, bool underrun)
    {
        Kind = kind;
        From = from;
        To = to;
        T = t;
        Underrun = underrun;
    }

    public RenderKind Kind { get; }

    public byte[] From { get; }

    public byte[] To { get; }

    public float T { get; }

    public bool Underrun { get; }

    public static RenderOutcome None { get; } = new(RenderKind.None, [], [], 0f, false);

    public static RenderOutcome Snap(byte[] state, bool underrun) => new(RenderKind.Snap, state, state, 0f, underrun);

    public static RenderOutcome Blend(byte[] from, byte[] to, float t) => new(RenderKind.Blend, from, to, t, false);
}

/// <summary>
/// A small per-entity ring of recent authoritative samples, oldest-first. Appends are
/// monotonic in tick (the replicator delivers forward-ordered snapshots, possibly with gaps);
/// the oldest sample is evicted past capacity.
/// </summary>
internal sealed class EntityInterpBuffer(int capacity)
{
    private readonly List<Sample> _samples = new(capacity);

    /// <summary>Append a sample. A repeated tick replaces in place; a stale (older-or-equal
    /// going backwards) tick is ignored.</summary>
    public void Append(uint tick, byte[] state)
    {
        if (_samples.Count > 0)
        {
            var last = _samples[^1];
            if (tick == last.Tick)
            {
                _samples[^1] = new Sample(tick, state);
                return;
            }

            if (tick < last.Tick)
            {
                return; // stale / out-of-order; KeepLatest shouldn't produce these, but guard.
            }
        }

        _samples.Add(new Sample(tick, state));
        if (_samples.Count > capacity)
        {
            _samples.RemoveAt(0);
        }
    }

    /// <summary>
    /// Resolve what to render at <paramref name="renderTick"/>: blend between the bracketing
    /// pair, or snap/hold the nearest sample (an underrun when the tick is strictly outside the
    /// buffered range or there's only one sample).
    /// </summary>
    public RenderOutcome Resolve(uint renderTick)
    {
        if (_samples.Count == 0)
        {
            return RenderOutcome.None;
        }

        var oldest = _samples[0];
        var newest = _samples[^1];

        // Single sample, or sitting exactly on a boundary: hold it. Strictly-outside is the
        // underrun (still filling / gap longer than the buffer / stall); on-boundary is not.
        if (_samples.Count == 1 || renderTick < oldest.Tick)
        {
            return RenderOutcome.Snap(oldest.State, underrun: renderTick < oldest.Tick || _samples.Count == 1);
        }

        if (renderTick >= newest.Tick)
        {
            return RenderOutcome.Snap(newest.State, underrun: renderTick > newest.Tick);
        }

        if (renderTick == oldest.Tick)
        {
            return RenderOutcome.Snap(oldest.State, underrun: false);
        }

        for (var i = 0; i < _samples.Count - 1; i++)
        {
            var a = _samples[i];
            var b = _samples[i + 1];
            if (renderTick >= a.Tick && renderTick < b.Tick)
            {
                var span = b.Tick - a.Tick;
                var t = span == 0 ? 0f : (renderTick - a.Tick) / (float)span;
                return RenderOutcome.Blend(a.State, b.State, t);
            }
        }

        // Unreachable given the range checks above, but stay defensive.
        return RenderOutcome.Snap(newest.State, underrun: false);
    }

    private readonly record struct Sample(uint Tick, byte[] State);
}
