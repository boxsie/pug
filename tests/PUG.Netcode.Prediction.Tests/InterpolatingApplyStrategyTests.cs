using System.Buffers;
using System.Buffers.Binary;

namespace PUG.Netcode.Prediction.Tests;

/// <summary>
/// C1 interpolation lane, tested at the strategy boundary — the strategy only ever sees
/// decoded <c>(tick, state)</c> pairs, so a synthetic snapshot stream (gaps = loss, off-cadence
/// ticks = jitter) reproduces lossy/jittery transport exactly where the blend logic lives.
/// Deterministic, pumped, no <c>Task.Delay</c>.
/// </summary>
public sealed class InterpolatingApplyStrategyTests
{
    // Authority motion is linear: X == 5 * tick. Linear motion + linear lerp means a correctly
    // bracketed render recovers the exact authoritative value, so asserts can be tight.
    private static short AuthorityX(uint tick) => (short)(5 * tick);

    private static byte[] Pos(short x)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(bytes, x);
        return bytes;
    }

    private static void FeedLinear(InterpolatingApplyStrategy strategy, Point entity, uint fromTick, uint toTick, uint step)
    {
        for (var tick = fromTick; tick <= toTick; tick += step)
        {
            strategy.Apply(entity, tick, Pos(AuthorityX(tick)));
        }
    }

    [Fact]
    public void SmoothPath_NoStaircase_BoundedPerFrameDeltas()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 4, bufferCapacity: 16);
        var point = new Point();

        // 30 Hz snapshots on a 60 Hz tick = one every 2 ticks.
        FeedLinear(strategy, point, fromTick: 0, toTick: 20, step: 2);

        var rendered = new List<float>();
        for (uint render = 4; render <= 16; render++)
        {
            strategy.Render(render);
            rendered.Add(point.X);
        }

        // Each render recovers the exact authoritative position (linear motion).
        for (var i = 0; i < rendered.Count; i++)
        {
            var expected = AuthorityX((uint)(4 + i));
            Assert.True(Math.Abs(rendered[i] - expected) < 0.01f, $"frame {i}: {rendered[i]} vs {expected}");
        }

        // No 30 Hz staircase: every frame moves ~5 units, never a 0-then-jump pattern.
        for (var i = 1; i < rendered.Count; i++)
        {
            var delta = rendered[i] - rendered[i - 1];
            Assert.InRange(delta, 4f, 6f);
        }

        Assert.Equal(0, strategy.Underruns);
    }

    [Fact]
    public void RenderedState_TrailsAuthority_ByInterpDelayTicks()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 4, bufferCapacity: 16);
        var point = new Point();
        FeedLinear(strategy, point, 0, 20, 2);

        // AuthorityTickNow == 20 (newest fed); render trails by the delay.
        strategy.Render(20 - strategy.InterpDelayTicks);

        Assert.True(Math.Abs(point.X - AuthorityX(16)) < 0.01f, $"{point.X} should be the authority's tick-16 position");
    }

    [Fact]
    public void SingleDroppedSnapshot_IsBridged_NoSnapNoUnderrun()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 4, bufferCapacity: 16);
        var point = new Point();

        // Feed 0,2,4, DROP 6, then 8,10 — the buffer spans the hole.
        strategy.Apply(point, 0, Pos(AuthorityX(0)));
        strategy.Apply(point, 2, Pos(AuthorityX(2)));
        strategy.Apply(point, 4, Pos(AuthorityX(4)));
        strategy.Apply(point, 8, Pos(AuthorityX(8)));
        strategy.Apply(point, 10, Pos(AuthorityX(10)));

        // Render right at the dropped tick: blends across the 4->8 gap, recovers tick-6 position.
        strategy.Render(6);

        Assert.True(Math.Abs(point.X - AuthorityX(6)) < 0.01f, $"{point.X} should bridge the drop to tick-6");
        Assert.Equal(0, strategy.Underruns);
    }

    [Fact]
    public void LongGap_FreezesAndCountsUnderrun_ThenResumes()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 4, bufferCapacity: 16);
        var point = new Point();
        FeedLinear(strategy, point, 0, 4, 2); // newest == tick 4

        // Render well past the newest sample: hold the last known position, count an underrun.
        strategy.Render(10);
        Assert.True(Math.Abs(point.X - AuthorityX(4)) < 0.01f, "should freeze on the newest sample");
        Assert.True(strategy.Underruns >= 1);

        // A fresh sample arrives; the same render tick now brackets and motion resumes.
        var underrunsBefore = strategy.Underruns;
        strategy.Apply(point, 12, Pos(AuthorityX(12)));
        strategy.Render(10); // now between tick 4 and tick 12
        Assert.True(Math.Abs(point.X - AuthorityX(10)) < 0.01f, "should resume interpolation");
        Assert.Equal(underrunsBefore, strategy.Underruns); // no new underrun this frame
    }

    [Fact]
    public void NonInterpolableEntity_SnapsImmediately_OnApply()
    {
        var strategy = new InterpolatingApplyStrategy();
        var snap = new SnapOnly();

        strategy.Apply(snap, snapshotTick: 7, Pos(42));

        // Snapped at Apply time (parity with ImmediateApply), no Render needed.
        Assert.Equal(1, snap.AppliedCount);
        Assert.Equal(42, snap.Value);

        strategy.Render(7); // does nothing for a non-buffered entity
        Assert.Equal(1, snap.AppliedCount);
    }

    [Fact]
    public void JustSpawned_SingleSample_HoldsItAndCountsUnderrun()
    {
        var strategy = new InterpolatingApplyStrategy();
        var point = new Point();
        strategy.Apply(point, 100, Pos(AuthorityX(100)));

        strategy.Render(100);

        Assert.True(Math.Abs(point.X - AuthorityX(100)) < 0.01f, "single sample is held so a fresh entity is visible");
        Assert.True(strategy.Underruns >= 1);
    }

    [Fact]
    public void Forget_DropsBuffer_StopsRendering()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 2, bufferCapacity: 16);
        var point = new Point();
        FeedLinear(strategy, point, 0, 10, 2);

        strategy.Forget(point);
        var before = point.X;
        strategy.Render(6);

        Assert.Equal(before, point.X); // untouched — no longer tracked
        var stats = strategy.SampleStats().ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(0d, stats["entities"]);
    }

    [Fact]
    public void Diagnostics_FlowThroughTheCrossPackageHook()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 4, bufferCapacity: 16);
        var point = new Point();
        FeedLinear(strategy, point, 0, 4, 2);
        strategy.Render(99); // force an underrun

        var diagnostics = new NetDiagnostics();
        diagnostics.RegisterSource("interp", strategy);

        var group = Assert.Single(diagnostics.Snapshot().Sources);
        Assert.Equal("interp", group.Label);
        var stats = group.Stats.ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(4d, stats["interpDelayTicks"]);
        Assert.Equal(1d, stats["entities"]);
        Assert.True(stats["underruns"] >= 1);

        Assert.Contains("interp", diagnostics.Describe());
    }

    [Fact]
    public void ExcludedEntity_IsDropped_NotSnapped_PreventsRubberBand()
    {
        var strategy = new InterpolatingApplyStrategy();
        var owned = new SnapOnly();

        // The owned/predicted entity is excluded so a snapshot can't clobber the prediction.
        strategy.Exclude(owned);
        strategy.Apply(owned, snapshotTick: 1, Pos(42));
        Assert.Equal(0, owned.AppliedCount); // dropped — not snapped to the stale authoritative value

        // Forget clears the exclusion (e.g. on despawn): the normal snap path resumes.
        strategy.Forget(owned);
        strategy.Apply(owned, snapshotTick: 2, Pos(7));
        Assert.Equal(1, owned.AppliedCount);
        Assert.Equal(7, owned.Value);
    }

    [Fact]
    public void ExcludedEntity_CapturesLatestAuthoritative_ForReconciliation()
    {
        var strategy = new InterpolatingApplyStrategy();
        var owned = new SnapOnly();
        strategy.Exclude(owned);
        Assert.False(strategy.TryGetLatestAuthoritative(owned, out _, out _)); // nothing seen yet

        strategy.Apply(owned, snapshotTick: 5, Pos(42));

        Assert.True(strategy.TryGetLatestAuthoritative(owned, out var tick, out var state));
        Assert.Equal(5u, tick);
        Assert.Equal(42, BinaryPrimitives.ReadInt16BigEndian(state)); // the reconciler's rewind bytes
        Assert.Equal(0, owned.AppliedCount); // captured, NOT applied to the entity (no rubber-band)
    }

    [Fact]
    public void ExcludedInterpolable_IsNeitherBufferedNorRendered()
    {
        var strategy = new InterpolatingApplyStrategy(interpDelayTicks: 2, bufferCapacity: 16);
        var point = new Point();
        strategy.Exclude(point);

        FeedLinear(strategy, point, 0, 10, 2);
        strategy.Render(6);

        // Excluded interpolable accrued no buffer and was never moved by Render.
        Assert.Equal(0f, point.X);
        var stats = strategy.SampleStats().ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(0d, stats["entities"]);
    }

    /// <summary>A 1-D interpolable entity: position quantized to an int16, lerped linearly.</summary>
    private sealed class Point : INetInterpolable
    {
        public float X { get; private set; }

        public byte Kind => 1;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(span, (short)MathF.Round(X));
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state) => X = BinaryPrimitives.ReadInt16BigEndian(state);

        public void ApplyInterpolated(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to, float t)
        {
            float a = BinaryPrimitives.ReadInt16BigEndian(from);
            float b = BinaryPrimitives.ReadInt16BigEndian(to);
            X = a + ((b - a) * t);
        }
    }

    /// <summary>A non-interpolable entity: the strategy must snap it through ApplyState.</summary>
    private sealed class SnapOnly : INetEntityState
    {
        public short Value { get; private set; }

        public int AppliedCount { get; private set; }

        public byte Kind => 2;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(span, Value);
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            Value = BinaryPrimitives.ReadInt16BigEndian(state);
            AppliedCount++;
        }
    }
}
