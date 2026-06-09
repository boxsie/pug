using System.Buffers;
using System.Buffers.Binary;

namespace PUG.Netcode.Prediction.Tests;

/// <summary>
/// C3 reconciliation, tested at the Reconciler boundary with synthetic divergence — deterministic,
/// no FakePeerLink, no Task.Delay. A second-by-second dt keeps the arithmetic exact (Simulate adds
/// dir*10 per step), so convergence and smoothing bounds can be asserted tightly.
/// </summary>
public sealed class ReconcilerTests
{
    private static readonly TimeSpan Dt = TimeSpan.FromSeconds(1.0); // step == dir * 10

    private static byte[] Dir(sbyte direction) => [(byte)direction];

    private static byte[] Pos(short x)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(bytes, x);
        return bytes;
    }

    [Fact]
    public void ZeroDivergence_ProducesNoCorrection()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt);
        var client = new Mover();

        // Predict forward; X == 50 after five +1 steps, inputs buffered at ticks 1..5.
        for (uint tick = 1; tick <= 5; tick++)
        {
            predictor.Predict(client, Dir(1), tick, Dt);
        }

        // Authority agreed: at tick 0 the state was 0; replaying ticks 1..5 reproduces 50 == prediction.
        reconciler.Reconcile(client, authoritativeTick: 0, Pos(0));

        Assert.Equal(0, reconciler.Corrections);
        Assert.Equal(50f, client.X); // unchanged — prediction matched
    }

    [Fact]
    public void Divergence_WithReplay_CorrectsTowardReplayedState_AndAcks()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt, correctionRate: 0.2f);
        Assert.Equal(0.2f, reconciler.CorrectionRate);
        var client = new Mover();
        for (uint tick = 1; tick <= 5; tick++)
        {
            predictor.Predict(client, Dir(1), tick, Dt); // client predicts X = 50
        }

        // Authority says X was 25 at tick 2 (client had predicted 20 there) — a divergence.
        reconciler.Reconcile(client, authoritativeTick: 2, Pos(25));

        // Corrected = 25 + replay(ticks 3,4,5 = +30) = 55. Visible eases 0.2 of (55-50) from 50 => 51.
        Assert.Equal(1, reconciler.Corrections);
        Assert.Equal(51f, client.X);
        // Inputs through tick 2 are confirmed and pruned; 3,4,5 remain for the next replay.
        Assert.Equal(3, predictor.BufferedInputs.Count);
        Assert.Equal(3u, predictor.BufferedInputs[0].Tick);
    }

    [Fact]
    public void LargeDivergence_IsSmoothed_BoundedPerStep_ThenConverges()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt, correctionRate: 0.2f);
        var client = new Mover(); // X = 0, no predicted inputs => corrected == authoritative

        var deltas = new List<float>();
        var previous = client.X;
        for (var i = 0; i < 40; i++)
        {
            reconciler.Reconcile(client, authoritativeTick: 0, Pos(100));
            deltas.Add(Math.Abs(client.X - previous));
            previous = client.X;
        }

        // No single-frame pop: the biggest step is the first, bounded by rate*error = 0.2*100 = 20,
        // nowhere near the full 100 divergence.
        Assert.True(Math.Abs(deltas[0] - 20f) < 0.001f, $"first step {deltas[0]} should be the 20-unit bound");
        Assert.All(deltas, d => Assert.True(d <= 20f + 0.001f, $"step {d} exceeded the smoothing bound"));
        // And it converges to within the quantization floor: each reconcile restores the
        // int16-rounded pre-state before blending, so a coarse entity (integer units here)
        // plateaus a couple units short. A real entity quantizes far finer (e.g. 1/65535 of its
        // range), shrinking this residual to noise. Started at 0, divergence 100 → now ~98.
        Assert.True(Math.Abs(client.X - 100f) < 2.5f, $"converged to {client.X}");
    }

    [Fact]
    public void NonPredictableEntity_HardSnapsToAuthority()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt);
        var plain = new PlainEntity { Value = 5 };

        reconciler.Reconcile(plain, authoritativeTick: 3, Pos(77));

        Assert.Equal(77, plain.Value); // took authority directly
        Assert.Equal(0, reconciler.Corrections); // no prediction loop ran
    }

    [Fact]
    public void PredictableButNotReconcilable_CorrectsAccurately_WithoutSmoothing()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt, correctionRate: 0.2f);
        var client = new PredictOnlyMover();
        for (uint tick = 1; tick <= 3; tick++)
        {
            predictor.Predict(client, Dir(1), tick, Dt); // X = 30
        }

        // Authority diverges at tick 1 (X=100); replay ticks 2,3 (+20) => corrected 120, applied hard.
        reconciler.Reconcile(client, authoritativeTick: 1, Pos(100));

        Assert.Equal(1, reconciler.Corrections);
        Assert.Equal(120f, client.X); // no blend — straight to the corrected state
    }

    [Fact]
    public void Diagnostics_FlowThroughTheCrossPackageHook()
    {
        var predictor = new Predictor();
        var reconciler = new Reconciler(predictor, Dt);
        var client = new Mover();
        reconciler.Reconcile(client, 0, Pos(50)); // a correction with no replay

        var diagnostics = new NetDiagnostics();
        diagnostics.RegisterSource("reconcile", reconciler);

        var group = Assert.Single(diagnostics.Snapshot().Sources);
        Assert.Equal("reconcile", group.Label);
        var stats = group.Stats.ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(1d, stats["corrections"]);
        Assert.True(stats.ContainsKey("replaysTotal"));
        Assert.True(stats.ContainsKey("lastReplayCount"));
    }

    /// <summary>A predictable + reconcilable 1-D mover: input byte 0 is a direction, 10 units/step.</summary>
    private sealed class Mover : INetReconcilable
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

        public void Simulate(ReadOnlySpan<byte> input, TimeSpan dt) => X += (sbyte)input[0] * 10f * (float)dt.TotalSeconds;

        public void BlendCorrection(ReadOnlySpan<byte> target, float t)
        {
            float to = BinaryPrimitives.ReadInt16BigEndian(target);
            X += (to - X) * t;
        }
    }

    /// <summary>Predictable but not reconcilable: corrected accurately, never smoothed.</summary>
    private sealed class PredictOnlyMover : INetPredictable
    {
        public float X { get; private set; }

        public byte Kind => 2;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(span, (short)MathF.Round(X));
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state) => X = BinaryPrimitives.ReadInt16BigEndian(state);

        public void Simulate(ReadOnlySpan<byte> input, TimeSpan dt) => X += (sbyte)input[0] * 10f * (float)dt.TotalSeconds;
    }

    /// <summary>Not predictable at all: reconcile must hard-snap it to authority.</summary>
    private sealed class PlainEntity : INetEntityState
    {
        public short Value { get; set; }

        public byte Kind => 3;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteInt16BigEndian(span, Value);
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state) => Value = BinaryPrimitives.ReadInt16BigEndian(state);
    }
}
