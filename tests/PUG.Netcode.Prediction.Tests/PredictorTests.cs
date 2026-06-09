using System.Buffers;
using System.Buffers.Binary;

namespace PUG.Netcode.Prediction.Tests;

/// <summary>
/// C2 prediction lane, tested at the Predictor boundary — the lane is session/replicator-free,
/// so a synthetic input stream drives it deterministically (no FakePeerLink, no Task.Delay).
/// Convergence is checked by running the SAME inputs through a second "authority" entity.
/// </summary>
public sealed class PredictorTests
{
    private static readonly TimeSpan Dt = TimeSpan.FromSeconds(1.0 / 60.0);

    private static byte[] Dir(sbyte direction) => [(byte)direction];

    [Fact]
    public void OwnedEntity_MovesOnTheSameSimulateCall_NoRttDelay()
    {
        var predictor = new Predictor();
        var mover = new Mover();

        var predicted = predictor.Predict(mover, Dir(1), authorityTick: 10, Dt);

        Assert.True(predicted);
        Assert.True(mover.X > 0f, "owned entity responds on the same frame the input was sampled");
    }

    [Fact]
    public void PredictedState_EqualsAuthority_UnderZeroDivergence()
    {
        var predictor = new Predictor();
        var client = new Mover();
        var authority = new Mover();

        // Identical input stream, identical Simulate + dt on both sides => no divergence,
        // which is exactly the "no correction needed" baseline C3 builds on.
        sbyte[] inputs = [1, 1, 0, -1, -1, 0, 1, -1, 1, 0];
        for (var i = 0; i < inputs.Length; i++)
        {
            predictor.Predict(client, Dir(inputs[i]), authorityTick: (uint)(100 + i), Dt);
            authority.Simulate(Dir(inputs[i]), Dt); // what the authority runs for the same input
        }

        Assert.Equal(authority.X, client.X);
        Assert.Equal(inputs.Length, predictor.PredictedSteps);
    }

    [Fact]
    public void NonPredictableOwnedEntity_IsNoOp_NoCrash()
    {
        var predictor = new Predictor();
        var plain = new PlainEntity();

        var predicted = predictor.Predict(plain, Dir(1), authorityTick: 1, Dt);

        Assert.False(predicted);
        Assert.Equal(0, predictor.PredictedSteps);
        Assert.Empty(predictor.BufferedInputs); // nothing to replay for an unpredicted entity
    }

    [Fact]
    public void LocalInputs_AreBuffered_OldestFirst_ForReplay()
    {
        var predictor = new Predictor();
        var mover = new Mover();

        predictor.Predict(mover, Dir(1), authorityTick: 50, Dt);
        predictor.Predict(mover, Dir(-1), authorityTick: 51, Dt);
        predictor.Predict(mover, Dir(0), authorityTick: 52, Dt);

        var buffered = predictor.BufferedInputs;
        Assert.Equal(3, buffered.Count);
        Assert.Equal(50u, buffered[0].Tick);
        Assert.Equal(52u, buffered[2].Tick);
        Assert.Equal(Dir(-1)[0], buffered[1].Input.Span[0]); // the -1 input round-tripped through the ring
    }

    [Fact]
    public void InputRing_EvictsOldest_PastCapacity()
    {
        var predictor = new Predictor(inputBufferCapacity: 3);
        var mover = new Mover();
        for (uint tick = 0; tick < 5; tick++)
        {
            predictor.Predict(mover, Dir(1), tick, Dt);
        }

        var buffered = predictor.BufferedInputs;
        Assert.Equal(3, buffered.Count);
        Assert.Equal(2u, buffered[0].Tick); // ticks 0,1 evicted
        Assert.Equal(4u, buffered[2].Tick);
    }

    [Fact]
    public void Diagnostics_FlowThroughTheCrossPackageHook()
    {
        var predictor = new Predictor();
        var mover = new Mover();
        predictor.Predict(mover, Dir(1), 7, Dt);
        predictor.Predict(mover, Dir(1), 8, Dt);

        var diagnostics = new NetDiagnostics();
        diagnostics.RegisterSource("predict", predictor);

        var group = Assert.Single(diagnostics.Snapshot().Sources);
        Assert.Equal("predict", group.Label);
        var stats = group.Stats.ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(2d, stats["predictedSteps"]);
        Assert.Equal(2d, stats["inputBuffer"]);
        Assert.Equal(8d, stats["lastPredictedTick"]);
    }

    /// <summary>A predictable 1-D mover: input byte 0 is a direction (-1/0/+1), 100 units/s.</summary>
    private sealed class Mover : INetPredictable
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

        public void Simulate(ReadOnlySpan<byte> input, TimeSpan dt)
        {
            var direction = (sbyte)input[0];
            X += direction * 100f * (float)dt.TotalSeconds;
        }
    }

    /// <summary>A non-predictable entity: the predictor must no-op on it.</summary>
    private sealed class PlainEntity : INetEntityState
    {
        public byte Kind => 2;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(1);
            span[0] = 0;
            writer.Advance(1);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            // no-op; this entity exists only to prove Predict ignores non-predictables.
        }
    }
}
