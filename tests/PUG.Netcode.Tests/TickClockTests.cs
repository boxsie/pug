namespace PUG.Netcode.Tests;

/// <summary>
/// The pumped fixed-timestep clock. Tests feed exact multiples of
/// <see cref="TickClock.Delta"/> so the accumulator is float-noise-free — a real
/// engine feeds wall-clock deltas where ±1-tick rounding just lands a step a
/// frame early/late, which is imperceptible and not what these pin down.
/// </summary>
public class TickClockTests
{
    [Fact]
    public void Delta_IsOneOverTickHz()
    {
        var clock = new TickClock(tickHz: 60);
        Assert.Equal(60, clock.TickHz);
        Assert.Equal(TimeSpan.FromTicks(TimeSpan.TicksPerSecond / 60), clock.Delta);
    }

    [Fact]
    public void Advance_OneDtFrame_RunsExactlyOneStep()
    {
        var clock = new TickClock(tickHz: 60);

        var steps = clock.Advance(clock.Delta);

        Assert.Equal(1, steps);
        Assert.Equal(1ul, clock.CurrentTick);
    }

    [Fact]
    public void Advance_DoubleDtFrame_RunsTwoSteps()
    {
        var clock = new TickClock(tickHz: 60);

        var steps = clock.Advance(clock.Delta * 2);

        Assert.Equal(2, steps);
        Assert.Equal(2ul, clock.CurrentTick);
    }

    [Fact]
    public void Advance_HalfDtFrames_AlternateZeroOne()
    {
        var clock = new TickClock(tickHz: 60);
        var half = clock.Delta / 2;

        Assert.Equal(0, clock.Advance(half)); // acc = 0.5 dt
        Assert.Equal(1, clock.Advance(half)); // acc = 1.0 dt → step
        Assert.Equal(0, clock.Advance(half)); // acc = 0.5 dt
        Assert.Equal(1, clock.Advance(half)); // acc = 1.0 dt → step
        Assert.Equal(2ul, clock.CurrentTick);
    }

    [Fact]
    public void Advance_AccumulatesFractionalFramesAcrossCalls()
    {
        var clock = new TickClock(tickHz: 60);
        var fourTenths = clock.Delta * 0.4;

        Assert.Equal(0, clock.Advance(fourTenths)); // 0.4
        Assert.Equal(0, clock.Advance(fourTenths)); // 0.8
        Assert.Equal(1, clock.Advance(fourTenths)); // 1.2 → step, 0.2 remains
        Assert.Equal(1ul, clock.CurrentTick);
        Assert.InRange(clock.Alpha, 0.0, 1.0);
    }

    [Fact]
    public void Advance_HugeDelta_ClampsToMaxStepsAndDropsBacklog()
    {
        var clock = new TickClock(tickHz: 60, maxStepsPerAdvance: 5);

        var steps = clock.Advance(TimeSpan.FromSeconds(10)); // would be ~600 steps unclamped

        Assert.Equal(5, steps);
        Assert.Equal(5ul, clock.CurrentTick);
        Assert.InRange(clock.Alpha, 0.0, 0.999999); // backlog dropped, alpha stays in [0,1)
    }

    [Fact]
    public void Advance_NonPositiveDelta_DoesNothing()
    {
        var clock = new TickClock(tickHz: 60);
        clock.Advance(clock.Delta); // tick = 1

        Assert.Equal(0, clock.Advance(TimeSpan.Zero));
        Assert.Equal(0, clock.Advance(TimeSpan.FromSeconds(-1)));
        Assert.Equal(1ul, clock.CurrentTick);
    }

    [Fact]
    public void Alpha_StaysInUnitIntervalAcrossManyIrregularFrames()
    {
        var clock = new TickClock(tickHz: 60);
        var frames = new[] { 0.3, 0.9, 0.1, 2.5, 0.05, 1.0, 0.7 };

        foreach (var f in frames)
        {
            clock.Advance(clock.Delta * f);
            Assert.InRange(clock.Alpha, 0.0, 0.999999);
        }
    }

    [Fact]
    public void Reset_ClearsTickAccumulatorAndAlpha()
    {
        var clock = new TickClock(tickHz: 60);
        clock.Advance(clock.Delta * 3);
        Assert.Equal(3ul, clock.CurrentTick);

        clock.Reset();

        Assert.Equal(0ul, clock.CurrentTick);
        Assert.Equal(0.0, clock.Alpha);
        Assert.Equal(1, clock.Advance(clock.Delta)); // accumulator was cleared too
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_RejectsNonPositiveTickHz(int tickHz)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new TickClock(tickHz));
    }

    [Fact]
    public void Constructor_RejectsNonPositiveMaxSteps()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new TickClock(tickHz: 60, maxStepsPerAdvance: 0));
    }
}
