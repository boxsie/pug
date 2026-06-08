namespace PUG.Netcode;

/// <summary>
/// A deterministic fixed-timestep clock the game <b>pumps</b> — PUG never owns a
/// loop or spawns a thread. Each engine frame the game hands its real frame delta
/// to <see cref="Advance"/>; the clock accumulates real time and tells the game
/// how many fixed simulation steps to run and how far (<see cref="Alpha"/>) it is
/// into the next one for render interpolation.
///
/// <para>
/// This is the canonical fix for the prototype's broken sim: a fixed-dt
/// integration gated by <c>Task.Delay</c> runs slow and machine-dependently
/// (PUG learning <c>c25df4da</c>). A real-clock accumulator with fixed-dt steps
/// and a catch-up cap decouples simulation rate from frame rate without any
/// sleeping or threading — the engine drives, the clock only accounts.
/// </para>
///
/// <para>
/// <b>Not thread-safe and deliberately so.</b> It's pumped from one place (the
/// game's frame callback, e.g. Godot <c>_Process</c>). No <c>Task.Delay</c>, no
/// background thread, no self-driving loop.
/// </para>
/// </summary>
public sealed class TickClock
{
    /// <summary>Default catch-up cap: at most this many fixed steps run in one
    /// <see cref="Advance"/>, after which leftover backlog is dropped.</summary>
    public const int DefaultMaxStepsPerAdvance = 5;

    private readonly long _deltaTicks;
    private readonly int _maxStepsPerAdvance;
    private long _accumulatorTicks;

    /// <summary>
    /// Create a clock running at <paramref name="tickHz"/> fixed steps per second.
    /// </summary>
    /// <param name="tickHz">Simulation rate in steps per second (e.g. 60). Must be
    ///   positive.</param>
    /// <param name="maxStepsPerAdvance">Spiral-of-death cap: the most fixed steps a
    ///   single <see cref="Advance"/> will emit before dropping the rest of the
    ///   backlog, so one long stall can't cascade into a runaway catch-up. Must be
    ///   positive.</param>
    public TickClock(int tickHz, int maxStepsPerAdvance = DefaultMaxStepsPerAdvance)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tickHz);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxStepsPerAdvance);

        TickHz = tickHz;
        _maxStepsPerAdvance = maxStepsPerAdvance;

        // Derive dt by integer division of the tick budget so it's exact and very
        // slightly short rather than long — the clock errs toward an extra step,
        // never toward silently missing one on an exact-multiple frame delta.
        _deltaTicks = TimeSpan.TicksPerSecond / tickHz;
        Delta = TimeSpan.FromTicks(_deltaTicks);
    }

    /// <summary>The configured simulation rate, in fixed steps per second.</summary>
    public int TickHz { get; }

    /// <summary>The fixed simulation timestep, <c>1 / TickHz</c>. This — not the
    /// engine's wall-clock frame delta — is the <c>dt</c> the game integrates with.</summary>
    public TimeSpan Delta { get; }

    /// <summary>
    /// Total fixed steps emitted since construction. A plain monotonic counter —
    /// unlike the 16-bit per-channel packet sequence (which wraps for wire
    /// economy), this is local-only and has no bandwidth cost, so it's a
    /// <see cref="ulong"/> that for any real session never wraps (≈9.7 billion
    /// years at 60 Hz). Treat it as never wrapping.
    /// </summary>
    public ulong CurrentTick { get; private set; }

    /// <summary>
    /// How far, in <c>[0, 1)</c>, the accumulator is into the next fixed step
    /// after the most recent <see cref="Advance"/>. The render-interpolation
    /// factor: blend the previous and current simulation state by this much to
    /// draw smoothly between fixed steps.
    /// </summary>
    public double Alpha { get; private set; }

    /// <summary>
    /// Fold one engine frame's real elapsed time into the accumulator and report
    /// how many fixed steps the game should run this frame. The game runs the
    /// returned number of steps (each advancing the world by <see cref="Delta"/>),
    /// then renders using <see cref="Alpha"/>.
    /// </summary>
    /// <param name="realDelta">Wall-clock time since the last <see cref="Advance"/>.
    ///   Non-positive deltas add nothing and emit no steps.</param>
    /// <returns>The number of fixed steps to run this frame, in
    ///   <c>[0, maxStepsPerAdvance]</c>.</returns>
    public int Advance(TimeSpan realDelta)
    {
        if (realDelta > TimeSpan.Zero)
        {
            _accumulatorTicks += realDelta.Ticks;
        }

        var steps = 0;
        while (_accumulatorTicks >= _deltaTicks && steps < _maxStepsPerAdvance)
        {
            _accumulatorTicks -= _deltaTicks;
            CurrentTick++;
            steps++;
        }

        // Spiral-of-death guard: if the cap stopped us with whole steps still
        // backed up, drop that backlog (keep only the sub-step remainder) so a
        // hitch doesn't cascade into an ever-growing catch-up — and Alpha stays
        // in [0, 1). When we weren't capped, the remainder is already < dt and
        // this is a no-op.
        if (_accumulatorTicks >= _deltaTicks)
        {
            _accumulatorTicks %= _deltaTicks;
        }

        Alpha = (double)_accumulatorTicks / _deltaTicks;
        return steps;
    }

    /// <summary>
    /// Reset the tick counter, accumulator, and alpha to their initial state —
    /// e.g. when starting a fresh match on a reused clock.
    /// </summary>
    public void Reset()
    {
        _accumulatorTicks = 0;
        CurrentTick = 0;
        Alpha = 0;
    }
}
