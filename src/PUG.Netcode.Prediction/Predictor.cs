namespace PUG.Netcode.Prediction;

/// <summary>
/// The Tier C prediction lane: advances the client's own entity under local input on the same
/// frame, so the player feels zero-latency control instead of a round-trip lag. Pair it with
/// <see cref="InterpolatingApplyStrategy.Exclude"/> on the owned entity so the interpolation
/// lane never clobbers the prediction with a stale authoritative sample (the <c>c25df4da</c>
/// rubber-band). C3 reconciles the residual divergence against authority — this lane keeps the
/// input ring C3 replays.
///
/// <para><b>Client-side only.</b> The host is already authoritative truth, so don't run a
/// predictor there — there is nothing to predict. The lane holds no session and never checks
/// <c>IsAuthority</c>; the game simply doesn't create one on the authority.</para>
///
/// <para><b>What the game wires around it</b> each input tick: select the owned entity
/// (<c>replicator.EntitiesOwnedBy(session.SelfId)</c>), send the input up
/// (<c>NetInputChannel.SendToAuthorityAsync(authorityTick, input)</c>), and call
/// <see cref="Predict"/> with the same input. <see cref="Predict"/> stamps the input with the
/// tick the <i>authority</i> will process it on (<c>AuthorityTickNow = localTick +
/// TimeSync.TickOffset</c>) so C3 can line the authoritative snapshot up with the right input.</para>
///
/// <para><b>Pumped, single-threaded</b> like the rest of the stack — no locking.</para>
/// </summary>
public sealed class Predictor : INetStatSource
{
    private readonly int _inputBufferCapacity;
    private readonly List<PredictedInput> _inputs;

    private long _predictedSteps;
    private uint _lastPredictedTick;

    /// <summary>
    /// Create the predictor. <paramref name="inputBufferCapacity"/> bounds the replay ring (the
    /// most recent inputs C3 can replay); the oldest is evicted past it. Default 128 ≈ ~2 s at
    /// 60 Hz, comfortably more than any plausible RTT of unacknowledged inputs.
    /// </summary>
    public Predictor(int inputBufferCapacity = 128)
    {
        if (inputBufferCapacity < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(inputBufferCapacity), inputBufferCapacity, "Need room for at least one input.");
        }

        _inputBufferCapacity = inputBufferCapacity;
        _inputs = new List<PredictedInput>(inputBufferCapacity);
    }

    /// <summary>Cumulative predicted steps applied (owned entity Simulate calls).</summary>
    public long PredictedSteps => _predictedSteps;

    /// <summary>
    /// The buffered local inputs, oldest-first — the inputs C3 replays after rewinding the owned
    /// entity to an authoritative state. Each is stamped with the tick the authority processes it
    /// on. Read-only view; the ring is maintained by <see cref="Predict"/>.
    /// </summary>
    public IReadOnlyList<PredictedInput> BufferedInputs => _inputs;

    /// <summary>
    /// Predict one step for the owned <paramref name="entity"/> under local <paramref name="input"/>
    /// over <paramref name="dt"/>, stamped with <paramref name="authorityTick"/> (the tick the
    /// authority will process it on). Buffers the input for C3 replay, then simulates immediately
    /// for same-frame response. Returns <see langword="true"/> if the entity was predicted; a
    /// non-<see cref="INetPredictable"/> entity is a no-op returning <see langword="false"/> (it
    /// still gets authoritative state through the normal snapshot path — parity, no crash).
    /// </summary>
    public bool Predict(INetEntityState entity, ReadOnlySpan<byte> input, uint authorityTick, TimeSpan dt)
    {
        ArgumentNullException.ThrowIfNull(entity);

        if (entity is not INetPredictable predictable)
        {
            return false;
        }

        _inputs.Add(new PredictedInput(authorityTick, input.ToArray()));
        if (_inputs.Count > _inputBufferCapacity)
        {
            _inputs.RemoveAt(0);
        }

        predictable.Simulate(input, dt);
        _predictedSteps++;
        _lastPredictedTick = authorityTick;
        return true;
    }

    /// <inheritdoc />
    public IReadOnlyList<NetStat> SampleStats() =>
    [
        new("predictedSteps", _predictedSteps),
        new("inputBuffer", _inputs.Count),
        new("lastPredictedTick", _lastPredictedTick),
    ];
}

/// <summary>
/// One buffered local input awaiting reconciliation: the tick the authority processes it on and
/// the game's opaque input bytes. C3 replays these (oldest-first, tick &gt; the acknowledged
/// snapshot tick) after rewinding the owned entity to an authoritative state.
/// </summary>
/// <param name="Tick">The authority tick this input is stamped for.</param>
/// <param name="Input">The game's opaque input bytes.</param>
public readonly record struct PredictedInput(uint Tick, ReadOnlyMemory<byte> Input);
