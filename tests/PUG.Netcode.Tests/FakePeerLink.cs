using System.Threading.Channels;

namespace PUG.Netcode.Tests;

/// <summary>
/// Knobs for a <see cref="FakePeerLink"/> pair. All default to a perfect link
/// (instant, lossless, in-order) so simple tests are deterministic; opt into
/// impairments to stress the tier-A reliability layer.
/// </summary>
public sealed class FakeLinkOptions
{
    /// <summary>Base one-way delivery delay added to every payload.</summary>
    public TimeSpan Latency { get; init; } = TimeSpan.Zero;

    /// <summary>Random extra delay in <c>[0, Jitter)</c> per payload. Non-zero
    /// jitter lets later sends overtake earlier ones — i.e. reordering.</summary>
    public TimeSpan Jitter { get; init; } = TimeSpan.Zero;

    /// <summary>Probability in <c>[0, 1]</c> that a given send is dropped.</summary>
    public double LossRate { get; init; }

    /// <summary>Seed for the loss/jitter RNG, so impaired runs are reproducible
    /// (delivery timing still has real-clock slack — assert on set membership,
    /// not exact permutations).</summary>
    public int Seed { get; init; } = 1;

    /// <summary>Capabilities both ends report. Defaults to a reliable-ordered
    /// stream; set to <see cref="PeerLinkCapabilities.UnreliableDatagram"/> when
    /// the impairments above are in play, to mirror a real lossy transport.</summary>
    public PeerLinkCapabilities Capabilities { get; init; } = PeerLinkCapabilities.ReliableOrderedStream();
}

/// <summary>
/// In-memory <see cref="IPeerLink"/> loopback: two ends wired so what one sends
/// the other receives, with optional latency / jitter / loss. The workhorse for
/// every tier A/B/C test — two netcode stacks talk with no daemon, no sockets,
/// no infra. Lives in the test project; promote to a <c>PUG.Netcode.Testing</c>
/// package if consumers ever want it.
/// </summary>
public sealed class FakePeerLink : IPeerLink
{
    private readonly FakeLinkOptions _options;
    private readonly Channel<ReadOnlyMemory<byte>> _inbound;
    private readonly Random _rng;
    private readonly object _rngLock = new();
    private FakePeerLink _peer = null!;
    private int _disposed;

    private FakePeerLink(FakeLinkOptions options)
    {
        _options = options;
        _rng = new Random(options.Seed);
        _inbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
    }

    /// <inheritdoc />
    public PeerLinkCapabilities Capabilities => _options.Capabilities;

    /// <summary>
    /// Create a connected pair. A payload sent on <c>A</c> surfaces on
    /// <c>B</c>'s <see cref="ReceiveAsync"/> and vice versa, subject to
    /// <paramref name="options"/>.
    /// </summary>
    public static (FakePeerLink A, FakePeerLink B) CreatePair(FakeLinkOptions? options = null)
    {
        options ??= new FakeLinkOptions();
        var a = new FakePeerLink(options);
        var b = new FakePeerLink(options);
        a._peer = b;
        b._peer = a;
        return (a, b);
    }

    /// <inheritdoc />
    public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ct.ThrowIfCancellationRequested();

        // Copy now: the contract lets callers reuse their buffer once this
        // returns, and we may deliver later (latency/jitter).
        var copy = payload.ToArray();

        double lossRoll;
        double jitterMs;
        lock (_rngLock)
        {
            lossRoll = _rng.NextDouble();
            jitterMs = _options.Jitter > TimeSpan.Zero
                ? _rng.NextDouble() * _options.Jitter.TotalMilliseconds
                : 0;
        }

        if (lossRoll < _options.LossRate)
            return ValueTask.CompletedTask; // dropped — silent, as an unreliable link would

        var delay = _options.Latency + TimeSpan.FromMilliseconds(jitterMs);
        if (delay <= TimeSpan.Zero)
            _peer._inbound.Writer.TryWrite(copy); // synchronous → deterministic, in-order
        else
            _ = DeliverAfterAsync(copy, delay);   // fire-and-forget; jitter ⇒ reordering

        return ValueTask.CompletedTask;
    }

    private async Task DeliverAfterAsync(byte[] payload, TimeSpan delay)
    {
        await Task.Delay(delay).ConfigureAwait(false);
        // TryWrite is a no-op if the peer completed (disposed) — safe.
        _peer._inbound.Writer.TryWrite(payload);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default) =>
        _inbound.Reader.ReadAllAsync(ct);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;
        _inbound.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
