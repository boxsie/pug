using System.Buffers.Binary;
using System.Threading.Channels;

namespace PUG.Netcode;

/// <summary>
/// Multiplexes a single <see cref="IPeerLink"/> (one duplex byte pipe to one
/// peer) into several independently-sequenced <b>channels</b>, each with its own
/// receive policy (<see cref="ChannelMode"/>). This is the netcode substrate
/// everything above rides on: it moves <i>sequenced bytes</i> and is entirely
/// content-agnostic — what those bytes mean (snapshots, inputs, events) is a
/// higher tier's concern.
///
/// <para>
/// <b>Framing.</b> Each <see cref="IPeerLink.SendAsync"/> payload is one packet:
/// <c>[channelId: byte][seq: uint16 big-endian] || payload</c>. The sequence
/// number is <b>per channel</b> (its own space), starts at 0, and wraps at 2^16;
/// the receiver compares with <see cref="SequenceNumber.GreaterThan"/>. Per-channel
/// spaces mean a snapshot dropped as stale on one channel can't open a phantom gap
/// that stalls an ordered channel on another.
/// </para>
///
/// <para>
/// <b>Pump.</b> The mux owns a single background task draining
/// <see cref="IPeerLink.ReceiveAsync"/>; it parses each header and applies the
/// channel's policy as packets land, dispatching survivors into per-channel
/// inbound queues. The game drains those per frame via <see cref="TryReceive"/> /
/// <see cref="DrainInto"/> (non-blocking) and sends directly via
/// <see cref="SendAsync"/>. The drain must own resequencing because it has to see
/// every packet as it arrives, regardless of when the game polls.
/// </para>
///
/// <para>
/// <b>No reliability layer.</b> The mux adds sequencing, not retransmission. On a
/// reliable transport (Ensemble's QUIC streams) nothing is ever lost, so an
/// <see cref="ChannelMode.Ordered"/> channel may stall briefly on an in-flight
/// earlier packet but never permanently. Reliability over a lossy transport is an
/// explicit non-goal until such a transport exists. When the transport already
/// orders (<see cref="PeerLinkCapabilities.IsOrdered"/>), the Ordered policy is a
/// pass-through.
/// </para>
///
/// <para>
/// <b>Ownership.</b> The mux does <b>not</b> own the <see cref="IPeerLink"/> — the
/// caller (the match / session) created it and disposes it. Disposing the mux
/// stops the drain and completes the inbound queues; it leaves the link alone.
/// </para>
///
/// <para>
/// <b>Diagnostics.</b> The mux is the first producer of netcode stats. It exposes
/// cumulative per-channel counters via <see cref="Stats"/> (a pull surface the
/// wider <c>NetDiagnostics</c> aggregates) so nothing rolls blind, without taking
/// a logging dependency.
/// </para>
/// </summary>
public sealed class ChannelMux : IAsyncDisposable
{
    /// <summary>Header size: 1 byte channel id + 2 bytes big-endian sequence.</summary>
    internal const int HeaderBytes = 3;

    /// <summary>
    /// Cap on an Ordered channel's reorder buffer. Unreachable on a reliable
    /// transport (gaps always fill quickly); a safety valve only if a future
    /// lossy transport drops a packet permanently, so the channel skips the hole
    /// rather than stalling forever.
    /// </summary>
    private const int MaxReorderBuffer = 1024;

    private readonly IPeerLink _link;
    private readonly bool _transportOrdered;
    private readonly Dictionary<byte, ChannelState> _channels;
    private readonly ChannelState[] _ordered; // declaration order, for stable Stats
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _drainTask;

    private long _malformedPackets;
    private long _unknownChannelPackets;
    private int _disposed;

    /// <summary>
    /// Wrap <paramref name="link"/> and start draining it immediately. The
    /// declared <paramref name="channels"/> fix the id→mode mapping; the same set
    /// must be declared on the peer.
    /// </summary>
    /// <param name="link">The single-peer transport to multiplex. Not owned —
    ///   the caller disposes it.</param>
    /// <param name="channels">The channels to expose; at least one, with unique
    ///   ids.</param>
    /// <exception cref="ArgumentException">No channels, or a duplicate id.</exception>
    public ChannelMux(IPeerLink link, IEnumerable<ChannelSpec> channels)
    {
        ArgumentNullException.ThrowIfNull(link);
        ArgumentNullException.ThrowIfNull(channels);

        _link = link;
        _transportOrdered = link.Capabilities.IsOrdered;

        _channels = new Dictionary<byte, ChannelState>();
        var list = new List<ChannelState>();
        foreach (var spec in channels)
        {
            var state = new ChannelState(spec);
            if (!_channels.TryAdd(spec.Id, state))
            {
                throw new ArgumentException($"Duplicate channel id {spec.Id}.", nameof(channels));
            }

            list.Add(state);
        }

        if (_channels.Count == 0)
        {
            throw new ArgumentException("At least one channel is required.", nameof(channels));
        }

        _ordered = list.ToArray();
        _drainTask = Task.Run(() => DrainLoopAsync(_cts.Token));
    }

    /// <summary>The channels this mux exposes, in declaration order.</summary>
    public IReadOnlyList<ChannelSpec> Channels =>
        Array.ConvertAll(_ordered, s => s.Spec);

    /// <summary>
    /// Frame <paramref name="payload"/> with the channel's next sequence number
    /// and send it on the link. Sends go straight from the calling (game) thread;
    /// the per-channel sequence counter is the only shared send state and is
    /// advanced under a short lock.
    /// </summary>
    /// <param name="channelId">A declared channel id.</param>
    /// <param name="payload">The bytes to send. With the 3-byte header this must
    ///   stay within <see cref="PeerLinkCapabilities.MaxPayloadBytes"/> when set
    ///   (fragmentation above the cap is a higher tier's job).</param>
    /// <param name="ct">Cancels the send.</param>
    /// <exception cref="ArgumentException"><paramref name="channelId"/> is not declared.</exception>
    public async ValueTask SendAsync(byte channelId, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var state = GetChannel(channelId);

        ushort seq;
        lock (state.SendLock)
        {
            seq = state.NextSendSeq++;
        }

        var frame = new byte[HeaderBytes + payload.Length];
        frame[0] = channelId;
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(1), seq);
        payload.Span.CopyTo(frame.AsSpan(HeaderBytes));

        await _link.SendAsync(frame, ct).ConfigureAwait(false);

        Interlocked.Increment(ref state.PacketsSent);
        Interlocked.Add(ref state.BytesSent, frame.Length);
    }

    /// <summary>
    /// Take the next ready payload for <paramref name="channelId"/> without
    /// blocking. Returns <c>false</c> when the channel's inbound queue is empty.
    /// </summary>
    public bool TryReceive(byte channelId, out ReadOnlyMemory<byte> payload) =>
        GetChannel(channelId).Inbound.Reader.TryRead(out payload);

    /// <summary>
    /// Drain every currently-ready payload for <paramref name="channelId"/> into
    /// <paramref name="sink"/> and return how many were added. The natural
    /// per-frame call: pull everything the drain has resequenced so far. For a
    /// <see cref="ChannelMode.KeepLatest"/> channel the last item added is the
    /// freshest snapshot.
    /// </summary>
    public int DrainInto(byte channelId, ICollection<ReadOnlyMemory<byte>> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        var reader = GetChannel(channelId).Inbound.Reader;
        var count = 0;
        while (reader.TryRead(out var payload))
        {
            sink.Add(payload);
            count++;
        }

        return count;
    }

    /// <summary>
    /// A fresh snapshot of every channel's cumulative counters plus mux-level
    /// drop counts. Cheap enough to poll each frame.
    /// </summary>
    public ChannelMuxStats Stats
    {
        get
        {
            var channels = new ChannelStats[_ordered.Length];
            for (var i = 0; i < _ordered.Length; i++)
            {
                var s = _ordered[i];
                channels[i] = new ChannelStats(
                    s.Spec.Id,
                    s.Spec.Mode,
                    Interlocked.Read(ref s.PacketsSent),
                    Interlocked.Read(ref s.BytesSent),
                    Interlocked.Read(ref s.PacketsReceived),
                    Interlocked.Read(ref s.BytesReceived),
                    Interlocked.Read(ref s.DroppedStale),
                    Interlocked.Read(ref s.Reordered));
            }

            return new ChannelMuxStats(
                channels,
                Interlocked.Read(ref _malformedPackets),
                Interlocked.Read(ref _unknownChannelPackets));
        }
    }

    private ChannelState GetChannel(byte channelId)
    {
        if (!_channels.TryGetValue(channelId, out var state))
        {
            throw new ArgumentException($"Channel {channelId} is not declared on this mux.", nameof(channelId));
        }

        return state;
    }

    private async Task DrainLoopAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var msg in _link.ReceiveAsync(ct).ConfigureAwait(false))
            {
                Dispatch(msg.Span);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal shutdown: DisposeAsync cancelled the drain.
        }
    }

    private void Dispatch(ReadOnlySpan<byte> frame)
    {
        if (frame.Length < HeaderBytes)
        {
            Interlocked.Increment(ref _malformedPackets);
            return;
        }

        var channelId = frame[0];
        var seq = BinaryPrimitives.ReadUInt16BigEndian(frame.Slice(1));

        if (!_channels.TryGetValue(channelId, out var state))
        {
            Interlocked.Increment(ref _unknownChannelPackets);
            return;
        }

        Interlocked.Increment(ref state.PacketsReceived);
        Interlocked.Add(ref state.BytesReceived, frame.Length);

        // The frame is only valid for this iteration (IPeerLink buffer-lifetime
        // contract), so copy the payload out before it's queued or buffered.
        var payload = frame.Slice(HeaderBytes).ToArray();

        switch (state.Spec.Mode)
        {
            case ChannelMode.Unreliable:
                Deliver(state, payload);
                break;
            case ChannelMode.KeepLatest:
                DispatchKeepLatest(state, seq, payload);
                break;
            case ChannelMode.Ordered:
                if (_transportOrdered)
                {
                    Deliver(state, payload); // transport already orders ⇒ no resequencing
                }
                else
                {
                    DispatchOrdered(state, seq, payload);
                }

                break;
        }
    }

    private static void Deliver(ChannelState state, byte[] payload) =>
        state.Inbound.Writer.TryWrite(payload);

    private static void DispatchKeepLatest(ChannelState state, ushort seq, byte[] payload)
    {
        if (state.HasReceived && !SequenceNumber.GreaterThan(seq, state.HighestSeq))
        {
            Interlocked.Increment(ref state.DroppedStale);
            return;
        }

        state.HasReceived = true;
        state.HighestSeq = seq;
        Deliver(state, payload);
    }

    private static void DispatchOrdered(ChannelState state, ushort seq, byte[] payload)
    {
        // The sender's per-channel seq starts at 0, so the receiver's expected
        // seq also starts at 0 (the ushort default) — no baseline negotiation.
        if (SequenceNumber.GreaterThan(state.NextExpectedSeq, seq))
        {
            // Already released this seq (or earlier) — a stale duplicate.
            Interlocked.Increment(ref state.DroppedStale);
            return;
        }

        if (seq != state.NextExpectedSeq)
        {
            // A future packet: hold it until the gap ahead of it fills.
            state.ReorderBuffer[seq] = payload;
            Interlocked.Increment(ref state.Reordered);
            ClampReorderBuffer(state);
            return;
        }

        // seq == NextExpected: release it, then any now-contiguous buffered run.
        Deliver(state, payload);
        state.NextExpectedSeq++;
        while (state.ReorderBuffer.Remove(state.NextExpectedSeq, out var buffered))
        {
            Deliver(state, buffered);
            state.NextExpectedSeq++;
        }
    }

    private static void ClampReorderBuffer(ChannelState state)
    {
        // Only reachable if a packet went permanently missing (lossy transport,
        // currently out of scope). Skip the hole so the channel keeps flowing:
        // advance past the missing seq(s) until the next buffered packet, then
        // release the contiguous run from there.
        while (state.ReorderBuffer.Count > MaxReorderBuffer)
        {
            state.NextExpectedSeq++;
            while (state.ReorderBuffer.Remove(state.NextExpectedSeq, out var buffered))
            {
                Deliver(state, buffered);
                state.NextExpectedSeq++;
            }
        }
    }

    /// <summary>
    /// Stop the drain and complete the inbound queues. Does not dispose the
    /// underlying <see cref="IPeerLink"/> — the caller owns its lifetime.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _cts.Cancel();
        try
        {
            await _drainTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Drain cancelled — expected.
        }

        foreach (var state in _ordered)
        {
            state.Inbound.Writer.TryComplete();
        }

        _cts.Dispose();
    }

    /// <summary>
    /// Per-channel state. Send-side fields are touched only by the game thread
    /// (guarded by <see cref="SendLock"/> for the seq counter); receive-side
    /// fields are touched only by the single drain task. Counters are updated
    /// with <see cref="Interlocked"/> and read the same way for <see cref="Stats"/>.
    /// </summary>
    private sealed class ChannelState
    {
        public ChannelState(ChannelSpec spec)
        {
            Spec = spec;
            Inbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
        }

        public ChannelSpec Spec { get; }
        public Channel<ReadOnlyMemory<byte>> Inbound { get; }

        // Send-side.
        public readonly object SendLock = new();
        public ushort NextSendSeq;

        // Receive-side (drain task only).
        public bool HasReceived;        // KeepLatest: at least one packet seen
        public ushort HighestSeq;       // KeepLatest: highest seq delivered
        public ushort NextExpectedSeq;  // Ordered: next seq to release (starts at 0)
        public readonly Dictionary<ushort, byte[]> ReorderBuffer = new();

        // Counters (Interlocked).
        public long PacketsSent;
        public long BytesSent;
        public long PacketsReceived;
        public long BytesReceived;
        public long DroppedStale;
        public long Reordered;
    }
}
