using System.Buffers.Binary;
using System.Diagnostics;

namespace PUG.Netcode;

/// <summary>
/// Estimates round-trip time and the <b>tick offset</b> to the authoritative end
/// over one peer link, by periodic ping/pong on a reserved A1 channel. Tier C
/// needs both: RTT to pick an interpolation delay, and "what tick is the authority
/// on <i>now</i>" so a predicted input is stamped with the tick the authority will
/// actually process it on.
///
/// <para>
/// <b>Ping/pong, not acks.</b> We deliberately don't carry an ack bitfield (that's
/// reliability machinery this stack doesn't need on a reliable transport). Instead
/// a ping carries <c>(sendTimestamp, senderTick)</c>; the peer echoes a pong
/// carrying the original timestamp plus <i>its</i> current tick. On the pong:
/// <c>rtt = now − sendTimestamp</c>, and the authority's tick now ≈
/// <c>responderTick + rtt/2</c>, so <c>offset = responderTick + rtt/2 − localTick</c>.
/// </para>
///
/// <para>
/// <b>Smoothing: sliding-min.</b> The sample with the <i>smallest</i> RTT in a
/// short window queued the least, so it carries the truest offset (classic
/// NTP-lite). <see cref="Stats"/> reports that window-min RTT and the offset
/// paired with it — a single jitter spike is never the minimum, so it's ignored
/// until it ages out rather than chased.
/// </para>
///
/// <para>
/// <b>Symmetric, pumped, one reference.</b> Every instance answers pings it
/// receives (a cheap stateless echo). Only an instance with
/// <see cref="TimeSyncOptions.AutoPing"/> set <i>initiates</i> them — the syncing
/// end (P2P guest → host, client → server). The authority answers but doesn't sync
/// to anyone. Like <see cref="TickClock"/> it owns no thread and no
/// <c>Task.Delay</c>: the game pumps <see cref="UpdateAsync"/> each frame, which
/// drains the channel and sends a ping when one is due.
/// </para>
///
/// <para>
/// <b>Not thread-safe</b> — pumped from one place, same as the clock it reads.
/// </para>
/// </summary>
public sealed class TimeSync
{
    private const byte PingType = 0;
    private const byte PongType = 1;

    /// <summary>1-byte type tag + 8-byte timestamp + 8-byte tick.</summary>
    private const int MessageBytes = 1 + sizeof(long) + sizeof(ulong);

    private readonly ChannelMux _mux;
    private readonly byte _channelId;
    private readonly TickClock _localClock;
    private readonly TimeSyncOptions _options;
    private readonly long _pingIntervalStamp;
    private readonly RttWindow _window;

    private long _lastPingStamp;
    private bool _everPinged;
    private int _sampleCount;

    /// <summary>
    /// Create a time-sync over <paramref name="mux"/>'s reserved
    /// <paramref name="channelId"/> (e.g. channel 0), reading and stamping local
    /// sim ticks from <paramref name="localClock"/>.
    /// </summary>
    /// <param name="mux">The peer's A1 mux. Not owned.</param>
    /// <param name="channelId">A declared channel reserved for time-sync. A
    ///   low-rate self-describing message stream — <see cref="ChannelMode.Unreliable"/>
    ///   is the natural mode (a lost sample just skips).</param>
    /// <param name="localClock">The local fixed-step clock, for the current tick
    ///   and tick rate.</param>
    /// <param name="options">Cadence, window size, ping-initiation, and the
    ///   monotonic timestamp source (a real <see cref="Stopwatch"/> by default;
    ///   injectable for deterministic tests).</param>
    public TimeSync(ChannelMux mux, byte channelId, TickClock localClock, TimeSyncOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(mux);
        ArgumentNullException.ThrowIfNull(localClock);

        _mux = mux;
        _channelId = channelId;
        _localClock = localClock;
        _options = options ?? new TimeSyncOptions();
        _window = new RttWindow(_options.WindowSize);

        // Convert the ping interval into the timestamp source's own units once.
        _pingIntervalStamp = (long)(_options.PingInterval.TotalSeconds * _options.TimestampFrequency);
    }

    /// <summary>The channel this time-sync rides.</summary>
    public byte ChannelId => _channelId;

    /// <summary>The current smoothed estimate: window-min RTT, its paired tick
    /// offset, and how many pongs have been folded in over the session.</summary>
    public TimeSyncStats Stats
    {
        get
        {
            if (_window.Count == 0)
            {
                return new TimeSyncStats(TimeSpan.Zero, 0, 0);
            }

            var rtt = TimeSpan.FromSeconds((double)_window.BestRttTicks / _options.TimestampFrequency);
            return new TimeSyncStats(rtt, _window.BestOffsetTicks, _sampleCount);
        }
    }

    /// <summary>Smoothed round-trip time (window-min). <see cref="TimeSpan.Zero"/>
    /// until the first pong lands.</summary>
    public TimeSpan Rtt => Stats.Rtt;

    /// <summary>How many ticks ahead (positive) or behind (negative) the
    /// authority's clock is relative to the local clock.</summary>
    public long TickOffset => Stats.TickOffset;

    /// <summary>Total pongs folded into the estimate over the session.</summary>
    public int SampleCount => _sampleCount;

    /// <summary>
    /// Pump one frame: answer any pings received, fold in any pongs, and — when
    /// <see cref="TimeSyncOptions.AutoPing"/> is set and the interval has elapsed —
    /// send a fresh ping. Call once per game frame on both ends.
    /// </summary>
    public async ValueTask UpdateAsync(CancellationToken ct = default)
    {
        while (_mux.TryReceive(_channelId, out var message))
        {
            await HandleAsync(message, ct).ConfigureAwait(false);
        }

        if (_options.AutoPing && IsPingDue(_options.TimestampTicks()))
        {
            await PingAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Send a ping immediately, regardless of cadence. The deterministic entry
    /// point for tests that drive an exact round trip; production code normally
    /// lets <see cref="UpdateAsync"/> schedule pings.
    /// </summary>
    public async ValueTask PingAsync(CancellationToken ct = default)
    {
        var now = _options.TimestampTicks();
        var frame = new byte[MessageBytes];
        frame[0] = PingType;
        BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(1), now);
        BinaryPrimitives.WriteUInt64BigEndian(frame.AsSpan(1 + sizeof(long)), _localClock.CurrentTick);

        await _mux.SendAsync(_channelId, frame, ct).ConfigureAwait(false);

        _lastPingStamp = now;
        _everPinged = true;
    }

    private async ValueTask HandleAsync(ReadOnlyMemory<byte> message, CancellationToken ct)
    {
        var span = message.Span;
        if (span.Length < MessageBytes)
        {
            return; // not a time-sync message — ignore rather than guess
        }

        switch (span[0])
        {
            case PingType:
                await PongAsync(BinaryPrimitives.ReadInt64BigEndian(span.Slice(1)), ct).ConfigureAwait(false);
                break;
            case PongType:
                Ingest(span);
                break;
        }
    }

    private async ValueTask PongAsync(long echoStamp, CancellationToken ct)
    {
        var frame = new byte[MessageBytes];
        frame[0] = PongType;
        BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(1), echoStamp);
        BinaryPrimitives.WriteUInt64BigEndian(frame.AsSpan(1 + sizeof(long)), _localClock.CurrentTick);

        await _mux.SendAsync(_channelId, frame, ct).ConfigureAwait(false);
    }

    private void Ingest(ReadOnlySpan<byte> pong)
    {
        var echoStamp = BinaryPrimitives.ReadInt64BigEndian(pong.Slice(1));
        var responderTick = BinaryPrimitives.ReadUInt64BigEndian(pong.Slice(1 + sizeof(long)));

        var rttStamps = _options.TimestampTicks() - echoStamp;
        if (rttStamps < 0)
        {
            return; // a clock that went backwards — drop the sample
        }

        // Authority tick at "now" ≈ its stamped tick + half the round trip, so the
        // offset that aligns our clock to its is that minus our tick now.
        var rttSeconds = (double)rttStamps / _options.TimestampFrequency;
        var halfRttTicks = (long)Math.Round(rttSeconds / 2.0 * _localClock.TickHz);
        var offset = (long)responderTick + halfRttTicks - (long)_localClock.CurrentTick;

        _window.Push(rttStamps, offset);
        _sampleCount++;
    }

    private bool IsPingDue(long now)
    {
        return !_everPinged || (now - _lastPingStamp >= _pingIntervalStamp);
    }
}

/// <summary>
/// Tunables for a <see cref="TimeSync"/>. Defaults give a 10 Hz ping cadence, a
/// 16-sample sliding-min window, ping initiation on, and a real high-resolution
/// <see cref="Stopwatch"/> as the monotonic clock — override the clock to make
/// round trips deterministic in tests.
/// </summary>
public sealed class TimeSyncOptions
{
    /// <summary>How often <see cref="TimeSync.UpdateAsync"/> initiates a ping when
    /// <see cref="AutoPing"/> is set.</summary>
    public TimeSpan PingInterval { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>How many recent samples the sliding-min window keeps. The estimate
    /// is the minimum RTT across this window and its paired offset.</summary>
    public int WindowSize { get; init; } = 16;

    /// <summary>Whether this instance initiates pings (the syncing end) or only
    /// answers them (the authoritative reference).</summary>
    public bool AutoPing { get; init; } = true;

    /// <summary>The monotonic timestamp source, in the units of
    /// <see cref="TimestampFrequency"/>. Defaults to
    /// <see cref="Stopwatch.GetTimestamp"/>; inject a controllable counter to make
    /// RTT/offset tests deterministic.</summary>
    public Func<long> TimestampTicks { get; init; } = Stopwatch.GetTimestamp;

    /// <summary>Ticks per second of <see cref="TimestampTicks"/>. Defaults to
    /// <see cref="Stopwatch.Frequency"/>.</summary>
    public long TimestampFrequency { get; init; } = Stopwatch.Frequency;
}

/// <summary>
/// A <see cref="TimeSync"/> estimate: smoothed RTT, the tick offset to the
/// authority, and how many samples back it.
/// </summary>
/// <param name="Rtt">Window-min round-trip time.</param>
/// <param name="TickOffset">Authority tick minus local tick (signed).</param>
/// <param name="SampleCount">Total pongs folded in over the session.</param>
public readonly record struct TimeSyncStats(TimeSpan Rtt, long TickOffset, int SampleCount);

/// <summary>
/// A fixed-capacity sliding window over recent (RTT, offset) samples that reports
/// the minimum-RTT sample. The min RTT queued the least, so its paired offset is
/// the most trustworthy; a lone latency spike never wins the minimum and so is
/// ignored until it ages out. RTT is held in raw timestamp ticks (cheaper to
/// compare; converted to time at the reporting edge).
/// </summary>
internal sealed class RttWindow
{
    private readonly long[] _rtt;
    private readonly long[] _offset;
    private int _next;

    public RttWindow(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _rtt = new long[capacity];
        _offset = new long[capacity];
    }

    /// <summary>How many samples are currently held (≤ capacity).</summary>
    public int Count { get; private set; }

    /// <summary>The minimum RTT in the window, in timestamp ticks.</summary>
    public long BestRttTicks { get; private set; }

    /// <summary>The offset paired with <see cref="BestRttTicks"/>.</summary>
    public long BestOffsetTicks { get; private set; }

    /// <summary>Add a sample, evicting the oldest once full, and recompute the
    /// window minimum.</summary>
    public void Push(long rttTicks, long offsetTicks)
    {
        _rtt[_next] = rttTicks;
        _offset[_next] = offsetTicks;
        _next = (_next + 1) % _rtt.Length;
        if (Count < _rtt.Length)
        {
            Count++;
        }

        var bestIndex = 0;
        for (var i = 1; i < Count; i++)
        {
            if (_rtt[i] < _rtt[bestIndex])
            {
                bestIndex = i;
            }
        }

        BestRttTicks = _rtt[bestIndex];
        BestOffsetTicks = _offset[bestIndex];
    }
}
