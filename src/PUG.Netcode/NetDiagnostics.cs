using System.Text;

namespace PUG.Netcode;

/// <summary>
/// One observability surface for the whole netcode stack, so it can be watched as
/// it's built rather than debugged only in hindsight. Netcode fails silently and
/// subtly — the canonical PUG e2e (learning <c>68df6c92</c>) wasted a debugging
/// session because the sample injected a null logger and every diagnostic
/// vanished; the FIRST fix was wiring a real one. This is that seam, made
/// first-class and dependency-free.
///
/// <para>It offers two things:</para>
/// <list type="number">
/// <item><b>A log sink</b> — a single <see cref="Action{T1, T2}"/> the host points
///   at its own logger. Default is a safe no-op, but unlike a null logger this is
///   a deliberate, overridable seam, not a silent void.</item>
/// <item><b>A stats snapshot</b> — <see cref="Snapshot"/> pulls cumulative counters
///   from every registered source into one record the host polls each frame, and
///   <see cref="Describe"/> formats them for an F3-style overlay.</item>
/// </list>
///
/// <para>
/// <b>Pull, not push.</b> Sources (today the <see cref="ChannelMux"/>; later
/// TimeSync RTT, session link state, snapshot age, prediction error) expose their
/// own counters and diagnostics <i>reads</i> them on demand. That keeps each tier
/// free of any diagnostics/logging dependency — they just expose stats.
/// </para>
/// </summary>
public sealed class NetDiagnostics
{
    private static readonly Action<NetLogLevel, string> NoSink = static (_, _) => { };

    private readonly object _gate = new();
    private readonly List<(string Label, ChannelMux Mux)> _muxes = new();
    private Action<NetLogLevel, string> _logSink = NoSink;

    /// <summary>
    /// Where log events go. Set this to bridge into the host's logger; assigning
    /// <c>null</c> restores the no-op sink. Defaults to no-op — nothing is lost,
    /// but nothing is emitted until the host opts in.
    /// </summary>
    public Action<NetLogLevel, string> LogSink
    {
        get => _logSink;
        set => _logSink = value ?? NoSink;
    }

    /// <summary>Emit a log event at <paramref name="level"/> to the current sink.</summary>
    public void Log(NetLogLevel level, string message) => _logSink(level, message);

    /// <summary>Emit a <see cref="NetLogLevel.Trace"/> event.</summary>
    public void Trace(string message) => _logSink(NetLogLevel.Trace, message);

    /// <summary>Emit a <see cref="NetLogLevel.Debug"/> event.</summary>
    public void Debug(string message) => _logSink(NetLogLevel.Debug, message);

    /// <summary>Emit an <see cref="NetLogLevel.Info"/> event.</summary>
    public void Info(string message) => _logSink(NetLogLevel.Info, message);

    /// <summary>Emit a <see cref="NetLogLevel.Warn"/> event.</summary>
    public void Warn(string message) => _logSink(NetLogLevel.Warn, message);

    /// <summary>Emit an <see cref="NetLogLevel.Error"/> event.</summary>
    public void Error(string message) => _logSink(NetLogLevel.Error, message);

    /// <summary>
    /// Register a <see cref="ChannelMux"/> so its per-channel counters appear in
    /// <see cref="Snapshot"/> and <see cref="Describe"/> under
    /// <paramref name="label"/> (e.g. the peer's name).
    /// </summary>
    public void RegisterMux(string label, ChannelMux mux)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(label);
        ArgumentNullException.ThrowIfNull(mux);
        lock (_gate)
        {
            _muxes.Add((label, mux));
        }
    }

    /// <summary>
    /// Pull a fresh snapshot of every registered source's counters. Cheap enough
    /// to call each frame.
    /// </summary>
    public NetDiagnosticsSnapshot Snapshot()
    {
        lock (_gate)
        {
            var muxes = new MuxDiagnostics[_muxes.Count];
            for (var i = 0; i < _muxes.Count; i++)
            {
                muxes[i] = new MuxDiagnostics(_muxes[i].Label, _muxes[i].Mux.Stats);
            }

            return new NetDiagnosticsSnapshot(muxes);
        }
    }

    /// <summary>
    /// A human-readable multi-line dump of the current snapshot, for a debug
    /// overlay or a one-shot log line. Stable enough to eyeball; not meant to be
    /// parsed (poll <see cref="Snapshot"/> for that).
    /// </summary>
    public string Describe()
    {
        var snapshot = Snapshot();
        var sb = new StringBuilder();
        sb.Append("NetDiagnostics: ").Append(snapshot.Muxes.Count).AppendLine(" mux(es)");

        foreach (var mux in snapshot.Muxes)
        {
            sb.Append("  mux \"").Append(mux.Label).Append('"');
            if (mux.Stats.MalformedPackets > 0 || mux.Stats.UnknownChannelPackets > 0)
            {
                sb.Append(" [malformed ").Append(mux.Stats.MalformedPackets)
                    .Append(", unknown-channel ").Append(mux.Stats.UnknownChannelPackets).Append(']');
            }

            sb.AppendLine();

            foreach (var ch in mux.Stats.Channels)
            {
                sb.Append("    ch").Append(ch.ChannelId).Append(' ').Append(ch.Mode)
                    .Append(": sent ").Append(ch.PacketsSent).Append('/').Append(ch.BytesSent).Append('B')
                    .Append(", recv ").Append(ch.PacketsReceived).Append('/').Append(ch.BytesReceived).Append('B')
                    .Append(", stale ").Append(ch.DroppedStale)
                    .Append(", reorder ").Append(ch.Reordered)
                    .AppendLine();
            }
        }

        return sb.ToString();
    }
}

/// <summary>
/// One registered <see cref="ChannelMux"/>'s counters within a
/// <see cref="NetDiagnosticsSnapshot"/>.
/// </summary>
/// <param name="Label">The label the mux was registered under.</param>
/// <param name="Stats">The mux's counter snapshot at poll time.</param>
public readonly record struct MuxDiagnostics(string Label, ChannelMuxStats Stats);

/// <summary>
/// A point-in-time aggregate of everything <see cref="NetDiagnostics"/> can see.
/// Grows a field per tier as they land (TimeSync RTT/offset, session link state,
/// Tier B snapshot age, Tier C prediction error); today it carries the channel
/// muxes.
/// </summary>
/// <param name="Muxes">Per-mux counters, in registration order.</param>
public readonly record struct NetDiagnosticsSnapshot(IReadOnlyList<MuxDiagnostics> Muxes);
