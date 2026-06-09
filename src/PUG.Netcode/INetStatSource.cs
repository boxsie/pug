namespace PUG.Netcode;

/// <summary>
/// A pull-model source of named numeric stats contributed from <i>outside</i> the
/// core assembly. The built-in sources (<see cref="ChannelMux"/>, <see cref="TimeSync"/>,
/// <see cref="NetworkReplicator"/>) each expose a typed <c>Stats</c> record that
/// <see cref="NetDiagnostics.Snapshot"/> reads on demand — but a typed field can only
/// be added by code that can edit <see cref="NetDiagnosticsSnapshot"/>, i.e. code in
/// this assembly. An extension package (e.g. <c>PUG.Netcode.Prediction</c>) cannot.
///
/// <para>
/// This interface is the generic escape hatch: register an implementation with
/// <see cref="NetDiagnostics.RegisterSource"/> and its <see cref="SampleStats"/> output
/// appears in every <see cref="NetDiagnostics.Snapshot"/> as a named
/// <see cref="NamedStatGroup"/>, with the core knowing nothing about what the numbers
/// <i>mean</i>. Same pull discipline as the built-ins (diagnostics reads sources; sources
/// never push), just with no compile-time coupling to the stat's shape. This is what makes
/// the diagnostics surface itself unopinionated across an assembly boundary.
/// </para>
/// </summary>
public interface INetStatSource
{
    /// <summary>
    /// Pull the source's current named counters/gauges. Called on demand by
    /// <see cref="NetDiagnostics.Snapshot"/> — cheap enough to call each frame, and must
    /// not block. Return an empty list (not null) when there is nothing to report.
    /// </summary>
    IReadOnlyList<NetStat> SampleStats();
}

/// <summary>
/// One named numeric stat in a <see cref="INetStatSource.SampleStats"/> reading — a
/// counter or gauge the host can surface on an overlay or poll programmatically. Numeric
/// by design: it keeps the contract trivial and renderable without the core understanding
/// the value (interp buffer depth, underruns, current interp-delay ticks, reconciliation
/// error, …).
/// </summary>
/// <param name="Name">Short stat name, unique within its source (e.g. <c>"bufferDepth"</c>).</param>
/// <param name="Value">The current value.</param>
public readonly record struct NetStat(string Name, double Value);
