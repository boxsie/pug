namespace PUG.Netcode;

/// <summary>
/// Severity of a <see cref="NetDiagnostics"/> log event. Deliberately a tiny,
/// dependency-free mirror of the usual logging levels so the host can map it onto
/// whatever logger it actually uses (Godot <c>GD.Print</c>, <c>ILogger</c>, a
/// file) without <c>PUG.Netcode</c> taking a logging dependency.
/// </summary>
public enum NetLogLevel
{
    /// <summary>Very fine-grained tracing — per-packet / per-tick detail.</summary>
    Trace,

    /// <summary>Diagnostic detail useful when actively debugging.</summary>
    Debug,

    /// <summary>Normal lifecycle events (session up, peer joined).</summary>
    Info,

    /// <summary>Something unexpected but recoverable (a stall, a dropped frame).</summary>
    Warn,

    /// <summary>A failure the host should surface (link lost, unrecoverable state).</summary>
    Error,
}
