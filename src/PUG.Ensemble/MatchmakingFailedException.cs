namespace PUG.Ensemble;

/// <summary>
/// Thrown when the matchmaker-side flow fails in a way the player cannot
/// recover from: an <c>ErrorResponse</c> envelope from the matchmaker, an
/// unreachable matchmaker peer (<c>ConnectAsync</c> reported
/// <c>Accepted=false</c>), or a malformed reply on the wire.
///
/// Adversarial-introduction drops (provenance / expiry / session-id mismatch)
/// do NOT throw — they are silently discarded so the handle stays open and
/// the legitimate introduction can still arrive. This exception is reserved
/// for terminal failures.
/// </summary>
public sealed class MatchmakingFailedException : Exception
{
    /// <summary>
    /// Stable matchmaker-side classifier (e.g. <c>"queue_closed"</c>,
    /// <c>"unknown_code"</c>) lifted verbatim from
    /// <see cref="Proto.ErrorResponse.Code"/>. Empty for failures originating
    /// in the local SDK (connect refused, malformed wire reply, etc.).
    /// </summary>
    public string Code { get; }

    /// <summary>Constructs the exception with an empty <see cref="Code"/>.</summary>
    public MatchmakingFailedException(string message)
        : base(message)
    {
        Code = string.Empty;
    }

    /// <summary>Constructs the exception with a structured matchmaker error code.</summary>
    public MatchmakingFailedException(string message, string code)
        : base(message)
    {
        Code = code ?? string.Empty;
    }

    /// <summary>Constructs the exception wrapping an inner cause.</summary>
    public MatchmakingFailedException(string message, Exception inner)
        : base(message, inner)
    {
        Code = string.Empty;
    }
}
