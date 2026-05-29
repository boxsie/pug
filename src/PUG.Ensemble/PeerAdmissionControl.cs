using System.Collections.Concurrent;
using EnsembleNS = Ensemble.Client;
using Microsoft.Extensions.Logging;

namespace PUG.Ensemble;

/// <summary>
/// PUG's admission ruleset for introduced peers — the consumer side of the
/// Ensemble connection-authorization model (ADR <c>df82c69a</c>). Ensemble
/// stays default-deny; PUG decides for itself which inbound connections to
/// accept, reasoning purely in <b>service-identity</b> terms (never node
/// addresses).
///
/// <para>
/// The matchmaker is the chosen trust anchor. A peer is admissible iff the
/// verified matchmaker introduced us to that peer's service address within a
/// still-valid session horizon. Introductions are daemon-attested
/// (<see cref="EnsembleNS.ServiceEvent.PeerIntroduction.FromServiceAddr"/> is
/// stamped by the local daemon), but the SDK cross-checks anyway:
/// (1) provenance — <c>FromServiceAddr == matchmakerAddr</c>,
/// (2) session — <c>SessionId == this session's id</c>,
/// (3) expiry — <c>ExpiresAt</c> in the future (<c>0</c> = "no horizon").
/// </para>
///
/// <para>
/// <b>Invariant (from the ADR): an introduction is information, never a
/// grant.</b> Recording an introduced peer only makes that peer
/// <see cref="IsAuthorized"/>; the actual accept/reject of an inbound
/// <c>connection_request</c> is the caller's call, gated on this set.
/// </para>
///
/// <para>Thread-safe: the registered service's event callback records
/// introductions and queries admission from the daemon's stream-reader loop
/// while game code may read concurrently.</para>
/// </summary>
internal sealed class PeerAdmissionControl
{
    private readonly string _matchmakerAddr;
    private readonly ILogger _logger;

    // Authorized peer service-addr -> introduction expiry (unix ms; 0 = no
    // horizon). A peer is admitted only while a non-expired entry exists.
    private readonly ConcurrentDictionary<string, long> _authorized = new(StringComparer.Ordinal);

    // The matchmaker-issued session id this admission set is scoped to. Unknown
    // until the QueuedResponse first-reply lands, so it is set post-handshake.
    // Introductions and connection requests only arrive after a match forms
    // (well after the session id is known), so the late binding is safe.
    private volatile string? _sessionId;

    internal PeerAdmissionControl(string matchmakerAddr, ILogger logger)
    {
        if (string.IsNullOrWhiteSpace(matchmakerAddr))
            throw new ArgumentException("matchmakerAddr is required", nameof(matchmakerAddr));
        _matchmakerAddr = matchmakerAddr;
        _logger = logger;
    }

    /// <summary>The session id this admission set authorizes against. Set once
    /// the matchmaker's QueuedResponse assigns it.</summary>
    internal string? SessionId
    {
        get => _sessionId;
        set => _sessionId = value;
    }

    /// <summary>
    /// Validate an introduction against provenance, session, and expiry. Pure
    /// — records nothing. <see cref="QueueHandle{TPayload}"/> uses this so the
    /// match-wait path and the admission set share one definition of "valid".
    /// </summary>
    internal bool IsValidIntroduction(EnsembleNS.ServiceEvent.PeerIntroduction intro)
    {
        if (intro.FromServiceAddr != _matchmakerAddr) return false;
        var session = _sessionId;
        if (session is null || intro.SessionId != session) return false;
        // ExpiresAt == 0 is the daemon's "no horizon" sentinel; accept those.
        if (intro.ExpiresAt != 0 && intro.ExpiresAt <= NowMs()) return false;
        return true;
    }

    /// <summary>
    /// If the introduction is valid, authorize its peer service address for
    /// inbound connections until the introduction's horizon. Returns whether
    /// the introduction was recorded.
    /// </summary>
    internal bool RecordIntroduction(EnsembleNS.ServiceEvent.PeerIntroduction intro)
    {
        if (!IsValidIntroduction(intro)) return false;
        _authorized[intro.PeerAddr] = intro.ExpiresAt;
        return true;
    }

    /// <summary>
    /// Whether an inbound connection from <paramref name="fromAddr"/> is from a
    /// peer the verified matchmaker introduced us to, within its session
    /// horizon. Expired entries are evicted on read.
    /// </summary>
    internal bool IsAuthorized(string fromAddr)
    {
        if (string.IsNullOrEmpty(fromAddr)) return false;
        if (!_authorized.TryGetValue(fromAddr, out var expiresAt)) return false;
        if (expiresAt != 0 && expiresAt <= NowMs())
        {
            _authorized.TryRemove(fromAddr, out _);
            return false;
        }
        return true;
    }

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
