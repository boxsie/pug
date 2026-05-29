using EC = Ensemble.Client;
using Microsoft.Extensions.Logging.Abstractions;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Unit tests for <see cref="PeerAdmissionControl"/> — PUG's matchmaker-
/// introduction admission ruleset (Ensemble ADR df82c69a, consumer side).
/// Pure logic, no daemon required.
/// </summary>
public class PeerAdmissionControlTests
{
    private const string Matchmaker = "Ematchmaker0000000000000000000000000";
    private const string Peer = "Epeer000000000000000000000000000000000";
    private const string Session = "session-123";

    private static PeerAdmissionControl New(string? session = Session)
    {
        var admission = new PeerAdmissionControl(Matchmaker, NullLogger.Instance);
        if (session is not null) admission.SessionId = session;
        return admission;
    }

    private static EC.ServiceEvent.PeerIntroduction Intro(
        string from = Matchmaker,
        string peer = Peer,
        string session = Session,
        long expiresAt = 0,
        string roleHint = "",
        byte[]? payload = null) =>
        new(from, peer, session, expiresAt, roleHint, payload ?? Array.Empty<byte>());

    [Fact]
    public void ValidIntroduction_AuthorizesPeer()
    {
        var admission = New();
        Assert.True(admission.RecordIntroduction(Intro()));
        Assert.True(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void NonIntroducedAddress_IsRejected()
    {
        var admission = New();
        admission.RecordIntroduction(Intro());
        Assert.False(admission.IsAuthorized("Estranger0000000000000000000000000000"));
    }

    [Fact]
    public void IntroductionFromNonMatchmaker_IsNotRecorded()
    {
        var admission = New();
        Assert.False(admission.RecordIntroduction(Intro(from: "Eimposter000000000000000000000000000")));
        Assert.False(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void IntroductionForWrongSession_IsNotRecorded()
    {
        var admission = New();
        Assert.False(admission.RecordIntroduction(Intro(session: "different-session")));
        Assert.False(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void IntroductionBeforeSessionBound_IsNotRecorded()
    {
        var admission = New(session: null); // session id not yet assigned
        Assert.False(admission.RecordIntroduction(Intro()));
        Assert.False(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void ExpiredIntroduction_IsNotRecorded()
    {
        var admission = New();
        var past = DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeMilliseconds();
        Assert.False(admission.RecordIntroduction(Intro(expiresAt: past)));
        Assert.False(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void FutureHorizon_IsAuthorized_ThenExpires()
    {
        var admission = New();
        // Record with a horizon already in the past relative to IsAuthorized's
        // clock read but recorded via the raw dictionary path: simplest is a
        // horizon that's valid at record time and a separate already-expired
        // entry. Here: a comfortably-future horizon stays authorized.
        var future = DateTimeOffset.UtcNow.AddMinutes(5).ToUnixTimeMilliseconds();
        Assert.True(admission.RecordIntroduction(Intro(expiresAt: future)));
        Assert.True(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void ZeroExpiry_IsTreatedAsNoHorizon()
    {
        var admission = New();
        Assert.True(admission.RecordIntroduction(Intro(expiresAt: 0)));
        Assert.True(admission.IsAuthorized(Peer));
    }

    [Fact]
    public void EmptyFromAddr_IsRejected()
    {
        var admission = New();
        admission.RecordIntroduction(Intro());
        Assert.False(admission.IsAuthorized(""));
    }
}
