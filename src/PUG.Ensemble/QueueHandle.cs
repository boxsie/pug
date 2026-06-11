using System.Runtime.CompilerServices;
using System.Threading.Channels;
using EnsembleNS = Ensemble.Client;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using PUG.Ensemble.Proto;

namespace PUG.Ensemble;

/// <summary>
/// Per-queue handle returned by
/// <see cref="EnsemblePlayerClient.JoinMatchmakingAsync{TPayload}"/>. Owns
/// the player-side <see cref="EnsembleNS.RegisteredService"/> used to RPC
/// the matchmaker and to receive its replies + the daemon-attested
/// <c>peer_introduction</c> events. Disposing the handle deregisters that
/// service (clean half-close of the bidi stream) and tears down the wait
/// machinery; <see cref="LeaveAsync"/> additionally sends a
/// <see cref="LeaveQueueRequest"/> first.
///
/// <para>
/// <b>Provenance &amp; replay protection.</b> <see cref="WaitForMatchAsync"/>
/// filters incoming <c>PeerIntroduction</c> events through three mandatory
/// checks before completing: (1) <c>FromServiceAddr == MatchmakerAddr</c>
/// (the daemon attests provenance, but the SDK cross-checks anyway),
/// (2) <c>ExpiresAt</c> is in the future (treating <c>ExpiresAt == 0</c> as
/// "no horizon, accept"), and (3) <c>SessionId == this.SessionId</c>. Drops
/// are silent — the handle stays open so the legitimate introduction can
/// still land.
/// </para>
///
/// <para>
/// <b>Single-introduction completion model.</b> The ticket spec doesn't pin
/// a multi-peer wait policy; the first valid introduction completes the
/// task with a single <see cref="PeerEndpoint"/>. This matches the typical
/// 1v1 / introduce-to-host shape; for an N-peer mesh the matchmaker is
/// expected to deliver introductions in sequence and game code can re-queue
/// for additional peers.
/// </para>
/// </summary>
/// <typeparam name="TPayload">The host-supplied per-player payload type the
///   matchmaker bucket consumes. The SDK never deserialises it — only the
///   caller-supplied serializer is invoked, on send.</typeparam>
public sealed class QueueHandle<TPayload> : IAsyncDisposable, IPeerChannel
{
    // Separate channels keep WaitForMatchAsync and StatusStream from racing
    // for events: introductions/errors fan into one channel, queue-status
    // updates into the other. Either consumer can be absent without the
    // other starving — the matchmaker callback writes to both unconditionally.
    private readonly EnsembleNS.EnsembleClient _ensemble;
    private readonly EnsembleNS.RegisteredService _service;
    private readonly ILogger _logger;
    private readonly Channel<MatchSignal> _matchSignals;
    private readonly Channel<QueueStatus> _statusUpdates;
    private readonly Channel<PeerMessage> _peerMessages;
    private readonly PeerAdmissionControl _admission;
    private readonly CancellationTokenSource _shutdown = new();
    private int _disposed;
    private int _matchCompleted;

    /// <summary>The matchmaker E-address this handle was queued at.</summary>
    public string MatchmakerAddr { get; }

    /// <summary>The matchmaker-issued session id correlating this queue entry.</summary>
    public string SessionId { get; }

    /// <summary>
    /// This player's per-handle registered-service E-address. The matchmaker
    /// targets it with <c>IntroducePeersAsync</c>, and it is the address the
    /// peer sees as the dialer once we connect as our service identity. Game
    /// code uses it to elect a deterministic host by comparing it against the
    /// peer's service address (both sides order the same service-addr pair).
    /// </summary>
    public string PlayerServiceAddress => _service.ServiceAddress;

    internal QueueHandle(
        EnsembleNS.EnsembleClient ensemble,
        EnsembleNS.RegisteredService service,
        Channel<MatchSignal> matchSignals,
        Channel<QueueStatus> statusUpdates,
        Channel<PeerMessage> peerMessages,
        string matchmakerAddr,
        string sessionId,
        PeerAdmissionControl admission,
        ILogger logger)
    {
        _ensemble = ensemble;
        _service = service;
        _matchSignals = matchSignals;
        _statusUpdates = statusUpdates;
        _peerMessages = peerMessages;
        _admission = admission;
        _logger = logger;
        MatchmakerAddr = matchmakerAddr;
        SessionId = sessionId;
    }

    /// <summary>
    /// Wait for the matchmaker to introduce a peer for this session. Completes
    /// on the first <c>PeerIntroduction</c> event that passes provenance,
    /// expiry, and session-id checks; ignores introductions that don't.
    ///
    /// <para>Eagerly dials the introduced peer as this player service via
    /// <see cref="EnsembleNS.RegisteredService.ConnectPeerAsync"/> (so the
    /// peer's gate sees our service address) and surfaces whether the dial was
    /// enqueued in <see cref="PeerEndpoint.Connected"/>; a failed enqueue is
    /// reported, not thrown. The dial itself is fire-and-forget on the daemon.</para>
    /// </summary>
    /// <exception cref="MatchmakingFailedException">The matchmaker sent an
    /// <c>ErrorResponse</c> envelope.</exception>
    /// <exception cref="OperationCanceledException">The caller's
    /// <paramref name="ct"/> fired.</exception>
    public async Task<MatchFound> WaitForMatchAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        if (Interlocked.CompareExchange(ref _matchCompleted, 1, 0) != 0)
            throw new InvalidOperationException(
                "WaitForMatchAsync has already completed on this handle; create a new queue entry.");

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token, ct);
        var reader = _matchSignals.Reader;

        while (await reader.WaitToReadAsync(linked.Token).ConfigureAwait(false))
        {
            while (reader.TryRead(out var msg))
            {
                switch (msg)
                {
                    case MatchSignal.Introduction intro
                        when IsValidIntroduction(intro.Event):
                    {
                        // Single-dialer rule: only the lexicographically-lower
                        // service address dials; the higher one does NOT dial and
                        // instead accepts the peer's inbound dial (via its
                        // admission ruleset) and replies over the remembered
                        // inbound connection (rpc resolveSendPeer's inbound path).
                        // Both peers compute the same split from the same two
                        // addresses, so exactly one dial happens.
                        //
                        // PUG previously relied on a SYMMETRIC mutual dial,
                        // trusting the daemon to tolerate two services dialing
                        // each other at once (ensemble 4b77b7ba). That holds over
                        // loopback but NOT over Tor: the mutual dial never
                        // converges to one stable control channel — it storms
                        // (hundreds of churned Tor control channels), so
                        // rpc.Service.Send never finds a live reusable route and
                        // every send re-dials, leaving the higher-addr peer
                        // unable to reach the lower one (guest input never lands).
                        // The single-dialer rule sidesteps that collision
                        // deterministically; the underlying daemon bug is tracked
                        // separately. Fire-and-forget — failures surface on onError.
                        var peerAddr = intro.Event.PeerAddr;
                        bool weDial = string.CompareOrdinal(_service.ServiceAddress, peerAddr) < 0;
                        bool dialRequested = false;
                        if (weDial)
                        {
                            try
                            {
                                await _service.ConnectPeerAsync(peerAddr, linked.Token).ConfigureAwait(false);
                                dialRequested = true;
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogWarning(ex,
                                    "Service-identity dial to introduced peer {Peer} failed to enqueue", peerAddr);
                            }
                            _logger.LogDebug(
                                "Dialing introduced peer {Peer} as service {Service} (single-dialer: we are lower)",
                                peerAddr, _service.ServiceAddress);
                        }
                        else
                        {
                            _logger.LogDebug(
                                "Not dialing introduced peer {Peer} as service {Service} (single-dialer: peer is lower; accepting inbound)",
                                peerAddr, _service.ServiceAddress);
                        }
                        // Connected reflects whether OUR dial was enqueued; when
                        // we're the acceptor it's false and the link comes up via
                        // the peer's inbound dial + our admission accept.
                        var endpoint = new PeerEndpoint(peerAddr, dialRequested);
                        var roleHint = string.IsNullOrEmpty(intro.Event.RoleHint)
                            ? null
                            : intro.Event.RoleHint;
                        return new MatchFound(SessionId, new[] { endpoint }, roleHint);
                    }

                    case MatchSignal.Introduction badIntro:
                        _logger.LogDebug(
                            "Dropping peer_introduction (from={From} expected={Expected} session={Session} expires={Expires})",
                            badIntro.Event.FromServiceAddr,
                            MatchmakerAddr,
                            badIntro.Event.SessionId,
                            badIntro.Event.ExpiresAt);
                        break;

                    case MatchSignal.MatchmakerError err:
                        throw new MatchmakingFailedException(err.Message, err.Code);
                }
            }
        }

        linked.Token.ThrowIfCancellationRequested();
        throw new MatchmakingFailedException(
            "queue handle event stream closed before a match was found");
    }

    /// <summary>
    /// Stream <see cref="QueueStatus"/> updates the matchmaker sends for this
    /// session. The enumerable terminates cleanly when the handle is disposed
    /// or <paramref name="ct"/> fires.
    ///
    /// <para>Yields the generated proto type <see cref="QueueStatus"/>
    /// directly — game code reading status is reading proto types, which is
    /// fine because game code already lives downstream of this assembly's
    /// proto codegen.</para>
    /// </summary>
    public async IAsyncEnumerable<QueueStatus> StatusStream(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token, ct);
        var reader = _statusUpdates.Reader;
        while (await reader.WaitToReadAsync(linked.Token).ConfigureAwait(false))
        {
            while (reader.TryRead(out var update))
                yield return update;
        }
    }

    /// <summary>
    /// Send game-time bytes to a matched peer. Valid only after
    /// <see cref="WaitForMatchAsync"/> has succeeded; throws
    /// <see cref="InvalidOperationException"/> if no match has formed yet.
    /// Throws <see cref="ObjectDisposedException"/> on a disposed handle.
    ///
    /// <para>
    /// No retry, no ordering guarantee, no framing — a pure pass-through to
    /// the underlying <see cref="EnsembleNS.RegisteredService.SendBytesAsync"/>.
    /// Game code that needs sequencing or reliable delivery adds its own
    /// envelope (sequence numbers + acks).
    /// </para>
    /// </summary>
    /// <param name="peerAddr">The peer's Ensemble service address — typically
    ///   from <see cref="MatchFound.Peers"/>.</param>
    /// <param name="bytes">Caller-serialised payload. PUG.Ensemble does not
    ///   pick a codec; the wire format is whatever both sides agreed to.</param>
    /// <param name="ct">Cancels the send.</param>
    public Task SendToPeerAsync(string peerAddr, ReadOnlyMemory<byte> bytes, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (Volatile.Read(ref _matchCompleted) == 0)
        {
            throw new InvalidOperationException(
                "SendToPeerAsync is only valid after WaitForMatchAsync has completed; " +
                "no match has formed for this handle yet.");
        }
        if (string.IsNullOrWhiteSpace(peerAddr))
            throw new ArgumentException("peerAddr is required", nameof(peerAddr));

        // RegisteredService.SendBytesAsync takes byte[]; if the caller passed a
        // contiguous segment we can avoid a copy via MemoryMarshal, but the
        // simple ToArray() path keeps the API generic for now. ReadOnlyMemory
        // in the signature future-proofs us against a SendBytesAsync overload
        // that takes ReadOnlyMemory directly.
        var payload = bytes.ToArray();
        return _service.SendBytesAsync(peerAddr, payload, ct);
    }

    /// <summary>
    /// Stream inbound game-time messages from matched peers — every
    /// <c>ServiceEvent.RpcMessage</c> whose <c>FromAddr</c> is NOT the
    /// matchmaker this handle queued at. Terminates cleanly when the handle
    /// disposes or <paramref name="ct"/> fires.
    ///
    /// <para>
    /// Unlike <see cref="SendToPeerAsync"/>, enumerating this stream is
    /// allowed before a match has formed — the channel will simply block on
    /// <c>WaitToReadAsync</c> until something arrives. In practice the
    /// matchmaker filters out non-matchmaker traffic, so any item that
    /// surfaces is from a real matched peer.
    /// </para>
    /// </summary>
    public async IAsyncEnumerable<PeerMessage> PeerMessages(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token, ct);
        var reader = _peerMessages.Reader;
        while (true)
        {
            bool more;
            try
            {
                more = await reader.WaitToReadAsync(linked.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_shutdown.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                // Handle disposed while a consumer was enumerating — terminate
                // cleanly rather than propagating the inner cancel.
                yield break;
            }
            if (!more) yield break;
            while (reader.TryRead(out var msg))
                yield return msg;
        }
    }

    /// <summary>
    /// Test seam: synthesise an inbound peer RPC and route it through the
    /// same code path the daemon would. Mirrors
    /// <c>MatchmakerServiceHost.InjectRpcAsync</c>; same rationale —
    /// <c>rpc.Service.Send</c> has no same-daemon local fast path, so a
    /// real-wire test of <see cref="SendToPeerAsync"/> between two services
    /// on one daemon is structurally impossible. The seam lets unit tests
    /// exercise the routing / lifecycle without that infrastructure.
    /// </summary>
    internal Task InjectPeerMessageAsync(string fromAddr, byte[] payload)
    {
        if (string.IsNullOrEmpty(fromAddr))
            throw new ArgumentException("fromAddr required", nameof(fromAddr));
        if (fromAddr == MatchmakerAddr)
            throw new ArgumentException(
                "InjectPeerMessageAsync routes the inbound bytes through PeerMessages, " +
                "which by contract excludes the matchmaker's own address. " +
                "Use a different fromAddr for peer-simulating tests.",
                nameof(fromAddr));

        _peerMessages.Writer.TryWrite(
            new PeerMessage(fromAddr, payload, DateTimeOffset.UtcNow));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Send a <see cref="LeaveQueueRequest"/> to the matchmaker and dispose
    /// the handle. Idempotent: calling twice is safe and the second call is
    /// a no-op. Errors sending the leave message are swallowed because the
    /// dispose path must always complete — a stuck-in-queue session ages out
    /// matchmaker-side on its own TTL.
    /// </summary>
    public async Task LeaveAsync(CancellationToken ct = default)
    {
        if (_disposed != 0)
            return;

        try
        {
            var req = new MatchmakerRequest
            {
                LeaveQueue = new LeaveQueueRequest { SessionId = SessionId },
            };
            await _service.SendBytesAsync(MatchmakerAddr, req.ToByteArray(), ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Caller-cancelled; the handle still goes away.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "LeaveQueue send to {Matchmaker} failed; disposing anyway", MatchmakerAddr);
        }

        await DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Dispose the handle: cancel outstanding waits and tear down the
    /// player-side registered service (clean half-close of the bidi stream
    /// deregisters with the daemon).
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { _shutdown.Cancel(); } catch { /* best effort */ }
        _matchSignals.Writer.TryComplete();
        _statusUpdates.Writer.TryComplete();
        _peerMessages.Writer.TryComplete();
        await _service.DisposeAsync().ConfigureAwait(false);
        _shutdown.Dispose();
    }

    // Provenance / session / expiry validation lives in PeerAdmissionControl
    // so the match-wait path and the inbound-connection ruleset share one
    // definition of a "valid introduction".
    private bool IsValidIntroduction(EnsembleNS.ServiceEvent.PeerIntroduction intro) =>
        _admission.IsValidIntroduction(intro);

    /// <summary>
    /// Internal envelope written by the player-side registered service's
    /// event callback into the <c>matchSignals</c> channel. Status updates
    /// are routed to a separate channel and don't appear here.
    /// </summary>
    internal abstract record MatchSignal
    {
        internal sealed record Introduction(EnsembleNS.ServiceEvent.PeerIntroduction Event) : MatchSignal;
        internal sealed record MatchmakerError(string Code, string Message) : MatchSignal;
    }
}
