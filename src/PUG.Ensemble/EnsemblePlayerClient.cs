using System.Threading.Channels;
using EnsembleNS = Ensemble.Client;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PUG.Ensemble.Proto;

namespace PUG.Ensemble;

/// <summary>
/// Game-side entry point for PUG matchmaking. Wraps the
/// <c>EnsembleClient</c> + matchmaker-RPC interaction so game code can
/// queue, await a match, and dial the introduced peers in a handful of
/// lines.
///
/// <para>
/// <b>Design: the player runs as a registered service.</b> Ensemble has no
/// "client-level send raw bytes to a service" API today —
/// <c>SendBytesAsync</c> is a <see cref="EnsembleNS.RegisteredService"/>
/// method. To round-trip a <see cref="MatchmakerRequest"/> envelope through
/// the daemon under <c>SERVICE_TRANSPORT_RPC</c>, the player itself
/// registers a small per-queue service with the daemon (ACL=Contacts,
/// Transport=Rpc). The service's bidi stream is the player's pipe for
/// receiving <c>RpcMessage</c> replies from the matchmaker AND the daemon's
/// <c>PeerIntroduction</c> events. Each
/// <see cref="JoinMatchmakingAsync{TPayload}"/> call spins up its own
/// service (unique name) so multiple outstanding queues per player are
/// naturally isolated — separate names, separate event streams, separate
/// dispose semantics.
/// </para>
///
/// <para>
/// <b>Payload serialisation.</b> The matchmaker bucket consumes whatever
/// bytes the player produces; the SDK refuses to pick a default codec.
/// Callers MUST pass an explicit <c>serializePayload</c> delegate — JSON,
/// MessagePack, raw proto, whatever pairs with the matchmaker host's
/// symmetric deserializer.
/// </para>
///
/// <para>
/// Dispose this object once the game shuts down; outstanding
/// <see cref="QueueHandle{TPayload}"/>s own their own service and clean up
/// independently when disposed.
/// </para>
/// </summary>
public sealed class EnsemblePlayerClient : IAsyncDisposable
{
    private readonly EnsembleNS.EnsembleClient _ensemble;
    private readonly ILogger<EnsemblePlayerClient> _logger;
    private int _disposed;

    /// <summary>
    /// Wraps an existing <see cref="EnsembleNS.EnsembleClient"/>. Lifetime
    /// of the underlying client is NOT taken over — the caller owns its
    /// disposal. The wrapper holds onto it for the duration of its own
    /// lifetime so queue handles can dial peers.
    /// </summary>
    public EnsemblePlayerClient(
        EnsembleNS.EnsembleClient ensemble,
        ILogger<EnsemblePlayerClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(ensemble);
        _ensemble = ensemble;
        _logger = logger ?? NullLogger<EnsemblePlayerClient>.Instance;
    }

    /// <summary>
    /// Queue this player at <paramref name="matchmakerAddr"/>. On the wire:
    /// (1) registers a per-queue player-side service with the daemon,
    /// (2) opens a peer connection to the matchmaker, (3) sends a
    /// <see cref="JoinQueueRequest"/> with the caller-serialised
    /// <paramref name="payload"/>, (4) awaits the matchmaker's first reply
    /// (expected to be <c>QueuedResponse</c>), and (5) returns a handle
    /// that watches for the matching <c>PeerIntroduction</c>.
    /// </summary>
    /// <param name="matchmakerAddr">E-address of the matchmaker service.</param>
    /// <param name="payload">Host-defined per-player payload. Serialised via
    ///   <paramref name="serializePayload"/>.</param>
    /// <param name="serializePayload">Caller-supplied serializer. Required
    ///   when <typeparamref name="TPayload"/> is non-null; <c>null</c>
    ///   yields a <see cref="NotSupportedException"/>.</param>
    /// <param name="privateGameId">Optional private-match correlation id; the
    ///   matchmaker treats <see cref="Guid.Empty"/> / null as "public queue".</param>
    /// <param name="ct">Cancels the handshake. After the handle is returned,
    ///   cancellation is decoupled — dispose the handle to abort the wait.</param>
    public async Task<QueueHandle<TPayload>> JoinMatchmakingAsync<TPayload>(
        string matchmakerAddr,
        TPayload payload,
        Func<TPayload, byte[]> serializePayload,
        Guid? privateGameId = null,
        CancellationToken ct = default)
    {
        var req = new MatchmakerRequest
        {
            JoinQueue = new JoinQueueRequest
            {
                Payload = ByteString.CopyFrom(SerializeOrThrow(payload, serializePayload)),
                PrivateGameId = privateGameId is { } gid && gid != Guid.Empty
                    ? gid.ToString()
                    : string.Empty,
            },
        };

        var (handle, first) = await SendAndAwaitFirstReplyAsync<TPayload>(
            matchmakerAddr, req, expect: FirstReplyKind.Queued, ct).ConfigureAwait(false);
        _ = first; // QueuedResponse already consumed by the helper to build the handle
        return handle;
    }

    /// <summary>
    /// Create a new private match. The matchmaker generates a fresh short
    /// code + private-game-id, enqueues this player, and replies with a
    /// <see cref="PrivateMatchCreated"/> envelope. Returns the code (which
    /// the creator shares with the second player out-of-band) and a queue
    /// handle that waits for the second player's join + introduction.
    ///
    /// <para>
    /// Symmetry with <see cref="JoinMatchmakingAsync{TPayload}"/>: same
    /// per-handle service registration, same pre-dial, same per-call
    /// service teardown on failure. The only difference is the request
    /// envelope variant and the expected first-reply oneof case.
    /// </para>
    /// </summary>
    public async Task<(string Code, QueueHandle<TPayload> Handle)> CreatePrivateMatchAsync<TPayload>(
        string matchmakerAddr,
        TPayload payload,
        Func<TPayload, byte[]> serializePayload,
        CancellationToken ct = default)
    {
        var req = new MatchmakerRequest
        {
            CreatePrivateMatch = new CreatePrivateMatchRequest
            {
                Payload = ByteString.CopyFrom(SerializeOrThrow(payload, serializePayload)),
            },
        };

        var (handle, first) = await SendAndAwaitFirstReplyAsync<TPayload>(
            matchmakerAddr, req, expect: FirstReplyKind.PrivateMatchCreated, ct).ConfigureAwait(false);
        return (first.PrivateMatchCreated!.Code, handle);
    }

    /// <summary>
    /// Join an existing private match by its short code. The matchmaker
    /// resolves the code to a private-game-id and enqueues this player
    /// against the creator's ticket. On an unknown / expired code the
    /// matchmaker replies with an <see cref="ErrorResponse"/> whose
    /// <c>Code</c> is <c>"unknown_code"</c> or <c>"expired_code"</c>; this
    /// surfaces as a <see cref="MatchmakingFailedException"/>.
    /// </summary>
    public async Task<QueueHandle<TPayload>> JoinPrivateByCodeAsync<TPayload>(
        string matchmakerAddr,
        TPayload payload,
        string code,
        Func<TPayload, byte[]> serializePayload,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(code))
            throw new ArgumentException("code is required", nameof(code));

        var req = new MatchmakerRequest
        {
            JoinPrivateByCode = new JoinPrivateByCodeRequest
            {
                Payload = ByteString.CopyFrom(SerializeOrThrow(payload, serializePayload)),
                Code = code,
            },
        };

        var (handle, _) = await SendAndAwaitFirstReplyAsync<TPayload>(
            matchmakerAddr, req, expect: FirstReplyKind.Queued, ct).ConfigureAwait(false);
        return handle;
    }

    private static byte[] SerializeOrThrow<TPayload>(TPayload payload, Func<TPayload, byte[]> serializePayload)
    {
        if (serializePayload is null)
            throw new NotSupportedException(
                "Matchmaking calls require an explicit serializePayload delegate; the SDK does not pick a codec.");

        var bytes = serializePayload(payload);
        if (bytes is null)
            throw new InvalidOperationException("serializePayload returned null");
        return bytes;
    }

    /// <summary>
    /// Shared dispatch for the three player→matchmaker request variants:
    /// register the per-handle service, optionally pre-dial, send the
    /// request envelope, and await the first matchmaker reply. The
    /// <paramref name="expect"/> parameter pins which oneof case is the
    /// "queued"-equivalent first reply that promotes to a
    /// <see cref="QueueHandle{TPayload}"/>; any other oneof case received
    /// during the handshake window throws <see cref="MatchmakingFailedException"/>.
    /// </summary>
    private async Task<(QueueHandle<TPayload> Handle, FirstReply First)>
        SendAndAwaitFirstReplyAsync<TPayload>(
            string matchmakerAddr,
            MatchmakerRequest req,
            FirstReplyKind expect,
            CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        if (string.IsNullOrWhiteSpace(matchmakerAddr))
            throw new ArgumentException("matchmakerAddr is required", nameof(matchmakerAddr));

        // Per-handle service. Unique name keeps multiple outstanding queues
        // for the same player from colliding on the daemon's registry.
        // Service name validation is lowercase-alphanumeric+dash+underscore
        // (see ensemble's registry); Guid.ToString("N") is hex-lowercase.
        var serviceName = $"pug-player-{Guid.NewGuid():N}";
        // DecidesConnections opts this service into the Ensemble "service
        // decides" gate (ADR df82c69a): the daemon routes every unknown-peer
        // CONN_REQUEST to our stream as a connection_request event, and PUG's
        // own admission ruleset (PeerAdmissionControl) accepts only peers the
        // verified matchmaker introduced us to. This replaces the old
        // ServiceAcl.Contacts dead-end, where introduced player↔player dials
        // were rejected "no per-service contacts configured".
        var manifest = EnsembleNS.ServiceManifest.NewBuilder(serviceName)
            .Description("PUG player-side queue session")
            .DecidesConnections()
            .Transport(EnsembleNS.ServiceTransport.Rpc)
            .Build();

        // PUG's admission ruleset for this queue session. Populated from valid
        // peer introductions; consulted when the daemon asks us to decide an
        // inbound connection. SessionId is bound once the QueuedResponse lands.
        var admission = new PeerAdmissionControl(matchmakerAddr, _logger);

        // Three channels per handle — see QueueHandle for the rationale.
        // peerMessages absorbs any RpcMessage whose FromAddr is NOT the
        // matchmaker; surfaced to game code via QueueHandle.PeerMessages.
        var matchSignals = Channel.CreateUnbounded<QueueHandle<TPayload>.MatchSignal>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        var statusUpdates = Channel.CreateUnbounded<QueueStatus>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        var peerMessages = Channel.CreateUnbounded<PeerMessage>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        // FirstReply TCS holds whichever of {Queued, PrivateMatchCreated,
        // Error} arrives first; the caller picked which kind it expects so
        // mismatched-kind replies are surfaced as exceptions. Wiring the
        // callback BEFORE SendBytesAsync avoids racing the matchmaker's
        // reply.
        var firstTcs = new TaskCompletionSource<FirstReply>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var startupErrorTcs = new TaskCompletionSource<EnsembleNS.ServiceError>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        // Captured so the connection-request handler below can accept/reject on
        // the same registered service. Assigned right after registration; any
        // connection_request can only arrive after a post-match peer dial, long
        // after this assignment runs.
        EnsembleNS.RegisteredService? serviceRef = null;

        EnsembleNS.RegisteredService service = await _ensemble.RegisterServiceAsync(
            manifest,
            onEvent: async ev =>
            {
                switch (ev)
                {
                    case EnsembleNS.ServiceEvent.RpcMessage rpc when rpc.FromAddr == matchmakerAddr:
                        HandleMatchmakerRpc(rpc.Payload, firstTcs, matchSignals, statusUpdates);
                        break;
                    case EnsembleNS.ServiceEvent.RpcMessage rpc:
                        // Post-introduction game-time traffic from a peer.
                        // Routed unconditionally — game code reads it via
                        // QueueHandle.PeerMessages and applies its own
                        // sender-filtering as needed.
                        peerMessages.Writer.TryWrite(
                            new PeerMessage(rpc.FromAddr, rpc.Payload, DateTimeOffset.UtcNow));
                        break;
                    case EnsembleNS.ServiceEvent.PeerIntroduction intro:
                        // Authorize the introduced peer for inbound connections
                        // BEFORE surfacing the match — the peer may dial us the
                        // moment it receives its own introduction.
                        var recorded = admission.RecordIntroduction(intro);
                        _logger.LogDebug(
                            "PeerIntroduction peer={Peer} session={Session} recorded={Recorded}",
                            intro.PeerAddr, intro.SessionId, recorded);
                        matchSignals.Writer.TryWrite(
                            new QueueHandle<TPayload>.MatchSignal.Introduction(intro));
                        break;
                    case EnsembleNS.ServiceEvent.ConnectionRequest cr:
                        _logger.LogDebug("ConnectionRequest id={Id} from={From}", cr.RequestId, cr.FromAddr);
                        // Deliberately NOT the handshake ct: connection requests
                        // arrive at gameplay time, long after the JoinMatchmaking
                        // call (and its token) has gone. Riding that token would
                        // throw the moment the caller cancels it (e.g. the lobby
                        // scene exiting), leaving the daemon to time out and
                        // fail-closed. The response write is bounded by the
                        // daemon's own connection-decision backstop.
                        await DecideConnectionAsync(serviceRef, admission, cr, _logger)
                            .ConfigureAwait(false);
                        break;
                }
            },
            onError: err =>
            {
                if (!firstTcs.Task.IsCompleted)
                    startupErrorTcs.TrySetResult(err);
                else
                    _logger.LogWarning("Player service error code={Code} message={Message}", err.Code, err.Message);
                return ValueTask.CompletedTask;
            },
            // CancellationToken.None — NOT the caller's handshake ct. The SDK
            // ties the registered service's whole bidi stream + event reader
            // loop to this token, but the player service must live for the
            // entire match (it carries matchmaker replies, peer introductions,
            // connection-request decisions, and game-time RPC). Its lifetime is
            // owned by the returned QueueHandle, which disposes the service on
            // teardown. Binding it to the handshake ct would kill the service
            // the moment the caller cancels that token (e.g. a lobby scene
            // exiting on match-found), silently breaking the peer connection.
            // The handshake stays cancellable: AwaitFirstAsync observes ct and
            // the catch below disposes the service if the caller bails early.
            ct: CancellationToken.None).ConfigureAwait(false);
        serviceRef = service;

        try
        {
            // No client-side pre-dial. The matchmaker uses strict envelope
            // attribution (Ensemble VerifyAndAttribute), so our service-signed
            // JOIN is only accepted once the matchmaker has bound THIS service's
            // key via a service-identity handshake. A node-identity pre-dial
            // (the old EnsembleClient.ConnectAsync) bound the wrong key and the
            // JOIN was silently dropped. The daemon now establishes the
            // service-identity connection on demand when this SendBytes finds no
            // route, so the first send "just works" with the correct binding.
            await service.SendBytesAsync(matchmakerAddr, req.ToByteArray(), ct).ConfigureAwait(false);

            var first = await AwaitFirstAsync(firstTcs.Task, startupErrorTcs.Task, ct).ConfigureAwait(false);

            // Validate the first-reply kind matches the caller's expectation.
            string sessionId;
            switch (expect)
            {
                case FirstReplyKind.Queued:
                    if (first.Queued is null)
                        throw new MatchmakingFailedException(
                            $"matchmaker replied with {first.Kind} but the request expected Queued");
                    sessionId = first.Queued.SessionId;
                    break;
                case FirstReplyKind.PrivateMatchCreated:
                    if (first.PrivateMatchCreated is null)
                        throw new MatchmakingFailedException(
                            $"matchmaker replied with {first.Kind} but the request expected PrivateMatchCreated");
                    sessionId = first.PrivateMatchCreated.SessionId;
                    break;
                default:
                    throw new InvalidOperationException($"unknown FirstReplyKind {expect}");
            }

            // Bind the admission set to this session now that the matchmaker has
            // issued the id; introductions/connection requests arrive later.
            admission.SessionId = sessionId;

            var handle = new QueueHandle<TPayload>(
                _ensemble,
                service,
                matchSignals,
                statusUpdates,
                peerMessages,
                matchmakerAddr,
                sessionId,
                admission,
                _logger);
            return (handle, first);
        }
        catch
        {
            // Anything bad past the registration: tear the service down so we
            // don't leak a daemon-side registration.
            await service.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Dispose the wrapper. Does NOT dispose the wrapped
    /// <see cref="EnsembleNS.EnsembleClient"/> — the caller owns its
    /// lifetime. Outstanding <see cref="QueueHandle{TPayload}"/>s are also
    /// not disposed here; each owns its own service handle.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Test-only seam: register the per-handle player-side service and
    /// return both the handle (pre-queued with the caller-supplied
    /// <paramref name="sessionId"/>) and a delegate for re-driving the
    /// matchmaker-RPC ingress path from a unit test. Bypasses the
    /// <see cref="EnsembleNS.RegisteredService.SendBytesAsync"/> round-trip
    /// to the matchmaker that production
    /// <see cref="JoinMatchmakingAsync{TPayload}"/> performs — that round-
    /// trip requires a cross-daemon peer link the daemon's
    /// <c>rpc.Service.Send</c> can resolve, which a single-daemon test
    /// topology can't provide.
    ///
    /// <para>
    /// On the daemon's local-fast-path, <c>IntroducePeer</c> events DO
    /// still flow between same-daemon services — so the returned handle
    /// can exercise the provenance / expiry / session-id filtering against
    /// a stub matchmaker registered on the same daemon.
    /// </para>
    /// </summary>
    internal async Task<QueueHandle<TPayload>> CreatePreQueuedHandleAsync<TPayload>(
        string matchmakerAddr,
        string sessionId,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);

        var serviceName = $"pug-player-{Guid.NewGuid():N}";
        var manifest = EnsembleNS.ServiceManifest.NewBuilder(serviceName)
            .Description("PUG player-side queue session (test seam)")
            .Acl(EnsembleNS.ServiceAcl.Public)
            .Transport(EnsembleNS.ServiceTransport.Rpc)
            .Build();

        var admission = new PeerAdmissionControl(matchmakerAddr, _logger) { SessionId = sessionId };

        var matchSignals = Channel.CreateUnbounded<QueueHandle<TPayload>.MatchSignal>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        var statusUpdates = Channel.CreateUnbounded<QueueStatus>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        var peerMessages = Channel.CreateUnbounded<PeerMessage>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        // Identical event-routing logic to the production dispatch helper;
        // the only difference is we never wait for / consume a first reply.
        var dummyFirstTcs = new TaskCompletionSource<FirstReply>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        // Mark the first-reply TCS as already-completed so HandleMatchmakerRpc
        // treats subsequent envelopes as post-queue updates rather than
        // handshake-window results.
        dummyFirstTcs.SetResult(new FirstReply(
            FirstReplyKind.Queued, new QueuedResponse { SessionId = sessionId }, null));

        var service = await _ensemble.RegisterServiceAsync(
            manifest,
            onEvent: ev =>
            {
                switch (ev)
                {
                    case EnsembleNS.ServiceEvent.RpcMessage rpc when rpc.FromAddr == matchmakerAddr:
                        HandleMatchmakerRpc(rpc.Payload, dummyFirstTcs, matchSignals, statusUpdates);
                        break;
                    case EnsembleNS.ServiceEvent.RpcMessage rpc:
                        peerMessages.Writer.TryWrite(
                            new PeerMessage(rpc.FromAddr, rpc.Payload, DateTimeOffset.UtcNow));
                        break;
                    case EnsembleNS.ServiceEvent.PeerIntroduction intro:
                        admission.RecordIntroduction(intro);
                        matchSignals.Writer.TryWrite(
                            new QueueHandle<TPayload>.MatchSignal.Introduction(intro));
                        break;
                }
                return ValueTask.CompletedTask;
            },
            onError: null,
            ct: ct).ConfigureAwait(false);

        return new QueueHandle<TPayload>(
            _ensemble, service, matchSignals, statusUpdates, peerMessages, matchmakerAddr, sessionId, admission, _logger);
    }

    /// <summary>
    /// Decide an inbound connection request per PUG's admission ruleset: accept
    /// iff the requester's service address belongs to a peer the verified
    /// matchmaker introduced us to within a valid session horizon; otherwise
    /// reject. This is the consumer half of the Ensemble "service decides" gate
    /// (ADR df82c69a) — an introduction is information, never a grant.
    /// </summary>
    private static async Task DecideConnectionAsync(
        EnsembleNS.RegisteredService? service,
        PeerAdmissionControl admission,
        EnsembleNS.ServiceEvent.ConnectionRequest request,
        ILogger logger)
    {
        if (service is null)
        {
            // Registration hasn't finished publishing the handle yet. We can't
            // respond on a null service; the daemon's gate backstop fails
            // closed, which is the safe outcome.
            logger.LogWarning(
                "ConnectionRequest {RequestId} from {From} arrived before the service was ready; leaving it to the daemon backstop",
                request.RequestId, request.FromAddr);
            return;
        }

        try
        {
            if (admission.IsAuthorized(request.FromAddr))
            {
                await service.AcceptConnectionAsync(request.RequestId).ConfigureAwait(false);
                logger.LogDebug(
                    "Accepted introduced peer connection {RequestId} from {From}",
                    request.RequestId, request.FromAddr);
            }
            else
            {
                await service.RejectConnectionAsync(
                    request.RequestId, "not an introduced peer for this match").ConfigureAwait(false);
                logger.LogDebug(
                    "Rejected connection {RequestId} from non-introduced address {From}",
                    request.RequestId, request.FromAddr);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "Failed to respond to connection request {RequestId} from {From}",
                request.RequestId, request.FromAddr);
        }
    }

    private static void HandleMatchmakerRpc<TPayload>(
        byte[] payload,
        TaskCompletionSource<FirstReply> firstTcs,
        Channel<QueueHandle<TPayload>.MatchSignal> matchSignals,
        Channel<QueueStatus> statusUpdates)
    {
        // Tolerant decode: an unparsable envelope from a service the player
        // chose to RPC is a protocol violation we surface as a startup error
        // when we haven't yet been queued, or log-and-drop afterwards.
        MatchmakerResponse resp;
        try
        {
            resp = MatchmakerResponse.Parser.ParseFrom(payload);
        }
        catch (InvalidProtocolBufferException ex)
        {
            if (!firstTcs.Task.IsCompleted)
                firstTcs.TrySetException(new MatchmakingFailedException(
                    "matchmaker sent an unparsable MatchmakerResponse envelope", ex));
            return;
        }

        switch (resp.MsgCase)
        {
            case MatchmakerResponse.MsgOneofCase.Queued:
                firstTcs.TrySetResult(new FirstReply(FirstReplyKind.Queued, resp.Queued, null));
                break;
            case MatchmakerResponse.MsgOneofCase.PrivateMatchCreated:
                firstTcs.TrySetResult(new FirstReply(
                    FirstReplyKind.PrivateMatchCreated, null, resp.PrivateMatchCreated));
                break;
            case MatchmakerResponse.MsgOneofCase.Status:
                statusUpdates.Writer.TryWrite(resp.Status);
                break;
            case MatchmakerResponse.MsgOneofCase.Error:
                var err = new QueueHandle<TPayload>.MatchSignal.MatchmakerError(
                    resp.Error.Code, resp.Error.Message);
                if (!firstTcs.Task.IsCompleted)
                    firstTcs.TrySetException(new MatchmakingFailedException(
                        resp.Error.Message, resp.Error.Code));
                matchSignals.Writer.TryWrite(err);
                break;
        }
    }

    private static async Task<FirstReply> AwaitFirstAsync(
        Task<FirstReply> firstTask,
        Task<EnsembleNS.ServiceError> errorTask,
        CancellationToken ct)
    {
        using var cancelTcs = new CancellationTokenTaskSource<bool>(ct);
        var winner = await Task.WhenAny(firstTask, errorTask, cancelTcs.Task).ConfigureAwait(false);
        if (winner == cancelTcs.Task)
        {
            ct.ThrowIfCancellationRequested();
        }
        if (winner == errorTask)
        {
            var err = await errorTask.ConfigureAwait(false);
            throw new MatchmakingFailedException(
                $"player-side service error during queue handshake: {err.Message}", err.Code);
        }
        return await firstTask.ConfigureAwait(false);
    }

    /// <summary>
    /// Internal first-reply envelope. Exactly one of <see cref="Queued"/> /
    /// <see cref="PrivateMatchCreated"/> is non-null per instance.
    /// </summary>
    private sealed record FirstReply(
        FirstReplyKind Kind,
        QueuedResponse? Queued,
        PrivateMatchCreated? PrivateMatchCreated);

    private enum FirstReplyKind
    {
        Queued,
        PrivateMatchCreated,
    }

    /// <summary>
    /// Small helper to project a <see cref="CancellationToken"/> into a
    /// task that completes when the token fires. Cleans up the
    /// registration on dispose to avoid the well-known
    /// CancellationToken.Register leak when the token outlives the wait.
    /// </summary>
    private sealed class CancellationTokenTaskSource<T> : IDisposable
    {
        private readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenRegistration _reg;

        public CancellationTokenTaskSource(CancellationToken ct)
        {
            _reg = ct.Register(s => ((TaskCompletionSource<T>)s!).TrySetCanceled(), _tcs);
        }

        public Task<T> Task => _tcs.Task;

        public void Dispose() => _reg.Dispose();
    }
}
