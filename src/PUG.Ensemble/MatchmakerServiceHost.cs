using System.Collections.Concurrent;
using Ensemble.Client;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PUG.Core;
using PUG.Ensemble.Proto;

namespace PUG.Ensemble;

/// <summary>
/// Hosts a PUG matchmaker as an Ensemble service. Registers a public
/// <c>SERVICE_TRANSPORT_RPC</c> service on a supplied
/// <see cref="EnsembleClient"/>, dispatches inbound
/// <see cref="MatchmakerRequest"/> envelopes against a
/// <see cref="IQueue{TTicket}"/> / <see cref="IMatcher{TTicket}"/> pair,
/// and introduces matched peers via the daemon's
/// <c>IntroducePeersAsync</c> primitive.
/// </summary>
/// <remarks>
/// <para>
/// <b>Lifecycle.</b> Construct, then call <see cref="StartAsync"/> to
/// register the service and kick off the match loop. Disposal stops the
/// loop and half-closes the daemon stream so the service deregisters
/// cleanly.
/// </para>
/// <para>
/// <b>RPC dispatch.</b> The <c>onEvent</c> callback handed to
/// <see cref="EnsembleClient.RegisterServiceAsync"/> runs on the daemon
/// reader loop's task; the host serialises mutations to its session map
/// via <see cref="ConcurrentDictionary{TKey, TValue}"/> and writes
/// responses via <see cref="RegisteredService.SendBytesAsync"/> (whose
/// own write side is internally semaphore-serialised so concurrent
/// replies are safe).
/// </para>
/// <para>
/// <b>Session map.</b> The proto LeaveQueue request only carries
/// <c>session_id</c>; the queue exposes only <c>RemoveAsync(playerId)</c>.
/// The host keeps a <c>session_id → (player_id, peer_addr)</c> map so it
/// can resolve a leave to a removal and so it can look up each matched
/// ticket's peer address at introduction time.
/// </para>
/// <para>
/// <b>Match cadence.</b> A background loop polls the matcher every
/// <see cref="MatchmakerOptions{TPayload}.EffectiveMatchTickInterval"/>.
/// On a non-null <see cref="MatchResult{TTicket}"/> the host generates a
/// fresh session id <em>per match</em> (so every recipient in the same
/// match sees the same <c>session_id</c>, distinct from any previously
/// issued queued-response session id), introduces each pair according
/// to <see cref="MatchmakerOptions{TPayload}.IntroduceTeammatesOnly"/>,
/// and prunes both queue and session map.
/// </para>
/// </remarks>
public sealed class MatchmakerServiceHost<TPayload> : IAsyncDisposable
{
    private readonly EnsembleClient _ensemble;
    private readonly IMatcher<Ticket<TPayload>> _matcher;
    private readonly IQueue<Ticket<TPayload>> _queue;
    private readonly MatchmakerOptions<TPayload> _options;
    private readonly IPayloadVerifier<TPayload>? _verifier;
    private readonly IPrivateLobby _privateLobby;
    private readonly ILogger<MatchmakerServiceHost<TPayload>> _logger;

    // session_id -> (player_id, peer_addr, private_game_id?, code_if_creator?).
    // Keyed by session_id so a LeaveQueueRequest (which only carries session_id)
    // can resolve the owning player. Also indexed by player_id-reverse for the
    // match loop's "introduce this ticket -> what peer addr" lookup.
    //
    // Code-lifecycle bookkeeping uses Option A from the ticket: the creator's
    // entry carries CodeIfCreator (set on CreatePrivateMatch); joiners carry
    // PrivateGameId only. On match completion we release the creator's code
    // (canonical creator-of-code) and ignore joiners. On leave-without-match
    // by the creator we also release the code.
    private readonly ConcurrentDictionary<string, SessionEntry> _sessions = new();
    private readonly ConcurrentDictionary<Guid, string> _playerToSession = new();

    // code -> created_at, for host-side TTL pruning. Cleared on match
    // completion, leave-by-creator, or TTL eviction inside TickAsync.
    private readonly ConcurrentDictionary<string, DateTimeOffset> _codeCreatedAt =
        new(StringComparer.Ordinal);

    private readonly CancellationTokenSource _shutdownCts = new();
    private RegisteredService? _service;
    private Task? _matchLoop;
    private int _disposed;

    /// <summary>The registered service's E-address. Throws before <see cref="StartAsync"/>.</summary>
    public string ServiceAddress => _service?.ServiceAddress
        ?? throw new InvalidOperationException(
            "MatchmakerServiceHost is not started — call StartAsync first.");

    /// <summary>The registered service's onion endpoint. Throws before <see cref="StartAsync"/>.</summary>
    public string Onion => _service?.Onion
        ?? throw new InvalidOperationException(
            "MatchmakerServiceHost is not started — call StartAsync first.");

    /// <summary>
    /// Construct a host. Call <see cref="StartAsync"/> to register and run.
    /// </summary>
    /// <param name="ensemble">A live Ensemble client. Lifetime owned by the caller.</param>
    /// <param name="matcher">The PUG matcher; called from the background match loop.</param>
    /// <param name="queue">Backing queue. The host enqueues on
    ///   <c>JoinQueueRequest</c>, removes by player-id on
    ///   <c>LeaveQueueRequest</c> and after a successful match.</param>
    /// <param name="options">Manifest + behaviour options.</param>
    /// <param name="logger">Optional logger; defaults to a no-op logger.</param>
    /// <param name="verifier">Optional payload verifier — when set, the host
    ///   calls <see cref="IPayloadVerifier{TPayload}.VerifyAsync"/> on every
    ///   inbound <c>JoinQueueRequest</c> before enqueueing and replies with
    ///   <c>ErrorResponse{ Code = "rejected" }</c> on failure.</param>
    public MatchmakerServiceHost(
        EnsembleClient ensemble,
        IMatcher<Ticket<TPayload>> matcher,
        IQueue<Ticket<TPayload>> queue,
        MatchmakerOptions<TPayload> options,
        ILogger<MatchmakerServiceHost<TPayload>>? logger = null,
        IPayloadVerifier<TPayload>? verifier = null)
    {
        ArgumentNullException.ThrowIfNull(ensemble);
        ArgumentNullException.ThrowIfNull(matcher);
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.ServiceName))
            throw new ArgumentException("ServiceName is required", nameof(options));
        if (options.TeamSizes is null || options.TeamSizes.Count == 0)
            throw new ArgumentException("TeamSizes is required", nameof(options));

        _ensemble = ensemble;
        _matcher = matcher;
        _queue = queue;
        _options = options;
        _verifier = verifier;
        _privateLobby = options.PrivateLobby ?? new InMemoryPrivateLobby();
        _logger = logger ?? NullLogger<MatchmakerServiceHost<TPayload>>.Instance;
    }

    /// <summary>
    /// Register the service with the daemon and start the match loop. Idempotent
    /// is NOT supported — calling twice throws <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="ct">Cancellation token for the initial registration handshake
    ///   only. Subsequent cancellation flows through <see cref="DisposeAsync"/>.</param>
    public async Task StartAsync(CancellationToken ct = default)
    {
        if (_service is not null)
            throw new InvalidOperationException("MatchmakerServiceHost is already started.");

        var manifest = BuildManifest();

        _service = await _ensemble.RegisterServiceAsync(
            manifest,
            OnEventAsync,
            OnErrorAsync,
            ct).ConfigureAwait(false);

        _matchLoop = Task.Run(() => MatchLoopAsync(_shutdownCts.Token));

        _logger.LogInformation(
            "MatchmakerServiceHost started: service={Service} addr={Addr} onion={Onion}",
            _options.ServiceName, _service.ServiceAddress, _service.Onion);
    }

    /// <summary>
    /// Test seam: inject a synthetic <c>RpcMessage</c> as if it arrived from
    /// the daemon. Used by integration tests on a single-daemon topology
    /// where the real <c>SendBytes</c> path requires a libp2p peer
    /// connection between two distinct daemons (per Ensemble's
    /// <c>internal/rpc/service.go::Send</c>, which calls
    /// <c>resolver.GetPeer</c> and fails on same-daemon targets). Tests use
    /// this to deliver the player's <c>JoinQueueRequest</c> bytes to the
    /// host without exercising the cross-daemon wire path; introductions
    /// still flow through the real daemon (which has a local fast path).
    /// </summary>
    internal Task InjectRpcAsync(string fromAddr, byte[] payload) =>
        HandleRpcAsync(new ServiceEvent.RpcMessage(fromAddr, payload, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));

    /// <summary>
    /// Test seam: send a <see cref="MatchmakerResponse"/> back to a player.
    /// Production callers don't need this — the host replies via its
    /// registered service handle. Exposed only for tests that simulate
    /// inbound RPCs via <see cref="InjectRpcAsync"/> and want to observe
    /// replies through a side channel.
    /// </summary>
    internal Func<string, MatchmakerResponse, Task>? TestResponseSink { get; set; }

    /// <summary>
    /// Build the <see cref="ServiceManifest"/> the host would register. Exposed
    /// internally for unit-test inspection so tests can verify ACL / transport /
    /// rate-limit / payload-cap shape without paying for a real daemon round-trip.
    /// </summary>
    // Metric names declared in the manifest and pushed from the match loop
    // (ensemble ADR-0005). The daemon stamps the `service` label; the
    // dashboard renders these generically from kind+unit — no ensemble-side
    // code names them. Inert unless the daemon runs with --metrics.
    internal const string MetricQueueDepth = "queue_depth";
    internal const string MetricActiveLobbies = "active_lobbies";
    internal const string MetricMatchesFormed = "matches_formed_total";
    internal const string MetricTimeToMatch = "time_to_match_seconds";

    internal ServiceManifest BuildManifest() =>
        ServiceManifest.NewBuilder(_options.ServiceName)
            .Acl(ServiceAcl.Public)
            .Transport(ServiceTransport.Rpc)
            .MaxPayloadBytes(_options.MaxPayloadBytes)
            .RateLimit(_options.RateLimitPerMinute, _options.RateLimitBurst)
            .Metrics(
                new MetricSpec(MetricQueueDepth, "gauge", Unit: "count",
                    Help: "Players currently queued for a match"),
                new MetricSpec(MetricActiveLobbies, "gauge", Unit: "count",
                    Help: "Private-match codes outstanding"),
                new MetricSpec(MetricMatchesFormed, "counter", Unit: "count",
                    Help: "Matches formed since the host started"),
                new MetricSpec(MetricTimeToMatch, "histogram", Unit: "seconds",
                    Help: "Queue-join to match-formed wait per player",
                    Buckets: new[] { 1.0, 5, 15, 30, 60, 120, 300 }))
            .Build();

    /// <summary>
    /// Fire-and-forget metric push. Failures are logged at debug and never
    /// disturb the match loop — telemetry must not be able to break
    /// matchmaking, and a daemon without <c>--metrics</c> just drops samples.
    /// </summary>
    private async Task PushMetricSafeAsync(
        string name, double value, CancellationToken ct)
    {
        var svc = _service;
        if (svc is null) return;
        try
        {
            await svc.PushMetricAsync(name, value, ct: ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "metric push failed for {Metric}", name);
        }
    }

    private async ValueTask OnEventAsync(ServiceEvent ev)
    {
        try
        {
            switch (ev)
            {
                case ServiceEvent.RpcMessage rpc:
                    await HandleRpcAsync(rpc).ConfigureAwait(false);
                    break;
                case ServiceEvent.PeerIntroduction:
                    // Matchmakers don't expect inbound introductions; ignore.
                    break;
                default:
                    _logger.LogDebug("ignoring service event {Type}", ev.GetType().Name);
                    break;
            }
        }
        catch (Exception ex)
        {
            // Reader-loop callback must never throw — daemons treat that as a
            // stream fault. Swallow and log; the host stays alive.
            _logger.LogError(ex, "matchmaker dispatch failed");
        }
    }

    private ValueTask OnErrorAsync(ServiceError err)
    {
        _logger.LogWarning(
            "matchmaker daemon error: code={Code} message={Message} limit={Limit} retryAfter={Retry}",
            err.Code, err.Message, err.LimitBytes, err.RetryAfterMs);
        return ValueTask.CompletedTask;
    }

    private async Task HandleRpcAsync(ServiceEvent.RpcMessage rpc)
    {
        MatchmakerRequest req;
        try
        {
            req = MatchmakerRequest.Parser.ParseFrom(rpc.Payload);
        }
        catch (InvalidProtocolBufferException ex)
        {
            _logger.LogWarning(ex, "discarding malformed MatchmakerRequest from {Addr}", rpc.FromAddr);
            await SendErrorAsync(rpc.FromAddr, "malformed_request", "could not parse MatchmakerRequest").ConfigureAwait(false);
            return;
        }

        switch (req.MsgCase)
        {
            case MatchmakerRequest.MsgOneofCase.JoinQueue:
                await HandleJoinQueueAsync(rpc.FromAddr, req.JoinQueue).ConfigureAwait(false);
                break;
            case MatchmakerRequest.MsgOneofCase.LeaveQueue:
                await HandleLeaveQueueAsync(rpc.FromAddr, req.LeaveQueue).ConfigureAwait(false);
                break;
            case MatchmakerRequest.MsgOneofCase.CreatePrivateMatch:
                await HandleCreatePrivateMatchAsync(rpc.FromAddr, req.CreatePrivateMatch).ConfigureAwait(false);
                break;
            case MatchmakerRequest.MsgOneofCase.JoinPrivateByCode:
                await HandleJoinPrivateByCodeAsync(rpc.FromAddr, req.JoinPrivateByCode).ConfigureAwait(false);
                break;
            default:
                await SendErrorAsync(rpc.FromAddr, "unknown_request",
                    $"unknown oneof case {req.MsgCase}").ConfigureAwait(false);
                break;
        }
    }

    private async Task HandleJoinQueueAsync(string fromAddr, JoinQueueRequest join)
    {
        // The peer addr is the player's stable identity for the duration of
        // this match attempt — we use it both as the introduction target AND
        // as the player-id namespace. Map peer-addr → stable Guid via name-based
        // hashing so the same peer always lands in the same queue slot.
        var playerId = StableGuidFromAddr(fromAddr);

        TPayload payload;
        try
        {
            payload = DeserializePayload(join.Payload.ToByteArray());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "failed to deserialise payload from {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "bad_payload", "could not deserialise payload").ConfigureAwait(false);
            return;
        }

        if (_verifier is not null)
        {
            bool ok;
            try
            {
                ok = await _verifier.VerifyAsync(playerId, payload, _shutdownCts.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "verifier threw for {Addr}", fromAddr);
                await SendErrorAsync(fromAddr, "rejected", "payload rejected by verifier").ConfigureAwait(false);
                return;
            }
            if (!ok)
            {
                await SendErrorAsync(fromAddr, "rejected", "payload rejected by verifier").ConfigureAwait(false);
                return;
            }
        }

        Guid? privateGameId = null;
        if (!string.IsNullOrEmpty(join.PrivateGameId) &&
            Guid.TryParse(join.PrivateGameId, out var parsed))
        {
            privateGameId = parsed;
        }

        var sessionId = await EnqueueAndTrackAsync(
            playerId, fromAddr, payload, privateGameId, codeIfCreator: null).ConfigureAwait(false);

        var resp = new MatchmakerResponse
        {
            Queued = new QueuedResponse
            {
                SessionId = sessionId,
                PrivateGameId = join.PrivateGameId ?? string.Empty,
            },
        };
        await SendResponseAsync(fromAddr, resp).ConfigureAwait(false);
    }

    private async Task HandleCreatePrivateMatchAsync(string fromAddr, CreatePrivateMatchRequest req)
    {
        var playerId = StableGuidFromAddr(fromAddr);

        TPayload payload;
        try
        {
            payload = DeserializePayload(req.Payload.ToByteArray());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "failed to deserialise create-private-match payload from {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "bad_payload", "could not deserialise payload").ConfigureAwait(false);
            return;
        }

        if (!await VerifyPayloadAsync(playerId, fromAddr, payload).ConfigureAwait(false))
            return;

        string code;
        Guid privateGameId;
        try
        {
            (code, privateGameId) = await _privateLobby
                .GenerateCodeAsync(_shutdownCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "private-lobby code generation failed for {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "code_generation_failed",
                "could not allocate a private code").ConfigureAwait(false);
            return;
        }

        _codeCreatedAt[code] = DateTimeOffset.UtcNow;
        var sessionId = await EnqueueAndTrackAsync(
            playerId, fromAddr, payload, privateGameId, codeIfCreator: code).ConfigureAwait(false);

        var resp = new MatchmakerResponse
        {
            PrivateMatchCreated = new PrivateMatchCreated
            {
                SessionId = sessionId,
                Code = code,
                PrivateGameId = privateGameId.ToString(),
            },
        };
        await SendResponseAsync(fromAddr, resp).ConfigureAwait(false);
    }

    private async Task HandleJoinPrivateByCodeAsync(string fromAddr, JoinPrivateByCodeRequest req)
    {
        var playerId = StableGuidFromAddr(fromAddr);

        TPayload payload;
        try
        {
            payload = DeserializePayload(req.Payload.ToByteArray());
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "failed to deserialise join-private payload from {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "bad_payload", "could not deserialise payload").ConfigureAwait(false);
            return;
        }

        if (string.IsNullOrEmpty(req.Code))
        {
            await SendErrorAsync(fromAddr, "unknown_code",
                "code not recognised or expired").ConfigureAwait(false);
            return;
        }

        if (!await VerifyPayloadAsync(playerId, fromAddr, payload).ConfigureAwait(false))
            return;

        Guid? gameId;
        try
        {
            gameId = await _privateLobby
                .ResolveCodeAsync(req.Code, _shutdownCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_shutdownCts.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "private-lobby resolve failed for code from {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "lookup_failed",
                "code lookup failed").ConfigureAwait(false);
            return;
        }

        if (gameId is null)
        {
            await SendErrorAsync(fromAddr, "unknown_code",
                "code not recognised or expired").ConfigureAwait(false);
            return;
        }

        var sessionId = await EnqueueAndTrackAsync(
            playerId, fromAddr, payload, gameId, codeIfCreator: null).ConfigureAwait(false);

        var resp = new MatchmakerResponse
        {
            Queued = new QueuedResponse
            {
                SessionId = sessionId,
                PrivateGameId = gameId.Value.ToString(),
            },
        };
        await SendResponseAsync(fromAddr, resp).ConfigureAwait(false);
    }

    private async Task<bool> VerifyPayloadAsync(Guid playerId, string fromAddr, TPayload payload)
    {
        if (_verifier is null) return true;

        bool ok;
        try
        {
            ok = await _verifier.VerifyAsync(playerId, payload, _shutdownCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "verifier threw for {Addr}", fromAddr);
            await SendErrorAsync(fromAddr, "rejected", "payload rejected by verifier").ConfigureAwait(false);
            return false;
        }
        if (!ok)
        {
            await SendErrorAsync(fromAddr, "rejected", "payload rejected by verifier").ConfigureAwait(false);
            return false;
        }
        return true;
    }

    private async Task<string> EnqueueAndTrackAsync(
        Guid playerId,
        string fromAddr,
        TPayload payload,
        Guid? privateGameId,
        string? codeIfCreator)
    {
        var ticket = new Ticket<TPayload>(playerId, DateTime.UtcNow, payload, privateGameId);
        await _queue.EnqueueAsync(ticket, _shutdownCts.Token).ConfigureAwait(false);

        var sessionId = Guid.NewGuid().ToString("N");
        var entry = new SessionEntry(playerId, fromAddr, privateGameId, codeIfCreator);
        _sessions[sessionId] = entry;

        // Replace any prior session for this player (if they re-queued without
        // a leave). Keep at-most-one session per player so a leave-by-session
        // always resolves cleanly.
        if (_playerToSession.TryGetValue(playerId, out var oldSession) && oldSession != sessionId)
        {
            if (_sessions.TryRemove(oldSession, out var oldEntry))
            {
                // If the stale session owned a code, release it — we'll never
                // see a leave for it.
                if (oldEntry.CodeIfCreator is { } staleCode)
                {
                    _codeCreatedAt.TryRemove(staleCode, out _);
                    _ = TryExpireCodeAsync(staleCode);
                }
            }
        }
        _playerToSession[playerId] = sessionId;

        return sessionId;
    }

    private async Task HandleLeaveQueueAsync(string fromAddr, LeaveQueueRequest leave)
    {
        if (string.IsNullOrEmpty(leave.SessionId) ||
            !_sessions.TryRemove(leave.SessionId, out var entry))
        {
            await SendErrorAsync(fromAddr, "unknown_session",
                "session not found").ConfigureAwait(false);
            return;
        }

        _playerToSession.TryRemove(entry.PlayerId, out _);
        await _queue.RemoveAsync(entry.PlayerId, _shutdownCts.Token).ConfigureAwait(false);

        // Creator leaving without being matched releases the code — second
        // player can no longer join. Joiners leaving never touch the code.
        if (entry.CodeIfCreator is { } code)
        {
            _codeCreatedAt.TryRemove(code, out _);
            await TryExpireCodeAsync(code).ConfigureAwait(false);
        }

        // Acknowledge with an empty QueueStatus echoing the session id so the
        // caller can correlate. Defining a distinct LeaveAcknowledged proto
        // would be a v0.x addition.
        var resp = new MatchmakerResponse
        {
            Status = new QueueStatus
            {
                SessionId = leave.SessionId,
                Position = 0,
                WaitedMs = 0,
            },
        };
        await SendResponseAsync(fromAddr, resp).ConfigureAwait(false);
    }

    private async Task TryExpireCodeAsync(string code)
    {
        try
        {
            await _privateLobby.ExpireCodeAsync(code, _shutdownCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "private-lobby ExpireCodeAsync failed for code");
        }
    }

    private async Task SendResponseAsync(string toAddr, MatchmakerResponse resp)
    {
        if (TestResponseSink is { } sink)
        {
            try
            {
                await sink(toAddr, resp).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "test response sink threw");
            }
            return;
        }

        var svc = _service;
        if (svc is null) return;
        try
        {
            await svc.SendBytesAsync(toAddr, resp.ToByteArray(), _shutdownCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "send response to {Addr} failed", toAddr);
        }
    }

    private Task SendErrorAsync(string toAddr, string code, string message) =>
        SendResponseAsync(toAddr, new MatchmakerResponse
        {
            Error = new ErrorResponse { Code = code, Message = message },
        });

    private async Task MatchLoopAsync(CancellationToken ct)
    {
        var interval = _options.EffectiveMatchTickInterval;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await TickAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "match loop tick failed");
                }

                try
                {
                    await Task.Delay(interval, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            _logger.LogDebug("match loop exiting");
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        // 1. Prune expired private codes. Cheap and lazy — at most O(codes)
        //    per tick, and a host with no private flow pays an empty-dict
        //    enumeration. Set PrivateCodeTtl = Timeout.InfiniteTimeSpan in
        //    options to opt out (lobby-side TTL, e.g. Redis EXPIRE).
        await PruneExpiredPrivateCodesAsync(ct).ConfigureAwait(false);

        // Tick-cadence gauges (~1s): waiting players + outstanding private
        // codes. Pushed before the match attempt so a queue that drains this
        // tick still shows its pre-match depth once.
        await PushMetricSafeAsync(MetricQueueDepth, _sessions.Count, ct).ConfigureAwait(false);
        await PushMetricSafeAsync(MetricActiveLobbies, _codeCreatedAt.Count, ct).ConfigureAwait(false);

        var result = await _matcher.TryMatchAsync(ct).ConfigureAwait(false);
        if (result is null || result.Teams.Count == 0)
            return;

        var svc = _service;
        if (svc is null) return;

        var expiresAtMs = DateTimeOffset.UtcNow
            .Add(_options.EffectiveIntroductionExpiry)
            .ToUnixTimeMilliseconds();

        var teammatesOnly = _options.IntroduceTeammatesOnly;

        // Resolve every matched ticket's peer-addr AND queued session id from
        // the session map. The queued session id is the only correlator the
        // player has for replay protection — the player's QueueHandle filter
        // accepts a PeerIntroduction iff its SessionId matches the queued id
        // it was issued in the QueuedResponse.
        var matched = new List<(Ticket<TPayload> Ticket, string PeerAddr, string QueuedSessionId, int TeamIndex)>();
        foreach (var team in result.Teams)
        {
            foreach (var t in team.Members)
            {
                if (_playerToSession.TryGetValue(t.PlayerId, out var sid) &&
                    _sessions.TryGetValue(sid, out var entry))
                {
                    matched.Add((t, entry.PeerAddr, sid, team.Index));
                }
                else
                {
                    _logger.LogWarning(
                        "matched ticket {PlayerId} has no session — skipping introduction",
                        t.PlayerId);
                }
            }
        }

        // The daemon's IntroducePeer(toAddr, otherAddr, sessionId) delivers an
        // event with SessionId=sessionId to BOTH endpoints. Because each player
        // can only validate introductions carrying THEIR queued session id, we
        // emit two directed calls per unordered pair — one for each recipient's
        // correlator. Each player thus sees one accepted intro per pair
        // (sessionId == their queued id) and one filtered-out intro (sessionId
        // == the other player's queued id). The doubling is intentional.
        for (var i = 0; i < matched.Count; i++)
        {
            for (var j = i + 1; j < matched.Count; j++)
            {
                if (teammatesOnly && matched[i].TeamIndex != matched[j].TeamIndex)
                    continue;

                await EmitIntroductionAsync(svc, matched[i].PeerAddr, matched[j].PeerAddr,
                    matched[i].QueuedSessionId, expiresAtMs, ct).ConfigureAwait(false);
                await EmitIntroductionAsync(svc, matched[j].PeerAddr, matched[i].PeerAddr,
                    matched[j].QueuedSessionId, expiresAtMs, ct).ConfigureAwait(false);
            }
        }

        // One formed match per successful TryMatch result; per-player wait
        // observed from the ticket's own enqueue stamp.
        if (matched.Count > 0)
        {
            await PushMetricSafeAsync(MetricMatchesFormed, 1, ct).ConfigureAwait(false);
            var now = DateTime.UtcNow;
            foreach (var (ticket, _, _, _) in matched)
            {
                var wait = (now - ticket.EnqueuedAt).TotalSeconds;
                await PushMetricSafeAsync(MetricTimeToMatch, wait < 0 ? 0 : wait, ct).ConfigureAwait(false);
            }
        }

        // Prune queue and session map for every matched ticket. Best-effort —
        // a remove failure is logged but doesn't abort the loop. For tickets
        // owning a private-match code, release the code from the lobby (the
        // match has formed; the code's job is done).
        foreach (var (ticket, _, _, _) in matched)
        {
            try
            {
                await _queue.RemoveAsync(ticket.PlayerId, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "queue remove failed for {PlayerId}", ticket.PlayerId);
            }

            string? codeToRelease = null;
            if (_playerToSession.TryRemove(ticket.PlayerId, out var sid) &&
                _sessions.TryRemove(sid, out var entry) &&
                entry.CodeIfCreator is { } code)
            {
                codeToRelease = code;
            }

            if (codeToRelease is not null)
            {
                _codeCreatedAt.TryRemove(codeToRelease, out _);
                await TryExpireCodeAsync(codeToRelease).ConfigureAwait(false);
            }
        }
    }

    private async Task PruneExpiredPrivateCodesAsync(CancellationToken ct)
    {
        var ttl = _options.EffectivePrivateCodeTtl;
        if (ttl == Timeout.InfiniteTimeSpan || _codeCreatedAt.IsEmpty)
            return;

        var now = DateTimeOffset.UtcNow;
        List<string>? expired = null;
        foreach (var kvp in _codeCreatedAt)
        {
            if (now - kvp.Value >= ttl)
            {
                (expired ??= new List<string>()).Add(kvp.Key);
            }
        }
        if (expired is null) return;

        foreach (var code in expired)
        {
            if (!_codeCreatedAt.TryRemove(code, out _))
                continue;

            // Find the owning creator session (if still live) and clear it so
            // a subsequent LeaveQueue / match attempt sees a clean state.
            foreach (var sessionKvp in _sessions)
            {
                if (sessionKvp.Value.CodeIfCreator == code)
                {
                    if (_sessions.TryRemove(sessionKvp.Key, out var entry))
                    {
                        _playerToSession.TryRemove(entry.PlayerId, out _);
                        try
                        {
                            await _queue.RemoveAsync(entry.PlayerId, ct).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            _logger.LogWarning(ex,
                                "queue remove during private-code TTL eviction failed for {PlayerId}",
                                entry.PlayerId);
                        }
                    }
                    break;
                }
            }

            await TryExpireCodeAsync(code).ConfigureAwait(false);
        }
    }

    private async Task EmitIntroductionAsync(
        RegisteredService svc,
        string toAddr,
        string otherAddr,
        string sessionId,
        long expiresAtMs,
        CancellationToken ct)
    {
        try
        {
            await svc.IntroducePeersAsync(
                toAddr: toAddr,
                otherAddr: otherAddr,
                sessionId: sessionId,
                expiresAtMs: expiresAtMs,
                roleHint: string.Empty,
                payload: null,
                ct: ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex,
                "IntroducePeersAsync failed for {To} <- {Other}",
                toAddr, otherAddr);
        }
    }

    private TPayload DeserializePayload(byte[] bytes)
    {
        if (_options.DeserializePayload is { } deser)
            return deser(bytes);
        // Fallback for TPayload == byte[]: pass-through. Any other TPayload
        // without an explicit deserialiser is a configuration error.
        if (bytes is TPayload typed)
            return typed;
        throw new InvalidOperationException(
            $"MatchmakerOptions.DeserializePayload is required for TPayload = {typeof(TPayload).FullName}");
    }

    private static Guid StableGuidFromAddr(string addr)
    {
        // Deterministic name-based UUIDv5-ish: SHA-256 the addr, take 16 bytes,
        // mask version/variant. Not cryptographically meaningful — just a
        // stable mapping so a re-queueing player lands in the same queue slot.
        using var sha = System.Security.Cryptography.SHA256.Create();
        var hash = sha.ComputeHash(System.Text.Encoding.UTF8.GetBytes(addr));
        var bytes = new byte[16];
        Array.Copy(hash, bytes, 16);
        // RFC 4122 v5 variant/version bits.
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x50);
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80);
        return new Guid(bytes);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try { _shutdownCts.Cancel(); } catch { /* best effort */ }

        if (_matchLoop is not null)
        {
            try
            {
                await _matchLoop.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "match loop ended with exception during dispose");
            }
        }

        if (_service is not null)
        {
            try
            {
                await _service.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "service dispose threw");
            }
        }

        _shutdownCts.Dispose();
    }

    private readonly record struct SessionEntry(
        Guid PlayerId,
        string PeerAddr,
        Guid? PrivateGameId,
        string? CodeIfCreator);
}
