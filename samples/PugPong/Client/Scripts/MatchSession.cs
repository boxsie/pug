using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Godot;
using PUG.Ensemble;
using PUG.Netcode;
using PUG.Netcode.Prediction;
using PugPong.Proto;

namespace PugPong.Client;

/// <summary>
/// In-match networking + simulation glue on the <c>PUG.Netcode</c> stack, now with the
/// opt-in <c>PUG.Netcode.Prediction</c> (Tier C) feel layer wired on the guest:
/// <list type="bullet">
/// <item>world state (ball / both paddles / score) replicates as quantized
///   <see cref="INetEntityState"/> entities through a <see cref="NetworkReplicator"/>
///   (Tier B1, KeepLatest snapshots);</item>
/// <item>the guest's paddle rides <i>up</i> a KeepLatest input channel via
///   <see cref="NetInputChannel"/> (Tier B3, latest-wins);</item>
/// <item>goal / match-end are discrete <see cref="NetEventChannel"/> events on the
///   Ordered channel (Tier B2 — reliable, never dropped);</item>
/// <item>a pumped <see cref="TimeSync"/> on ch0 (Tier A3) gives the guest the
///   authority-tick domain it renders interpolation in;</item>
/// <item>the host simulation is driven by a pumped <see cref="TickClock"/> out of
///   Godot <c>_Process</c> — a real-clock accumulator, no <c>Task.Delay</c>, no
///   background sim thread.</item>
/// </list>
///
/// <para>
/// <b>Tier C is guest-only.</b> The host is authoritative truth and needs no smoothing
/// (<c>c25df4da</c>: in alphabetical-host P2P the GUEST is the choppy side). On the guest:
/// the ball and the host's paddle are <see cref="INetInterpolable"/> and rendered <i>between</i>
/// snapshots by an <see cref="InterpolatingApplyStrategy"/> at a render time held in the past
/// (C1); the guest's OWN paddle is <see cref="INetReconcilable"/> — predicted on the input frame
/// for zero-latency feel (C2), excluded from interpolation so a stale snapshot can't rubber-band
/// it, and reconciled against authority without popping (C3). The host pumps TimeSync only as a
/// ping responder and short-circuits every Tier C step.
/// </para>
///
/// <para>
/// <b>Authority is injected, never elected by the core.</b> Alphabetical-host: the lower-sorting
/// player-service address owns the world (<see cref="Role.Host"/>). At 1v1 the one client is
/// always <see cref="PeerId"/> 1, so no wire id-assignment is needed (the host tags the guest's
/// paddle owner = peer 1; the guest's injected <see cref="NetSession.SelfId"/> is peer 1 — they
/// match by construction).
/// </para>
/// </summary>
public sealed class MatchSession : IAsyncDisposable
{
    public enum Role { Host, Guest }

    public const float PaddleHalfHeight = 0.075f;        // 0..1 normalized
    public const float PaddleSurfaceXHost = 0.035f;      // x at which the host paddle's inner edge sits
    public const float PaddleSurfaceXGuest = 0.965f;
    public const float BallHalfH = 0.0111f;              // ~6 px on 540 vertical = ball radius in 0..1
    public const float BallHalfW = 0.00625f;             // ~6 px on 960 horizontal
    public const float BallSpeed = 0.6f;                 // units / sec
    public const int   ScoreCap = 5;
    public const float SimTickHz = 60f;
    public const float SendHz = 30f;

    // Channel layout, declared identically on both ends. ch0 carries A3 TimeSync,
    // pumped on both ends now (C5): the guest auto-pings, the host answers.
    private const byte ChTime = 0;
    private const byte ChSnapshot = 1;
    private const byte ChInput = 2;
    private const byte ChEvents = 3;
    private static readonly ChannelSpec[] Channels =
    {
        new(ChTime, ChannelMode.Unreliable),
        new(ChSnapshot, ChannelMode.KeepLatest),
        new(ChInput, ChannelMode.KeepLatest),
        new(ChEvents, ChannelMode.Ordered),
    };

    // Entity archetypes (the byte the replicator's spawn factory switches on).
    private const byte KindBall = 1;
    private const byte KindPaddle = 2;
    private const byte KindScore = 3;

    // Discrete event types (first byte of an opaque NetEventChannel payload).
    private const byte EvGoal = 0;
    private const byte EvMatchEnd = 1;

    // The single client is always peer 1 at 1v1 (host tags the guest paddle's owner
    // with this; the guest is constructed with this SelfId — they match).
    private static readonly PeerId GuestId = new(1);

    private static readonly TimeSpan SimDt = TimeSpan.FromSeconds(1.0 / SimTickHz);

    private readonly NetSession _session;
    private readonly NetDiagnostics _diagnostics;
    private readonly TickClock _clock;
    private readonly NetworkReplicator _replicator;
    private readonly NetEventChannel _events;
    private readonly NetInputChannel _input;
    private readonly TimeSync? _timeSync;
    private readonly Random _rng = new();
    private readonly List<GameEvent> _eventSink = new();
    private readonly byte[] _inputScratch = new byte[2];

    // Guest-only Tier C lanes. Null on the host (it IS authority — no smoothing).
    private readonly InterpolatingApplyStrategy? _interp;
    private readonly Predictor? _predictor;
    private readonly Reconciler? _reconciler;

    // Host-authoritative world. These objects ARE the host's sim state; the
    // replicator serializes the parts the guest needs. On the guest they stay null —
    // its world is the replicator's reconstructed entities.
    private readonly BallEntity? _ball;
    private readonly PaddleEntity? _hostPaddle;  // P0, left, owner = authority
    private readonly PaddleEntity? _guestPaddle; // P1, right, owner = peer 1
    private readonly ScoreEntity? _score;

    // Guest: the reconstructed PaddleEntity it owns (P1). Resolved lazily once the
    // first snapshot spawns it, then excluded from interpolation and driven by the
    // predict/reconcile lane.
    private PaddleEntity? _ownPaddle;

    private Task? _snapshotInFlight; // serializes CaptureAndBroadcast's reused buffer
    private Task? _timeSyncInFlight; // serializes TimeSync's ch0 receive/send pump
    private float _sendAccumulator;  // host snapshot cadence (~SendHz)
    private int _sendErrorLogged;
    private int _disposed;

    public Role Authority { get; }

    // Render-visible state, pulled by Match._Process. Reads on the scene thread
    // accept being one tick stale; invisible at 60 Hz.
    public float BallX = 0.5f, BallY = 0.5f;
    public float P0Y = 0.5f, P1Y = 0.5f;
    public int Score0, Score1;
    public uint Tick;
    public bool Ended;
    public int Winner = -1;

    // Local paddle command applied each render frame; the host writes its own paddle
    // here, the guest writes its predicted paddle target here AND sends it up the
    // input channel (where it also drives the prediction lane).
    public float LocalPaddleY = 0.5f;

    /// <param name="localServiceAddr">This player's own Ensemble player-service
    ///   address (<see cref="QueueHandle{TPayload}.PlayerServiceAddress"/>). MUST be
    ///   the player-service address, NOT the node identity: host election compares it
    ///   against the peer's service address, so both sides order the same pair or
    ///   both elect Host.</param>
    public MatchSession(QueueHandle<PongPayload> handle, MatchFound match, string localServiceAddr)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(match);
        if (match.Peers.Count == 0)
        {
            throw new ArgumentException("MatchFound has no peers", nameof(match));
        }

        var peerAddr = match.Peers[0].EnsembleAddr;

        // Alphabetical-host: of the two PLAYER-SERVICE addresses, the lower sorts
        // first → host. Both sides compare the same service-addr pair, so exactly one
        // elects Host with no coordination round.
        Authority = string.CompareOrdinal(localServiceAddr, peerAddr) < 0 ? Role.Host : Role.Guest;

        // Wire a real log sink — a null logger here is exactly the trap that cost a
        // debugging session (learning 68df6c92).
        _diagnostics = new NetDiagnostics
        {
            LogSink = static (level, message) => GD.Print($"[net:{level}] {message}"),
        };

        // The one place the netcode stack touches Ensemble: wrap the matched handle.
        var link = handle.AsPeerLink(match);
        _clock = new TickClock((int)SimTickHz);

        if (Authority == Role.Host)
        {
            _session = NetSession.CreateAuthority(Channels, new[] { link }, _diagnostics);
            _replicator = NetworkReplicator.CreateAuthority(_session, ChSnapshot);

            _ball = new BallEntity();
            _hostPaddle = new PaddleEntity();
            _guestPaddle = new PaddleEntity();
            _score = new ScoreEntity();

            _replicator.Register(_ball);                  // world-owned (authority)
            _replicator.Register(_hostPaddle);            // host's own paddle — also authority
            _replicator.Register(_guestPaddle, GuestId);  // the guest controls this one
            _replicator.Register(_score);                 // world-owned
        }
        else
        {
            // Tier C lanes (guest only). The interpolating strategy is the B1 apply
            // seam doing its job — remote entities blend between snapshots; the owned
            // paddle is excluded (below) and predicted instead.
            _interp = new InterpolatingApplyStrategy();
            _predictor = new Predictor();
            _reconciler = new Reconciler(_predictor, SimDt);

            _session = NetSession.CreateClient(link, Channels, GuestId, "host", _diagnostics);
            _replicator = NetworkReplicator.CreateClient(_session, ChSnapshot, Spawn, _interp, onDespawn: ForgetEntity);

            _diagnostics.RegisterSource("interp", _interp);
            _diagnostics.RegisterSource("predict", _predictor);
            _diagnostics.RegisterSource("reconcile", _reconciler);
        }

        _diagnostics.RegisterReplicator("world", _replicator);
        _events = new NetEventChannel(_session, ChEvents);
        _input = new NetInputChannel(_session, ChInput);

        // A3 TimeSync on ch0. The guest auto-pings to learn the authority-tick offset
        // it renders interpolation in; the host only answers (responder, no pinging).
        var syncMux = Authority == Role.Host
            ? (_session.Peers.Count > 0 ? _session.Peers[0].Mux : null)
            : _session.AuthorityPeer?.Mux;
        if (syncMux is not null)
        {
            _timeSync = new TimeSync(syncMux, ChTime, _clock,
                new TimeSyncOptions { AutoPing = Authority == Role.Guest });
        }
    }

    public void Start()
    {
        if (Authority == Role.Host && _ball is not null)
        {
            // Kick the ball off in a random-ish direction.
            var sign = _rng.Next(0, 2) == 0 ? -1f : 1f;
            _ball.Vx = sign * BallSpeed;
            _ball.Vy = (float)(_rng.NextDouble() - 0.5) * BallSpeed * 0.6f;
        }
    }

    /// <summary>Scene-side: apply local paddle input. Host moves P0 directly; the guest
    /// only records the target — the prediction lane (see <see cref="SendInput"/>) drives
    /// its paddle entity, and render reads that.</summary>
    public void SetLocalPaddleY(float y)
    {
        y = Math.Clamp(y, PaddleHalfHeight, 1f - PaddleHalfHeight);
        LocalPaddleY = y;
        if (Authority == Role.Host)
        {
            _hostPaddle!.Y = y;
            P0Y = y;
        }
    }

    /// <summary>Guest-side: predict the owned paddle on this frame and ship the input up.
    /// No-op on the host. Fire-and-forget send (fresh buffer); a dropped input is harmless
    /// on the latest-wins channel.</summary>
    public void SendInput()
    {
        if (Authority != Role.Guest)
        {
            return;
        }

        // Stamp with the tick the AUTHORITY will process this on (local tick + the
        // TimeSync offset), so reconciliation can line the snapshot up against the
        // right buffered input. The host ignores the stamp (latest-wins), but the
        // guest's own replay depends on it.
        var authorityTick = AuthorityTickNow();
        BinaryPrimitives.WriteUInt16BigEndian(_inputScratch, Quantize(LocalPaddleY));

        // Predict immediately for zero-latency feel: Simulate sets the paddle's Y from
        // the input. Buffers the input for C3 replay. The owned paddle is null until the
        // first snapshot spawns it (resolved in PumpGuest) — until then render falls back
        // to LocalPaddleY and there's nothing to predict yet.
        if (_ownPaddle is not null)
        {
            _predictor!.Predict(_ownPaddle, _inputScratch, authorityTick, SimDt);
        }

        _ = SafeSendAsync(_input.SendToAuthorityAsync(authorityTick, _inputScratch).AsTask());
    }

    /// <summary>Advance the session one engine frame — call once from
    /// <c>_Process(delta)</c>. The whole stack is pumped here: no background loop.</summary>
    public void Pump(double delta)
    {
        PumpTimeSync();
        if (Authority == Role.Host)
        {
            PumpHost(delta);
        }
        else
        {
            PumpGuest(delta);
        }
    }

    /// <summary>A compact diagnostics line for the F3 overlay — live RTT plus the Tier C
    /// counters pulled through the cross-package <see cref="INetStatSource"/> hook. Built
    /// on demand (the overlay is toggled), so the per-frame allocation only happens when
    /// it's visible.</summary>
    public string DiagnosticsText()
    {
        var rttMs = _timeSync?.Rtt.TotalMilliseconds ?? 0;
        if (Authority == Role.Host)
        {
            return $"HOST  tick={Tick}  rtt={rttMs:F0}ms  peers={_session.Peers.Count}\n{_diagnostics.Describe()}";
        }

        return $"GUEST tick={Tick}  rtt={rttMs:F0}ms  offset={_timeSync?.TickOffset ?? 0}\n{_diagnostics.Describe()}";
    }

    private void PumpTimeSync()
    {
        if (_timeSync is null)
        {
            return;
        }

        // Fire-and-forget, single in-flight: UpdateAsync drains ch0 (answer pings, fold
        // pongs) then maybe sends — guard it like the snapshot so two pumps don't race the
        // ch0 queue. A skipped frame just folds next frame; the 100 ms ping cadence is slow.
        if (_timeSyncInFlight is null || _timeSyncInFlight.IsCompleted)
        {
            _timeSyncInFlight = _timeSync.UpdateAsync().AsTask();
            _ = SafeSendAsync(_timeSyncInFlight);
        }
    }

    private void PumpHost(double delta)
    {
        // Fold the guest's latest paddle into P1 before stepping.
        _input.Drain();
        if (_input.TryGetLatest(GuestId, out var gi) && gi.Payload.Length >= 2)
        {
            _guestPaddle!.Y = Math.Clamp(Dequantize(BinaryPrimitives.ReadUInt16BigEndian(gi.Payload.Span)),
                PaddleHalfHeight, 1f - PaddleHalfHeight);
        }

        var dt = 1f / SimTickHz;
        var steps = _clock.Advance(TimeSpan.FromSeconds(delta));
        for (var i = 0; i < steps && !Ended; i++)
        {
            SimStep(dt);
        }

        Tick = (uint)_clock.CurrentTick;
        MirrorHostRenderState();

        // Broadcast a full-world snapshot at ~SendHz. Guard the replicator's reused
        // buffer with a single in-flight send: KeepLatest means dropping a snapshot
        // because the previous is still in flight is self-healing, not a loss.
        _sendAccumulator += (float)delta;
        if (_sendAccumulator >= 1f / SendHz)
        {
            _sendAccumulator = 0f;
            if (_snapshotInFlight is null || _snapshotInFlight.IsCompleted)
            {
                _snapshotInFlight = _replicator.CaptureAndBroadcastAsync((uint)_clock.CurrentTick).AsTask();
                _ = SafeSendAsync(_snapshotInFlight);
            }
        }
    }

    private void PumpGuest(double delta)
    {
        // Advance the local clock — both to stamp outgoing input ticks and to place the
        // interpolation render time in authority-tick space.
        _clock.Advance(TimeSpan.FromSeconds(delta));

        // Land the newest authoritative snapshot. Remote interpolable entities (ball,
        // host paddle) are BUFFERED here (not snapped); the owned paddle, once excluded,
        // is CAPTURED for reconciliation rather than applied.
        _replicator.Apply();

        ResolveOwnPaddle();

        // C3: rewind the owned paddle to its latest authoritative state and replay the
        // unconfirmed local inputs, easing the visible paddle toward the corrected state.
        if (_ownPaddle is not null && _interp!.TryGetLatestAuthoritative(_ownPaddle, out var authTick, out var authState))
        {
            _reconciler!.Reconcile(_ownPaddle, authTick, authState);
        }

        // C1: blend remote entities toward a render time held in the past so a bracketing
        // pair of snapshots always straddles it. renderTick is in authority-tick space.
        var authorityNow = AuthorityTickNow();
        var renderTick = authorityNow >= _interp!.InterpDelayTicks ? authorityNow - _interp.InterpDelayTicks : 0u;
        _interp.Render(renderTick);

        // Read the (now interpolated / reconciled) world into render fields.
        foreach (var entity in _replicator.Entities.Values)
        {
            switch (entity.Kind)
            {
                case KindBall:
                    var ball = (BallEntity)entity.State;
                    BallX = ball.X;
                    BallY = ball.Y;
                    break;
                case KindPaddle when entity.Owner.IsAuthority:
                    P0Y = ((PaddleEntity)entity.State).Y; // host's paddle (interpolated)
                    break;
                case KindPaddle:
                    break; // my own paddle — read below from the predicted entity
                case KindScore:
                    var score = (ScoreEntity)entity.State;
                    Score0 = score.S0;
                    Score1 = score.S1;
                    break;
            }
        }

        // Own paddle: the predicted + reconciled state (responsive); fall back to the raw
        // local target until the snapshot has spawned the entity.
        P1Y = _ownPaddle?.Y ?? LocalPaddleY;

        Tick = (uint)_clock.CurrentTick;

        // Discrete events: score TRUTH rides the snapshot (above); a goal event is a
        // transient notification (a place to hang a sound/flash), match-end is the
        // crucial transition that must never be lost — hence the reliable channel.
        _eventSink.Clear();
        _events.Drain(_eventSink);
        foreach (var ev in _eventSink)
        {
            if (ev.Payload.Length < 1)
            {
                continue;
            }

            switch (ev.Payload.Span[0])
            {
                case EvGoal:
                    var scorer = ev.Payload.Length >= 2 ? ev.Payload.Span[1] : (byte)0;
                    _diagnostics.Info($"goal: player {scorer} scored @tick {ev.Tick}");
                    break;
                case EvMatchEnd when ev.Payload.Length >= 4:
                    Ended = true;
                    Winner = ev.Payload.Span[1];
                    Score0 = ev.Payload.Span[2];
                    Score1 = ev.Payload.Span[3];
                    break;
            }
        }
    }

    /// <summary>Guest: find the reconstructed paddle this client owns (peer 1) once a
    /// snapshot has spawned it, then exclude it from the interpolation lane so it's driven
    /// purely by predict + reconcile (no rubber-band, per <c>c25df4da</c>).</summary>
    private void ResolveOwnPaddle()
    {
        if (_ownPaddle is not null)
        {
            return;
        }

        foreach (var entity in _replicator.EntitiesOwnedBy(GuestId))
        {
            if (entity.Kind == KindPaddle)
            {
                _ownPaddle = (PaddleEntity)entity.State;
                _interp!.Exclude(_ownPaddle);
                break;
            }
        }
    }

    /// <summary>Authority tick "now" = local tick + the TimeSync offset (authority − local).
    /// Falls back to the bare local tick before the first pong folds in.</summary>
    private uint AuthorityTickNow() => (uint)((long)_clock.CurrentTick + (_timeSync?.TickOffset ?? 0));

    private void ForgetEntity(ushort id, INetEntityState entity)
    {
        _interp?.Forget(entity);
        if (ReferenceEquals(entity, _ownPaddle))
        {
            _ownPaddle = null;
        }
    }

    private void SimStep(float dt)
    {
        var ball = _ball!;
        ball.X += ball.Vx * dt;
        ball.Y += ball.Vy * dt;

        // Top / bottom walls — bounce on ball EDGE, not center; Math.Abs guards a
        // double-reversal if a fast ball overshoots.
        if (ball.Y - BallHalfH <= 0f)
        {
            ball.Y = BallHalfH;
            ball.Vy = Math.Abs(ball.Vy);
        }
        else if (ball.Y + BallHalfH >= 1f)
        {
            ball.Y = 1f - BallHalfH;
            ball.Vy = -Math.Abs(ball.Vy);
        }

        // Host paddle (P0): leading edge reaches the inner X and the Y extents overlap
        // (expanded by ball half-height so a graze counts).
        if (ball.Vx < 0 && ball.X - BallHalfW <= PaddleSurfaceXHost && Math.Abs(ball.Y - _hostPaddle!.Y) <= PaddleHalfHeight + BallHalfH)
        {
            ball.X = PaddleSurfaceXHost + BallHalfW;
            ball.Vx = -ball.Vx;
            ball.Vy += (ball.Y - _hostPaddle.Y) * BallSpeed * 0.5f;
        }
        // Guest paddle (P1): mirror image.
        else if (ball.Vx > 0 && ball.X + BallHalfW >= PaddleSurfaceXGuest && Math.Abs(ball.Y - _guestPaddle!.Y) <= PaddleHalfHeight + BallHalfH)
        {
            ball.X = PaddleSurfaceXGuest - BallHalfW;
            ball.Vx = -ball.Vx;
            ball.Vy += (ball.Y - _guestPaddle.Y) * BallSpeed * 0.5f;
        }

        // No left/right walls — those are scoring zones.
        if (ball.X < 0f)
        {
            _score!.S1++;
            FireGoal(scorer: 1);
            ResetBall(servingTo: +1);
        }
        else if (ball.X > 1f)
        {
            _score!.S0++;
            FireGoal(scorer: 0);
            ResetBall(servingTo: -1);
        }

        if (_score!.S0 >= ScoreCap || _score.S1 >= ScoreCap)
        {
            Ended = true;
            Winner = _score.S0 >= ScoreCap ? 0 : 1;
            FireMatchEnd();
        }
    }

    private void ResetBall(int servingTo)
    {
        var ball = _ball!;
        ball.X = 0.5f;
        ball.Y = 0.5f;
        ball.Vx = servingTo * BallSpeed;
        ball.Vy = (float)(_rng.NextDouble() - 0.5) * BallSpeed * 0.4f;
    }

    private void MirrorHostRenderState()
    {
        BallX = _ball!.X;
        BallY = _ball.Y;
        P0Y = _hostPaddle!.Y;
        P1Y = _guestPaddle!.Y;
        Score0 = _score!.S0;
        Score1 = _score.S1;
    }

    // Goal / match-end are rare, so a fresh small array per event is fine and keeps
    // them clear of the snapshot buffer's in-flight guard.
    private void FireGoal(int scorer) =>
        _ = SafeSendAsync(_events.BroadcastAsync((uint)_clock.CurrentTick, new[] { EvGoal, (byte)scorer }).AsTask());

    private void FireMatchEnd() =>
        _ = SafeSendAsync(_events.BroadcastAsync(
            (uint)_clock.CurrentTick, new[] { EvMatchEnd, (byte)Winner, (byte)_score!.S0, (byte)_score.S1 }).AsTask());

    private INetEntityState Spawn(byte kind) => kind switch
    {
        KindBall => new BallEntity(),
        KindPaddle => new PaddleEntity(),
        KindScore => new ScoreEntity(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown entity archetype"),
    };

    private async Task SafeSendAsync(Task send)
    {
        try
        {
            await send.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Dispose / shutdown.
        }
        catch (Exception ex)
        {
            // Best-effort at game time, but surface the FIRST failure — a persistently
            // broken link would otherwise vanish behind this catch.
            if (Interlocked.Exchange(ref _sendErrorLogged, 1) == 0)
            {
                GD.PrintErr($"[MatchSession] net send failed (further failures suppressed): {ex}");
            }
        }
    }

    // 0..1 unit float ↔ uint16, the bandwidth lever: ~1.5e-5 resolution, plenty for a
    // 960×540 board, at 2 bytes/axis instead of 4.
    private static ushort Quantize(float unit) => (ushort)(Math.Clamp(unit, 0f, 1f) * 65535f + 0.5f);

    private static float Dequantize(ushort q) => q / 65535f;

    private static float DequantizeAt(ReadOnlySpan<byte> state, int offset) =>
        Dequantize(BinaryPrimitives.ReadUInt16BigEndian(state.Slice(offset)));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            if (_snapshotInFlight is not null)
            {
                await _snapshotInFlight.ConfigureAwait(false);
            }
        }
        catch
        {
            // Best-effort drain of the last in-flight snapshot.
        }

        // The session owns its mux and the link; disposing it tears both down. The
        // link's own DisposeAsync is a no-op (the Match scene owns the QueueHandle).
        await _session.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>The ball: full sim state lives here (the host integrates Vx/Vy); only the
    /// quantized position is replicated. As an <see cref="INetInterpolable"/> the guest
    /// renders it BETWEEN snapshots — Vx/Vy stay host-only (the guest derives nothing from
    /// velocity; the smoother is positional, per c25df4da).</summary>
    private sealed class BallEntity : INetInterpolable
    {
        public float X = 0.5f, Y = 0.5f, Vx, Vy;

        public byte Kind => KindBall;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(4);
            BinaryPrimitives.WriteUInt16BigEndian(span, Quantize(X));
            BinaryPrimitives.WriteUInt16BigEndian(span.Slice(2), Quantize(Y));
            writer.Advance(4);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            X = DequantizeAt(state, 0);
            Y = DequantizeAt(state, 2);
        }

        public void ApplyInterpolated(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to, float t)
        {
            X = Lerp(DequantizeAt(from, 0), DequantizeAt(to, 0), t);
            Y = Lerp(DequantizeAt(from, 2), DequantizeAt(to, 2), t);
        }

        private static float Lerp(float a, float b, float t) => a + ((b - a) * t);
    }

    /// <summary>A paddle: just its quantized Y. Which side it is comes from
    /// <see cref="ReplicatedEntity.Owner"/> (authority = host/left, peer 1 = guest/right),
    /// not from the state. Implements BOTH Tier C lanes so the same type serves either
    /// role on the guest: the REMOTE (host) paddle is <see cref="INetInterpolable"/>; the
    /// OWNED paddle is <see cref="INetReconcilable"/> — predicted, then corrected by
    /// blending. The game owns the byte interpretation; netcode stays opaque.</summary>
    private sealed class PaddleEntity : INetInterpolable, INetReconcilable
    {
        public float Y = 0.5f;

        public byte Kind => KindPaddle;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteUInt16BigEndian(span, Quantize(Y));
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state) => Y = DequantizeAt(state, 0);

        public void ApplyInterpolated(ReadOnlySpan<byte> from, ReadOnlySpan<byte> to, float t)
        {
            var a = DequantizeAt(from, 0);
            var b = DequantizeAt(to, 0);
            Y = a + ((b - a) * t);
        }

        // The paddle's input IS its target Y (state-ish input, latest-wins). Simulating a
        // step is just adopting that target; dt plays no part. Replay applies the
        // unconfirmed targets in order, so the latest wins — exactly right.
        public void Simulate(ReadOnlySpan<byte> input, TimeSpan dt) => Y = DequantizeAt(input, 0);

        // Ease the visible paddle toward the reconciler's corrected target, so a divergence
        // decays over a few snapshots instead of popping.
        public void BlendCorrection(ReadOnlySpan<byte> target, float t)
        {
            var to = DequantizeAt(target, 0);
            Y += (to - Y) * t;
        }
    }

    /// <summary>The scoreboard: two small counts (cap 5, a byte each). Discrete state — it
    /// is NOT interpolable, so the strategy snaps it through <see cref="ApplyState"/>
    /// (parity with ImmediateApply). The goal EVENT is just a notification; this is the
    /// truth and self-heals through the snapshot stream.</summary>
    private sealed class ScoreEntity : INetEntityState
    {
        public int S0, S1;

        public byte Kind => KindScore;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            span[0] = (byte)Math.Clamp(S0, 0, 255);
            span[1] = (byte)Math.Clamp(S1, 0, 255);
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state)
        {
            S0 = state[0];
            S1 = state[1];
        }
    }
}
