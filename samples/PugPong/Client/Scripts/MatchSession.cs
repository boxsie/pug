using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Godot;
using PUG.Ensemble;
using PUG.Netcode;
using PugPong.Proto;

namespace PugPong.Client;

/// <summary>
/// In-match networking + simulation glue, rebuilt on the <c>PUG.Netcode</c> stack.
/// The hand-rolled <c>SendToPeerAsync</c>/<c>PeerMessages</c> + proto-oneof transport
/// is gone; everything now rides a <see cref="NetSession"/> over the Ensemble peer
/// link:
/// <list type="bullet">
/// <item>world state (ball / both paddles / score) replicates as quantized
///   <see cref="INetEntityState"/> entities through a <see cref="NetworkReplicator"/>
///   (Tier B1, KeepLatest snapshots);</item>
/// <item>the guest's paddle rides <i>up</i> a KeepLatest input channel via
///   <see cref="NetInputChannel"/> (Tier B3, latest-wins);</item>
/// <item>goal / match-end are discrete <see cref="NetEventChannel"/> events on the
///   Ordered channel (Tier B2 — reliable, never dropped);</item>
/// <item>the host simulation is driven by a pumped <see cref="TickClock"/> out of
///   Godot <c>_Process</c> — a real-clock accumulator, no <c>Task.Delay</c>, no
///   background sim thread.</item>
/// </list>
///
/// <para>
/// <b>Authority is injected, never elected by the core.</b> Alphabetical-host: the
/// lower-sorting player-service address owns the world (<see cref="Role.Host"/>) and
/// the result is handed to <see cref="NetSession"/> as a designation. At 1v1 the one
/// client is always <see cref="PeerId"/> 1, so no wire id-assignment is needed (the
/// host tags the guest's paddle owner = peer 1; the guest's injected
/// <see cref="NetSession.SelfId"/> is peer 1 — they match by construction).
/// </para>
///
/// <para>
/// <b>No smoothing yet.</b> The guest applies snapshots immediately
/// (<c>ImmediateApply</c>) — it renders the ~30 Hz authoritative world directly, same
/// as the prototype did. Interpolation / prediction is Tier C; it drops into B1's
/// <see cref="ISnapshotApplyStrategy"/> seam later with no change here. The guest's
/// <i>own</i> paddle is rendered from local input for responsiveness (predict-lite,
/// no reconciliation), exactly as before.
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

    // Channel layout, declared identically on both ends. ch0 is reserved for A3
    // TimeSync (Tier C era) — declared so the wire numbering is stable, not pumped
    // yet (no interpolation delay to place without it).
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

    private readonly NetSession _session;
    private readonly NetDiagnostics _diagnostics;
    private readonly TickClock _clock;
    private readonly NetworkReplicator _replicator;
    private readonly NetEventChannel _events;
    private readonly NetInputChannel _input;
    private readonly Random _rng = new();
    private readonly List<GameEvent> _eventSink = new();
    private readonly byte[] _inputScratch = new byte[2];

    // Host-authoritative world. These objects ARE the host's sim state; the
    // replicator serializes the parts the guest needs. On the guest they stay null —
    // its world is the replicator's reconstructed entities.
    private readonly BallEntity? _ball;
    private readonly PaddleEntity? _hostPaddle;  // P0, left, owner = authority
    private readonly PaddleEntity? _guestPaddle; // P1, right, owner = peer 1
    private readonly ScoreEntity? _score;

    private Task? _snapshotInFlight; // serializes CaptureAndBroadcast's reused buffer
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
    // here, the guest writes its predicted paddle here AND sends it up the input
    // channel.
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
            _session = NetSession.CreateClient(link, Channels, GuestId, "host", _diagnostics);
            _replicator = NetworkReplicator.CreateClient(_session, ChSnapshot, Spawn);
        }

        _diagnostics.RegisterReplicator("world", _replicator);
        _events = new NetEventChannel(_session, ChEvents);
        _input = new NetInputChannel(_session, ChInput);
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

    /// <summary>Scene-side: apply local paddle input. Host moves P0; guest moves P1
    /// locally (responsive) and the next <see cref="SendInput"/> ships it up.</summary>
    public void SetLocalPaddleY(float y)
    {
        y = Math.Clamp(y, PaddleHalfHeight, 1f - PaddleHalfHeight);
        LocalPaddleY = y;
        if (Authority == Role.Host)
        {
            _hostPaddle!.Y = y;
            P0Y = y;
        }
        else
        {
            // Guest renders its own paddle from local input for responsiveness; the
            // authoritative copy still arrives in snapshots but we don't snap to it
            // (predict-lite, no reconciliation — that's Tier C).
            P1Y = y;
        }
    }

    /// <summary>Guest-side: ship the local paddle up the input channel. No-op on the
    /// host. Fire-and-forget: the frame is built synchronously (fresh buffer), and a
    /// dropped input is harmless on the latest-wins channel.</summary>
    public void SendInput()
    {
        if (Authority != Role.Guest)
        {
            return;
        }

        BinaryPrimitives.WriteUInt16BigEndian(_inputScratch, Quantize(LocalPaddleY));
        _ = SafeSendAsync(_input.SendToAuthorityAsync((uint)_clock.CurrentTick, _inputScratch).AsTask());
    }

    /// <summary>Advance the session one engine frame — call once from
    /// <c>_Process(delta)</c>. The whole stack is pumped here: no background loop.</summary>
    public void Pump(double delta)
    {
        if (Authority == Role.Host)
        {
            PumpHost(delta);
        }
        else
        {
            PumpGuest(delta);
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
        // Advance the local clock only to stamp outgoing input ticks consistently.
        _clock.Advance(TimeSpan.FromSeconds(delta));

        // Apply the newest authoritative snapshot, then read the reconstructed world.
        _replicator.Apply();
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
                    P0Y = ((PaddleEntity)entity.State).Y; // host's paddle (remote)
                    break;
                case KindPaddle:
                    // My own paddle: rendered from local input (see SetLocalPaddleY),
                    // so we deliberately ignore the authoritative copy here.
                    break;
                case KindScore:
                    var score = (ScoreEntity)entity.State;
                    Score0 = score.S0;
                    Score1 = score.S1;
                    break;
            }
        }

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

    /// <summary>The ball: full sim state lives here (the host integrates Vx/Vy); only
    /// the quantized position is replicated — the guest renders position directly and
    /// derives nothing from velocity (no smoother yet).</summary>
    private sealed class BallEntity : INetEntityState
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
            X = Dequantize(BinaryPrimitives.ReadUInt16BigEndian(state));
            Y = Dequantize(BinaryPrimitives.ReadUInt16BigEndian(state.Slice(2)));
        }
    }

    /// <summary>A paddle: just its quantized Y. Which side it is comes from
    /// <see cref="ReplicatedEntity.Owner"/> (authority = host/left, peer 1 = guest/
    /// right), not from the state.</summary>
    private sealed class PaddleEntity : INetEntityState
    {
        public float Y = 0.5f;

        public byte Kind => KindPaddle;

        public void WriteState(IBufferWriter<byte> writer)
        {
            var span = writer.GetSpan(2);
            BinaryPrimitives.WriteUInt16BigEndian(span, Quantize(Y));
            writer.Advance(2);
        }

        public void ApplyState(ReadOnlySpan<byte> state) =>
            Y = Dequantize(BinaryPrimitives.ReadUInt16BigEndian(state));
    }

    /// <summary>The scoreboard: two small counts (cap 5, a byte each). Continuous
    /// state, so it self-heals through the snapshot stream — the goal EVENT is just a
    /// notification, this is the truth.</summary>
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
