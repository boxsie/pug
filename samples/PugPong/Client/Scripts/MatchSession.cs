using System;
using System.Threading;
using System.Threading.Tasks;
using Godot;
using Google.Protobuf;
using PugPong.Proto;
using PUG.Ensemble;

namespace PugPong.Client;

/// <summary>
/// In-match networking + simulation glue, sitting between the Match scene
/// and PUG.Ensemble's post-match P2P surface. Alphabetical-host authority:
/// lower-sorting E-address owns the ~60 Hz physics and ~30 Hz state sends;
/// the other side sends paddle inputs.
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

    private readonly QueueHandle<PongPayload> _handle;
    private readonly string _peerAddr;
    private readonly CancellationTokenSource _cts = new();
    private Task? _readerTask;
    private Task? _simTask;
    private int _disposed;
    private int _sendErrorLogged;

    public Role Authority { get; }

    // Render-visible state. Volatile reads on the scene thread accept being
    // one tick stale; that's invisible at 60 Hz.
    public float BallX = 0.5f, BallY = 0.5f;
    public float BallVx, BallVy;
    public float P0Y = 0.5f, P1Y = 0.5f;
    public int Score0, Score1;
    public uint Tick;
    public bool Ended;
    public int Winner = -1;

    // Local paddle command applied each render frame; host writes own
    // paddle directly here, guest writes its predicted paddle here and
    // also sends an InputPacket to the host.
    public float LocalPaddleY = 0.5f;

    /// <param name="localServiceAddr">This player's own Ensemble
    ///   service address (<see cref="QueueHandle{TPayload}.PlayerServiceAddress"/>).
    ///   MUST be the player-service address, NOT the node identity: host
    ///   election compares it against the peer's service address, so both
    ///   sides have to order the same pair of service addresses or both elect
    ///   Host.</param>
    public MatchSession(QueueHandle<PongPayload> handle, MatchFound match, string localServiceAddr)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(match);
        if (match.Peers.Count == 0)
            throw new ArgumentException("MatchFound has no peers", nameof(match));

        _handle = handle;
        _peerAddr = match.Peers[0].EnsembleAddr;

        // Alphabetical-host: of the two PLAYER-SERVICE addresses, the lower
        // sorts first → host. Both sides compare the same service-addr pair, so
        // exactly one elects Host with no coordination round.
        Authority = string.CompareOrdinal(localServiceAddr, _peerAddr) < 0 ? Role.Host : Role.Guest;
    }

    public void Start()
    {
        // Reader runs in both roles.
        _readerTask = Task.Run(ReadLoopAsync);

        if (Authority == Role.Host)
        {
            // Kick the ball off in a random-ish direction.
            var rng = new Random();
            var sign = rng.Next(0, 2) == 0 ? -1f : 1f;
            BallVx = sign * BallSpeed;
            BallVy = (float)(rng.NextDouble() - 0.5) * BallSpeed * 0.6f;
            _simTask = Task.Run(SimLoopAsync);
        }
    }

    /// <summary>Scene-side: apply local paddle input. Host moves P0; guest moves P1 and sends an InputPacket.</summary>
    public void SetLocalPaddleY(float y)
    {
        y = Math.Clamp(y, PaddleHalfHeight, 1f - PaddleHalfHeight);
        LocalPaddleY = y;
        if (Authority == Role.Host) P0Y = y; else P1Y = y;
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            await foreach (var msg in _handle.PeerMessages(_cts.Token).ConfigureAwait(false))
            {
                if (msg.FromAddr != _peerAddr) continue;
                GameMessage env;
                try { env = GameMessage.Parser.ParseFrom(msg.Bytes.Span); } catch { continue; }
                switch (env.MsgCase)
                {
                    case GameMessage.MsgOneofCase.State when Authority == Role.Guest:
                        BallX = env.State.BallX;
                        BallY = env.State.BallY;
                        BallVx = env.State.BallVx;
                        BallVy = env.State.BallVy;
                        P0Y = env.State.P0Y;
                        // Guest trusts host for P1Y too (server reconciliation).
                        P1Y = env.State.P1Y;
                        Score0 = env.State.Score0;
                        Score1 = env.State.Score1;
                        Tick = env.State.Tick;
                        break;
                    case GameMessage.MsgOneofCase.Input when Authority == Role.Host:
                        P1Y = Math.Clamp(env.Input.PaddleY, PaddleHalfHeight, 1f - PaddleHalfHeight);
                        break;
                    case GameMessage.MsgOneofCase.End:
                        Ended = true;
                        Winner = env.End.Winner;
                        Score0 = env.End.Score0;
                        Score1 = env.End.Score1;
                        break;
                }
            }
        }
        catch (OperationCanceledException) { /* dispose */ }
    }

    private async Task SimLoopAsync()
    {
        var dt = 1f / SimTickHz;
        var sendEvery = (int)Math.Round(SimTickHz / SendHz);
        var t = 0;
        var rng = new Random();
        var period = TimeSpan.FromSeconds(dt);
        while (!_cts.IsCancellationRequested && !Ended)
        {
            BallX += BallVx * dt;
            BallY += BallVy * dt;

            // Top / bottom walls — bounce on ball EDGE, not center, so the ball
            // doesn't visually clip past the rail before reversing. Math.Abs
            // also guards against double-reversal if a fast ball overshoots.
            if (BallY - BallHalfH <= 0f) { BallY = BallHalfH; BallVy = Math.Abs(BallVy); }
            else if (BallY + BallHalfH >= 1f) { BallY = 1f - BallHalfH; BallVy = -Math.Abs(BallVy); }

            // Host paddle (P0) collision — leading edge of the ball must reach
            // the paddle's inner X, and the ball's Y extent must overlap the
            // paddle's Y extent (expanded by ball half-height so a graze counts).
            if (BallVx < 0 && BallX - BallHalfW <= PaddleSurfaceXHost && Math.Abs(BallY - P0Y) <= PaddleHalfHeight + BallHalfH)
            {
                BallX = PaddleSurfaceXHost + BallHalfW;
                BallVx = -BallVx;
                BallVy += (BallY - P0Y) * BallSpeed * 0.5f;
            }
            // Guest paddle (P1) collision — mirror image.
            else if (BallVx > 0 && BallX + BallHalfW >= PaddleSurfaceXGuest && Math.Abs(BallY - P1Y) <= PaddleHalfHeight + BallHalfH)
            {
                BallX = PaddleSurfaceXGuest - BallHalfW;
                BallVx = -BallVx;
                BallVy += (BallY - P1Y) * BallSpeed * 0.5f;
            }

            // No left/right walls — these are scoring zones. A miss past either
            // paddle (ball passes the surface without satisfying the Y overlap
            // above) sails into BallX < 0 or > 1 and gives the other side a point.
            if (BallX < 0f) { Score1++; ResetBall(rng, servingTo: +1); }
            else if (BallX > 1f) { Score0++; ResetBall(rng, servingTo: -1); }

            // End-of-match
            if (Score0 >= ScoreCap || Score1 >= ScoreCap)
            {
                Ended = true;
                Winner = Score0 >= ScoreCap ? 0 : 1;
                await TrySendAsync(new GameMessage { End = new MatchEnd { Winner = Winner, Score0 = Score0, Score1 = Score1 } }).ConfigureAwait(false);
                break;
            }

            Tick++;
            if (++t >= sendEvery)
            {
                t = 0;
                await TrySendStateAsync().ConfigureAwait(false);
            }

            await Task.Delay(period, _cts.Token).ConfigureAwait(false);
        }
    }

    private void ResetBall(Random rng, int servingTo)
    {
        BallX = 0.5f;
        BallY = 0.5f;
        BallVx = servingTo * BallSpeed;
        BallVy = (float)(rng.NextDouble() - 0.5) * BallSpeed * 0.4f;
    }

    private Task TrySendStateAsync() =>
        TrySendAsync(new GameMessage
        {
            State = new MatchStatePacket
            {
                BallX = BallX, BallY = BallY, BallVx = BallVx, BallVy = BallVy,
                P0Y = P0Y, P1Y = P1Y, Score0 = Score0, Score1 = Score1, Tick = Tick,
            },
        });

    /// <summary>Guest-side: notify host of paddle move. No-op if host.</summary>
    public Task SendInputAsync() =>
        Authority == Role.Guest
            ? TrySendAsync(new GameMessage { Input = new InputPacket { PaddleY = LocalPaddleY, Tick = Tick } })
            : Task.CompletedTask;

    private async Task TrySendAsync(GameMessage envelope)
    {
        try
        {
            await _handle.SendToPeerAsync(_peerAddr, envelope.ToByteArray(), _cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { /* dispose */ }
        catch (Exception ex)
        {
            // Best-effort at game time — one dropped frame is fine, so we don't
            // rethrow. But log the FIRST failure: a persistently broken peer
            // link (e.g. the connection was never authorized) would otherwise
            // be completely invisible behind this catch.
            if (Interlocked.Exchange(ref _sendErrorLogged, 1) == 0)
                GD.PrintErr($"[MatchSession] send to peer {_peerAddr} failed (further failures suppressed): {ex}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try { _cts.Cancel(); } catch { /* best-effort */ }
        try { if (_simTask is not null) await _simTask.ConfigureAwait(false); } catch { }
        try { if (_readerTask is not null) await _readerTask.ConfigureAwait(false); } catch { }
        _cts.Dispose();
    }
}
