using System;
using System.Threading.Tasks;
using Godot;

namespace PugPong.Client;

public partial class Match : Control
{
    private ColorRect _paddle0 = null!;
    private ColorRect _paddle1 = null!;
    private ColorRect _ball = null!;
    private Label _score0 = null!;
    private Label _score1 = null!;
    private Label _status = null!;

    private MatchSession? _session;
    private float _paddleSpeed = 1.2f; // 0..1 units per second
    private bool _exitScheduled;

    private const float ViewportW = 960f;
    private const float ViewportH = 540f;
    private const float PaddleW = 10f;
    private const float PaddleH = MatchSession.PaddleHalfHeight * 2f * ViewportH;
    private const float BallSize = 12f;

    public override void _Ready()
    {
        _paddle0 = GetNode<ColorRect>("Paddle0");
        _paddle1 = GetNode<ColorRect>("Paddle1");
        _ball = GetNode<ColorRect>("Ball");
        _score0 = GetNode<Label>("Score0");
        _score1 = GetNode<Label>("Score1");
        _status = GetNode<Label>("Status");

        var handle = SceneRouting.Handle ?? throw new InvalidOperationException("Match scene entered with no handle");
        var match = SceneRouting.Match ?? throw new InvalidOperationException("Match scene entered with no MatchFound");

        // Host election compares service addresses on both sides — pass our
        // player-service address (NOT SceneRouting.LocalAddr, which is the node
        // identity and would make the comparison asymmetric).
        _session = new MatchSession(handle, match, handle.PlayerServiceAddress);
        _session.Start();
        _status.Text = _session.Authority == MatchSession.Role.Host ? "host (left paddle)" : "guest (right paddle)";
    }

    public override void _Process(double delta)
    {
        if (_session is null) return;

        // Paddle input: up arrow / W decrements y, down / S increments. Always
        // moves the LOCAL player's paddle: host owns left, guest owns right.
        float dir = 0f;
        if (Input.IsKeyPressed(Key.Up) || Input.IsKeyPressed(Key.W)) dir -= 1f;
        if (Input.IsKeyPressed(Key.Down) || Input.IsKeyPressed(Key.S)) dir += 1f;
        if (dir != 0f)
        {
            var newY = _session.LocalPaddleY + dir * _paddleSpeed * (float)delta;
            _session.SetLocalPaddleY(newY);
            if (_session.Authority == MatchSession.Role.Guest)
                _ = _session.SendInputAsync();
        }

        // Render — pull state from session.
        _paddle0.Position = new Vector2(20f - PaddleW * 0.5f, _session.P0Y * ViewportH - PaddleH * 0.5f);
        _paddle1.Position = new Vector2(940f - PaddleW * 0.5f, _session.P1Y * ViewportH - PaddleH * 0.5f);
        _ball.Position = new Vector2(_session.BallX * ViewportW - BallSize * 0.5f, _session.BallY * ViewportH - BallSize * 0.5f);
        _score0.Text = _session.Score0.ToString();
        _score1.Text = _session.Score1.ToString();

        if (_session.Ended && !_exitScheduled)
        {
            _exitScheduled = true;
            var localIsHost = _session.Authority == MatchSession.Role.Host;
            var iWon = (localIsHost && _session.Winner == 0) || (!localIsHost && _session.Winner == 1);
            _status.Text = iWon ? "You win!" : "You lose";
            _ = ReturnToSplashAfterDelay();
        }
    }

    private async Task ReturnToSplashAfterDelay()
    {
        await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
        CallDeferred(MethodName.GoToSplash);
    }

    private void GoToSplash()
    {
        SceneRouting.Match = null;
        // Dispose the handle here — Match scene was the final consumer.
        var handle = SceneRouting.Handle;
        SceneRouting.Handle = null;
        _ = TeardownAsync(handle);
        GetTree().ChangeSceneToFile("res://Scenes/Splash.tscn");
    }

    private async Task TeardownAsync(PUG.Ensemble.QueueHandle<PugPong.Proto.PongPayload>? handle)
    {
        if (_session is not null) await _session.DisposeAsync().ConfigureAwait(false);
        if (handle is not null) await handle.DisposeAsync().ConfigureAwait(false);
    }

    public override void _ExitTree()
    {
        _ = _session?.DisposeAsync();
    }
}
