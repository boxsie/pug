using System;
using System.Threading;
using System.Threading.Tasks;
using Godot;
using PugPong.Proto;
using PUG.Ensemble;

namespace PugPong.Client;

public partial class Lobby : Control
{
    private Label _title = null!;
    private Label _status = null!;
    private Button _cancelButton = null!;

    private QueueHandle<PongPayload>? _handle;
    private CancellationTokenSource? _cts;

    public override void _Ready()
    {
        _title = GetNode<Label>("VBox/Title");
        _status = GetNode<Label>("VBox/Status");
        _cancelButton = GetNode<Button>("VBox/CancelButton");
        _cancelButton.Pressed += OnCancel;

        _ = RunAsync();
    }

    private async Task RunAsync()
    {
        _cts = new CancellationTokenSource();
        var payload = new PongPayload { PlayerName = SceneRouting.PlayerName, PreferredColor = "#ffaa00" };
        try
        {
            await EnsembleBridge.Instance.StartAsync(ct: _cts.Token).ConfigureAwait(false);
            SceneRouting.LocalAddr = EnsembleBridge.Instance.LocalAddr;

            switch (SceneRouting.Mode)
            {
                case LobbyMode.Public:
                    SetTitle("Looking for a match…");
                    _handle = await EnsembleBridge.Instance.JoinPublicAsync(payload, _cts.Token).ConfigureAwait(false);
                    break;
                case LobbyMode.PrivateCreate:
                    var (code, h) = await EnsembleBridge.Instance.CreatePrivateAsync(payload, _cts.Token).ConfigureAwait(false);
                    _handle = h;
                    SetTitle($"Code: {code} — share it");
                    break;
                case LobbyMode.PrivateJoin:
                    SetTitle($"Joining {SceneRouting.PrivateCode}…");
                    _handle = await EnsembleBridge.Instance.JoinPrivateAsync(payload, SceneRouting.PrivateCode!, _cts.Token).ConfigureAwait(false);
                    break;
            }

            SetStatus("queued — waiting for opponent");
            var match = await _handle!.WaitForMatchAsync(_cts.Token).ConfigureAwait(false);
            SceneRouting.Handle = _handle;
            SceneRouting.Match = match;
            CallDeferred(MethodName.GoToMatch);
        }
        catch (OperationCanceledException) { /* cancelled by user */ }
        catch (Exception ex)
        {
            GD.PrintErr($"[Lobby] queue error: {ex}");
            SceneRouting.LastError = ex.Message;
            CallDeferred(MethodName.GoToSplash);
        }
    }

    private void SetTitle(string text) => CallDeferred(MethodName.SetTitleDeferred, text);
    private void SetTitleDeferred(string text) => _title.Text = text;
    private void SetStatus(string text) => CallDeferred(MethodName.SetStatusDeferred, text);
    private void SetStatusDeferred(string text) => _status.Text = text;

    private void GoToMatch() => GetTree().ChangeSceneToFile("res://Scenes/Match.tscn");
    private void GoToSplash() => GetTree().ChangeSceneToFile("res://Scenes/Splash.tscn");

    private void OnCancel()
    {
        _cts?.Cancel();
        _ = DisposeHandleAsync();
        GetTree().ChangeSceneToFile("res://Scenes/Splash.tscn");
    }

    private async Task DisposeHandleAsync()
    {
        if (_handle is not null)
        {
            try { await _handle.DisposeAsync().ConfigureAwait(false); }
            catch { /* best-effort */ }
            _handle = null;
        }
    }

    public override void _ExitTree()
    {
        _cts?.Cancel();
        // Don't dispose the handle here — if we successfully matched, Match.tscn owns it.
        // The handle is in SceneRouting.Handle iff we matched; otherwise we already disposed via OnCancel.
    }
}
