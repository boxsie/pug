using System;
using System.IO;
using System.Threading.Tasks;
using Godot;

namespace PugPong.Client;

public partial class Splash : Control
{
    private LineEdit _nameEdit = null!;
    private LineEdit _codeEdit = null!;
    private Button _playButton = null!;
    private Button _createButton = null!;
    private Button _joinButton = null!;
    private Label _status = null!;

    public override void _Ready()
    {
        _nameEdit = GetNode<LineEdit>("VBox/NameEdit");
        _codeEdit = GetNode<LineEdit>("VBox/JoinRow/CodeEdit");
        _playButton = GetNode<Button>("VBox/PlayButton");
        _createButton = GetNode<Button>("VBox/CreateButton");
        _joinButton = GetNode<Button>("VBox/JoinRow/JoinButton");
        _status = GetNode<Label>("VBox/Status");

        _nameEdit.Text = LoadPlayerName();
        _playButton.Pressed += () => Go(LobbyMode.Public, null);
        _createButton.Pressed += () => Go(LobbyMode.PrivateCreate, null);
        _joinButton.Pressed += () =>
        {
            if (string.IsNullOrWhiteSpace(_codeEdit.Text)) { _status.Text = "enter a code"; return; }
            Go(LobbyMode.PrivateJoin, _codeEdit.Text.Trim());
        };

        if (SceneRouting.LastError is { } err)
        {
            _status.Text = err;
            SceneRouting.LastError = null;
        }

        _ = ConnectAsync();
    }

    private async Task ConnectAsync()
    {
        try { await EnsembleBridge.Instance.StartAsync().ConfigureAwait(false); }
        catch (Exception ex) { CallDeferred(MethodName.SetStatus, $"daemon error: {ex.Message}"); }
    }

    private void SetStatus(string text) => _status.Text = text;

    private void Go(LobbyMode mode, string? code)
    {
        var name = string.IsNullOrWhiteSpace(_nameEdit.Text) ? "Player" : _nameEdit.Text.Trim();
        SavePlayerName(name);
        SceneRouting.Mode = mode;
        SceneRouting.PlayerName = name;
        SceneRouting.PrivateCode = code;
        GetTree().ChangeSceneToFile("res://Scenes/Lobby.tscn");
    }

    private static string PlayerNamePath() => Path.Combine(OS.GetUserDataDir(), "player_name.txt");

    private static string LoadPlayerName()
    {
        try { return File.Exists(PlayerNamePath()) ? File.ReadAllText(PlayerNamePath()).Trim() : "Player"; }
        catch { return "Player"; }
    }

    private static void SavePlayerName(string name)
    {
        try { File.WriteAllText(PlayerNamePath(), name); } catch { /* best-effort */ }
    }
}
