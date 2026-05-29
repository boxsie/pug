using PugPong.Proto;
using PUG.Ensemble;

namespace PugPong.Client;

/// <summary>Mode the Lobby scene should drive on entry.</summary>
public enum LobbyMode { Public, PrivateCreate, PrivateJoin }

/// <summary>
/// Static handoff between scenes. Set by the previous scene before
/// <c>ChangeSceneToFile</c>, read by the next scene in its <c>_Ready</c>.
/// One pending value at a time — there's only ever one scene "in flight".
/// </summary>
public static class SceneRouting
{
    public static LobbyMode Mode;
    public static string PlayerName = "Player";
    public static string? PrivateCode;
    public static QueueHandle<PongPayload>? Handle;
    public static MatchFound? Match;
    public static string LocalAddr = string.Empty;
    public static string? LastError;
}
