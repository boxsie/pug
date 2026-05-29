using System;
using System.Threading;
using System.Threading.Tasks;
using EnsembleNS = Ensemble.Client;
using Godot;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using PugPong.Proto;
using PUG.Ensemble;
using SysEnv = System.Environment;

namespace PugPong.Client;

/// <summary>
/// Autoload singleton at /root/EnsembleBridge wrapping EnsembleClient +
/// EnsemblePlayerClient. Configured via env vars: ENSEMBLE_GRPC_ADDR
/// (default http://127.0.0.1:9090) and PUGPONG_MATCHMAKER_ADDR (required —
/// the E-address the matchmaker process prints on start).
/// </summary>
public partial class EnsembleBridge : Node
{
    public static EnsembleBridge Instance { get; private set; } = null!;

    private EnsembleNS.EnsembleClient? _ensemble;
    private EnsemblePlayerClient? _player;
    private string _matchmakerAddr = string.Empty;
    private string _localAddr = string.Empty;

    public string MatchmakerAddr => _matchmakerAddr;
    public string LocalAddr => _localAddr;
    public bool IsReady => _ensemble is not null && _player is not null;

    public override void _Ready()
    {
        Instance = this;
    }

    /// <summary>
    /// Connect to the daemon and resolve our own E-address. Safe to call
    /// multiple times; subsequent calls return the existing connection.
    /// </summary>
    public async Task StartAsync(string? ensembleAddr = null, string? matchmakerAddr = null, CancellationToken ct = default)
    {
        if (IsReady) return;

        ensembleAddr ??= SysEnv.GetEnvironmentVariable("ENSEMBLE_GRPC_ADDR")
            ?? "http://127.0.0.1:9090";
        matchmakerAddr ??= SysEnv.GetEnvironmentVariable("PUGPONG_MATCHMAKER_ADDR")
            ?? throw new InvalidOperationException(
                "PUGPONG_MATCHMAKER_ADDR is required — set it to the E-address the matchmaker process printed on start.");

        _ensemble = new EnsembleNS.EnsembleClient(ensembleAddr);
        _player = new EnsemblePlayerClient(_ensemble, new GodotLogger<EnsemblePlayerClient>());
        _matchmakerAddr = matchmakerAddr;

        var identity = await _ensemble.GetIdentityAsync(ct).ConfigureAwait(false);
        _localAddr = identity.Address;
        GD.Print($"[EnsembleBridge] daemon={ensembleAddr} matchmaker={matchmakerAddr} self={_localAddr}");
    }

    public Task<QueueHandle<PongPayload>> JoinPublicAsync(PongPayload payload, CancellationToken ct = default)
    {
        Require();
        return _player!.JoinMatchmakingAsync(_matchmakerAddr, payload, p => p.ToByteArray(), ct: ct);
    }

    public Task<(string Code, QueueHandle<PongPayload> Handle)> CreatePrivateAsync(PongPayload payload, CancellationToken ct = default)
    {
        Require();
        return _player!.CreatePrivateMatchAsync(_matchmakerAddr, payload, p => p.ToByteArray(), ct);
    }

    public Task<QueueHandle<PongPayload>> JoinPrivateAsync(PongPayload payload, string code, CancellationToken ct = default)
    {
        Require();
        return _player!.JoinPrivateByCodeAsync(_matchmakerAddr, payload, code, p => p.ToByteArray(), ct);
    }

    /// <summary>
    /// Thin wrapper around <see cref="QueueHandle{TPayload}.WaitForMatchAsync"/>
    /// so scene code can pretend the bridge owns the wait. The handle still
    /// owns its own lifecycle.
    /// </summary>
    public Task<MatchFound> WaitForMatchAsync(QueueHandle<PongPayload> handle, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handle);
        return handle.WaitForMatchAsync(ct);
    }

    private void Require()
    {
        if (!IsReady)
            throw new InvalidOperationException("EnsembleBridge.StartAsync must be awaited before matchmaking calls.");
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // Fire-and-forget the async dispose — Godot's _ExitTree doesn't
            // give us an awaitable context. The daemon will see the gRPC
            // channel close on process exit even if this misses.
            _ = DisposeAsyncCore();
        }
        base.Dispose(disposing);
    }

    private async Task DisposeAsyncCore()
    {
        try
        {
            if (_player is not null) await _player.DisposeAsync().ConfigureAwait(false);
            if (_ensemble is not null) await _ensemble.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            GD.PrintErr($"[EnsembleBridge] dispose error: {ex.Message}");
        }
    }
}
