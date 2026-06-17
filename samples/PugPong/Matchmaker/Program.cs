using Ensemble.Client;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using PUG.Core;
using PUG.Ensemble;
using PugPong.Matchmaker;
using PugPong.Proto;

// Daemon-supervised when ENSEMBLE_SOCKET is set (ADR-0009 §5): the daemon spawns
// us and brokers our connection over its socket with a per-spawn token, and tells
// us the installed service name. Otherwise fall back to a hardcoded gRPC addr —
// the local Godot demo (run/run-demo.sh) runs us as a plain sidecar process.
var supervised = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ENSEMBLE_SOCKET"));
var ensembleAddr = Environment.GetEnvironmentVariable("ENSEMBLE_GRPC_ADDR") ?? "http://localhost:9090";
var serviceName = Environment.GetEnvironmentVariable("ENSEMBLE_SERVICE_NAME")
    ?? Environment.GetEnvironmentVariable("PUGPONG_SERVICE_NAME") ?? "pug-pong-matchmaker";

using var loggerFactory = LoggerFactory.Create(b => b.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; }));
var log = loggerFactory.CreateLogger("pugpong");

await using var ensemble = supervised
    ? EnsembleClient.FromEnv(loggerFactory.CreateLogger<EnsembleClient>())
    : new EnsembleClient(ensembleAddr, loggerFactory.CreateLogger<EnsembleClient>());

var queue = new InMemoryQueue<Ticket<PongPayload>>();
var matcher = new FifoMatcher<Ticket<PongPayload>>(queue, new[] { 1, 1 });
var options = new MatchmakerOptions<PongPayload>(
    ServiceName: serviceName,
    TeamSizes: new[] { 1, 1 },
    MaxPayloadBytes: 4096,
    RateLimitPerMinute: 60,
    RateLimitBurst: 10,
    // The introduced-peer authorization must outlast the WHOLE match, not just
    // the dial. Over Tor the player-service onion descriptors take ~30-60s to
    // publish before the peers can connect at all, so the SDK's 30s default
    // expires the introduction before the P2P link forms — the host then
    // rejects the guest's input channel as "non-introduced" and the guest goes
    // inert. 1h comfortably covers Tor setup + a full match; the per-match
    // player service is torn down at match end anyway, so this is the real bound.
    IntroductionExpiry: TimeSpan.FromHours(1),
    SerializePayload: p => p.ToByteArray(),
    DeserializePayload: PongPayload.Parser.ParseFrom);

await using var host = new MatchmakerServiceHost<PongPayload>(
    ensemble, matcher, queue, options, loggerFactory.CreateLogger<MatchmakerServiceHost<PongPayload>>());
await host.StartAsync();

log.LogInformation("pug-pong-matchmaker up: addr={Addr} onion={Onion}", host.ServiceAddress, host.Onion);

// Demo-orchestration handoff: if PUGPONG_ADDR_FILE is set, write the matchmaker's
// E-address there so run/run-demo.sh can pick it up without scraping logs. The
// script polls for this file to exist + be non-empty before launching clients.
if (Environment.GetEnvironmentVariable("PUGPONG_ADDR_FILE") is { Length: > 0 } addrFile)
{
    try { File.WriteAllText(addrFile, host.ServiceAddress); }
    catch (Exception ex) { log.LogWarning("could not write PUGPONG_ADDR_FILE={Path}: {Msg}", addrFile, ex.Message); }
}

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdown.Cancel(); };
AppDomain.CurrentDomain.ProcessExit += (_, _) => shutdown.Cancel();

// Periodic queue-stats line for demo observability.
var stats = Task.Run(async () =>
{
    while (!shutdown.IsCancellationRequested)
    {
        try { await Task.Delay(TimeSpan.FromSeconds(10), shutdown.Token); }
        catch (OperationCanceledException) { break; }
        var n = await queue.CountAsync(shutdown.Token);
        log.LogInformation("queue depth: {Count}", n);
    }
});

try { await Task.Delay(Timeout.Infinite, shutdown.Token); }
catch (OperationCanceledException) { }
log.LogInformation("shutting down");
await stats;
