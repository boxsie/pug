# PUG.Ensemble

Matchmaker-specific glue layered on top of [`Ensemble.Client`][ec], the .NET
SDK for the Ensemble peer-to-peer messaging daemon. This project ships:

- `MatchmakerServiceHost<TPayload>` — the server side: hosts a `PUG.Core`
  matcher as an Ensemble service, runs the queue/match loop, and introduces
  paired players to each other.
- `EnsemblePlayerClient` — the player side: join matchmaking, create/join a
  private match by short code, and await a `MatchFound`.
- `QueueHandle<TPayload>` — post-match peer-to-peer messaging
  (`SendToPeerAsync` / `PeerMessages`) once players are introduced.
- The matchmaker RPC envelope (`Proto/MatchmakerRpc.proto`).

`PUG.Ensemble` deliberately does **not** vendor `ensemble.proto`. That proto
is `Ensemble.Client`'s job: it generates the daemon's gRPC stubs and exposes
typed wrappers (`EnsembleClient`, `RegisteredService`, `ServiceEvent`, etc.)
which `PUG.Ensemble` consumes as a normal NuGet/project dependency.

## Matchmaker RPC

`Proto/MatchmakerRpc.proto` defines two top-level envelopes —
`MatchmakerRequest` and `MatchmakerResponse` — each carrying a `oneof` of
case-specific message types: `JoinQueueRequest`, `QueuedResponse`,
`QueueStatus`, `ErrorResponse`, plus the private-match flow
(`CreatePrivateMatchRequest`, `JoinPrivateByCodeRequest`,
`PrivateMatchCreated`). Messages are serialised to bytes and shipped over
Ensemble's service-transport `SendBytesAsync`; we do **not** generate gRPC
service stubs (`GrpcServices="None"`).

Generated C# lives under `obj/Debug/net8.0/Proto/` with the
`PUG.Ensemble.Proto` namespace.

## Ensemble.Client dependency

`PUG.Ensemble.csproj` references `Ensemble.Client` as a normal NuGet
`PackageReference`. The integration test project
(`tests/PUG.Ensemble.Tests`) drives a real daemon via the
`EnsembleDaemonHarness` shipped inside that same package — point it at an
`ensemble` binary with the `ENSEMBLE_BIN` environment variable, and the
daemon-backed tests skip automatically when no binary is available.

[ec]: https://github.com/boxsie/ensemble/tree/main/clients/dotnet/Ensemble.Client
