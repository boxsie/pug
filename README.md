# PUG

**PUG (Pick-Up Group)** is a .NET matchmaking library: queues, matchers,
private lobbies, and session handoff for multiplayer games and services. The
core is dependency-free and generic over your own ticket/payload types; concrete
storage and transport adapters live in separate packages so you only pull in
what you use.

## Built on Ensemble

[Ensemble](https://github.com/boxsie/ensemble) is a fully decentralized
peer-to-peer network: no central server, anonymous peer discovery over Tor (your
IP stays off the network), and direct QUIC/TLS 1.3 connections established only
after mutual consent. Identity is a Bitcoin-style cryptographic keypair — no
accounts, no sign-up — and the daemon handles NAT traversal for you.

That maps cleanly onto matchmaking. With `PUG.Ensemble`, the matchmaker runs as
an Ensemble **service** with its own address; players reach it peer-to-peer, and
once it pairs them it **introduces** the two players directly to each other.
After the introduction the matchmaker drops out of the path — gameplay traffic
flows directly between the matched peers over their Ensemble link, with no relay
and no game server to run. PUG's core stays transport-agnostic, so Ensemble is
one host option, not a hard requirement (you can drive the queue/matcher
yourself, or back them with `PUG.Redis`).

## Packages

| Package | Targets | What it is |
|---|---|---|
| `PUG.Core` | net8.0 / net9.0 / net10.0 | The abstractions and in-memory implementations: `IQueue<T>`, `IMatcher<T>`, `FifoMatcher<T>`, `IPrivateLobby` + `InMemoryPrivateLobby`, `ISessionStore<T>`, `IBackfillProvider<T>`, `IDistributedLock`, `Ticket<TPayload>`, `MatchResult<T>`, `ShortCodeGenerator`. No infrastructure dependencies. |
| `PUG.Redis` | net8.0 | Redis-backed `IQueue`, `IDistributedLock`, and `ISessionStore` implementations via StackExchange.Redis. The queue is a ZSET + side hash so time-window and oldest-N operations stay `O(log N)` without deserialising payloads. See [`src/PUG.Redis/README.md`](src/PUG.Redis/README.md). |
| `PUG.Ensemble` | net8.0 | Adapter that runs a matchmaker as a service on the [Ensemble](https://github.com/boxsie/ensemble) peer-to-peer network: `MatchmakerServiceHost`, `EnsemblePlayerClient`, private-lobby-by-code. Depends on the `Ensemble.Client` NuGet package. See [`src/PUG.Ensemble/README.md`](src/PUG.Ensemble/README.md). |

## Design

PUG separates three concerns most matchmakers tangle together:

- **The queue** (`IQueue<T>`) — where tickets wait, ordered by enqueue time, with timeout sweeps and indexed removal.
- **The matcher** (`IMatcher<T>`) — the policy that pulls eligible tickets and forms teams. `FifoMatcher` is the reference implementation (1vN team sizes, public/private partitioning).
- **The transport/host** — how players reach the matchmaker and how a formed match is handed off. `PUG.Ensemble` is one such host; the core has no opinion.

Everything is generic over a ticket payload, so the queue never deserialises a
payload to make an ordering decision and your match-specific data rides along
untouched.

## Build & test

```bash
dotnet build PUG.sln
dotnet test PUG.sln
```

`PUG.Redis` and `PUG.Ensemble` have integration tests that need external
infrastructure (a Redis/Podman container, and an `ensemble` daemon binary
respectively). Those tests skip automatically when the dependency isn't
present; to run them, see each package's README. To run only the
infrastructure-free tests:

```bash
dotnet test --filter "Category!=Integration"
```

## Sample: PugPong

[`samples/PugPong`](samples/PugPong) is a complete two-player matchmaking + P2P
pong game — a Godot 4 client plus a `PUG.Ensemble` matchmaker — that exercises
the queue, private-match codes, and post-match peer-to-peer messaging end to
end. See its [README](samples/PugPong/README.md) for the one-command demo.

## License

MIT — see [LICENSE](LICENSE).
