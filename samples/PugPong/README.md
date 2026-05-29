# PugPong sample

Two-player matchmaking + P2P pong built on **[PUG](../../)** and **[Ensemble](https://github.com/boxsie/ensemble)**. The matchmaker is a tiny .NET console app; gameplay runs peer-to-peer between two Godot 4 clients over the direct Ensemble link the matchmaker introduces.

## Architecture

```
  ┌────────────────┐                                   ┌────────────────┐
  │   Godot        │                                   │   Godot        │
  │   client A     │                                   │   client B     │
  └───────┬────────┘                                   └────────┬───────┘
          │ gRPC                                                │ gRPC
  ┌───────▼────────┐         ┌────────────────┐        ┌────────▼───────┐
  │  ensembled     │         │  ensembled     │        │  ensembled     │
  │  daemon A      │         │  daemon M      │        │  daemon B      │
  │  (:9091)       │         │  (:9090)       │        │  (:9092)       │
  └───────┬────────┘         │  ┌───────────┐ │        └────────┬───────┘
          │   loopback       │  │ matchmaker│ │   loopback      │
          │   signaling      │  │  service  │ │   signaling     │
          └─────────────────►│  │ (process) │ │◄────────────────┘
            (unix sockets)   │  └───────────┘ │  (unix sockets)
                             └────────────────┘

  The matchmaker runs on its OWN daemon (M). Each client has its own daemon
  (A, B). Both clients reach the matchmaker cross-daemon — no client shares a
  daemon with it, so neither silently takes a same-daemon local-delivery
  shortcut. (An earlier 2-daemon layout co-located the matchmaker with client
  A; that masked cross-daemon reply bugs on A's side and made "which client
  worked?" a coin-flip.)

  At match time the matchmaker introduces the two players via Ensemble's
  PeerIntroduction event. After that, the matchmaker is gone — the rally
  flows directly between client A and client B over QUIC.
```

## Prerequisites

- **.NET 8 SDK** (the repo targets net8/9/10; net8 is the floor)
- **Godot 4.3 mono** — `godot --version` should print `4.3.stable.mono...`
- **Docker or Podman** is optional — only used by the included `docker-compose.yml` to run a single daemon. The dev loop runs daemons as raw processes.

The demo uses Ensemble's `--signaling=loopback` backend (a Unix-socket rendezvous under `${XDG_RUNTIME_DIR:-/tmp}/ensemble-loopback/`), so **no Tor binary is required** for this same-host setup. Daemons boot in tens of milliseconds rather than ~60s of Tor bootstrap. See the Ensemble repo's signaling-backends docs if you want the WAN / LAN variants (`--signaling=tor` / `--signaling=mdns`).

## Quickstart (one-command)

If you just want to see it work:

```bash
cd samples/PugPong
./run/run-demo.sh
```

That brings up three daemons (matchmaker + one per client), the matchmaker process, and two Godot clients with the right env vars wired through. All runtime artefacts (daemon `--data-dir`s, logs, the matchmaker's E-address handoff file, a pids file for `stop.sh`) land in `run/output/` (gitignored). Ctrl-C tears it all down cleanly. If something orphans, `./run/stop.sh` reads `run/output/pids` and kills anything still around.

The script reads these env vars if set (defaults shown):

| Var | Default |
|---|---|
| `ENSEMBLE_BIN` | `../../../ensemble/bin/ensemble` (sibling repo) |
| `GODOT_BIN` | `godot` (must be on `PATH`) |
| `DAEMON_M_PORT` | `9090` (matchmaker daemon) |
| `DAEMON_A_PORT` / `DAEMON_B_PORT` | `9091` / `9092` (client daemons) |

## Quickstart (manual, step-by-step)

If you want to understand what the script does, or run pieces independently:

### 1. Build the Ensemble daemon

The daemons are an `ensembled` binary from the sibling Ensemble repo:

```bash
cd ../../../ensemble
make build                              # produces ./bin/ensemble
```

### 2. Start three daemons (matchmaker + one per Godot client)

The matchmaker gets its own daemon, and each Godot client gets its own. Co-locating the matchmaker with a client would let that client's traffic take the same-daemon local-delivery path, hiding cross-daemon bugs from half the demo — so keep them separate.

```bash
# Terminal M: matchmaker daemon on :9090
~/Documents/projects/ensemble/bin/ensemble \
    --headless \
    --signaling=loopback \
    --api-addr 127.0.0.1:9090 \
    --data-dir /tmp/pugpong-daemon-M

# Terminal A: daemon for client A on :9091
~/Documents/projects/ensemble/bin/ensemble \
    --headless \
    --signaling=loopback \
    --api-addr 127.0.0.1:9091 \
    --data-dir /tmp/pugpong-daemon-A

# Terminal B: daemon for client B on :9092
~/Documents/projects/ensemble/bin/ensemble \
    --headless \
    --signaling=loopback \
    --api-addr 127.0.0.1:9092 \
    --data-dir /tmp/pugpong-daemon-B
```

Daemons stand up in well under a second — the registry is ready as soon as gRPC is listening, so the matchmaker can register immediately.

### 3. Run the matchmaker against daemon M

```bash
cd samples/PugPong/Matchmaker
ENSEMBLE_GRPC_ADDR=http://127.0.0.1:9090 dotnet run
```

Copy the E-address it prints — it looks like `E1Hg7...` and starts with `E`. That's `PUGPONG_MATCHMAKER_ADDR` for both clients.

### 4. Launch two Godot clients

```bash
# Terminal C: client A against daemon A
ENSEMBLE_GRPC_ADDR=http://127.0.0.1:9091 \
PUGPONG_MATCHMAKER_ADDR=<matchmaker E-addr> \
    godot --path samples/PugPong/Client

# Terminal D: client B against daemon B
ENSEMBLE_GRPC_ADDR=http://127.0.0.1:9092 \
PUGPONG_MATCHMAKER_ADDR=<same matchmaker E-addr> \
    godot --path samples/PugPong/Client
```

Type a name on the splash screen of each. Click **Play** on both → they queue → the matchmaker pairs them → both transition to the Match scene and the rally starts.

### 5. Private matches

On client A, click **Create private match** instead of Play. The Lobby shows a short code (e.g. `ABCD23`). On client B's splash, type the code into the **Join** field and click Join. Same pairing flow.

## Controls + rules

- **Up / Down arrows** or **W / S** — move your paddle
- First to **5** points wins
- The player whose Ensemble E-address sorts lexicographically lower is the **host** (owns the simulation, controls the left paddle); the other is the **guest** (controls the right paddle, sends inputs to the host). Both sides reach the same role assignment deterministically with no coordination round.

## What PUG primitives this exercises

- **Matchmaking queue** — `MatchmakerServiceHost<PongPayload>` hosting `FifoMatcher` with 1v1 team sizes
- **Private match codes** — short-code creation + join-by-code, partitioned at the matcher so private and public players never cross-pair
- **Post-match P2P** — `QueueHandle<T>.SendToPeerAsync` and `PeerMessages` for the in-match state stream (added in T17)

The actual game-state protocol is in `Client/Proto/GameMessage.proto` — `MatchStatePacket` (host → guest, ~30 Hz), `InputPacket` (guest → host, on paddle move), `MatchEnd`. The matchmaker doesn't see any of it; it's gone after the introduction.

## Layout

```
samples/PugPong/
├── Matchmaker/             # .NET console app — runs the matchmaker service
│   ├── Program.cs          # wires FifoMatcher + InMemoryQueue; writes E-addr to PUGPONG_ADDR_FILE
│   ├── InMemoryQueue.cs    # single-process IQueue impl
│   ├── Proto/
│   │   └── PongPayload.proto   # per-player queue payload (linked into Client)
│   └── Dockerfile          # builds the matchmaker against the published Ensemble.Client NuGet package
├── Client/                 # Godot 4 mono project — the game
│   ├── project.godot
│   ├── PugPong.Client.csproj
│   ├── Scenes/
│   │   ├── Splash.tscn     # name + Play / Create / Join
│   │   ├── Lobby.tscn      # queue state / code display / cancel
│   │   └── Match.tscn      # paddles + ball + scores
│   ├── Scripts/
│   │   ├── EnsembleBridge.cs    # autoload singleton wrapping EnsembleClient + EnsemblePlayerClient
│   │   ├── SceneRouting.cs      # static handoff between scenes
│   │   ├── Splash.cs / Lobby.cs / Match.cs
│   │   └── MatchSession.cs      # alphabetical-host authority + physics + GameMessage routing
│   └── Proto/
│       └── GameMessage.proto    # in-match envelope
├── run/                    # one-command demo orchestration
│   ├── run-demo.sh         # daemons + matchmaker + two Godot clients, clean SIGINT teardown
│   ├── stop.sh             # kill anything orphaned (reads output/pids)
│   ├── .gitignore          # → output/
│   └── output/             # data dirs, logs, matchmaker.addr, pids — gitignored
├── docker-compose.yml      # single-daemon dev only (see note)
└── README.md               # this file
```

## Known friction

- **Each Godot client needs its own daemon.** Same-daemon `rpc.Service.Send` has no local fast path; two services on one daemon can't bridge bytes between themselves. Two daemons + loopback signaling is the supported topology for the same-host demo.
- **The `docker-compose.yml` builds the `ensembled` daemon image from a local Ensemble checkout.** It expects an `ensemble` checkout alongside this repo (`../../../ensemble`); clone it from [github.com/boxsie/ensemble](https://github.com/boxsie/ensemble) if you want the dockerised daemon. The host-side `dotnet run` dev loop needs only the `ensemble` binary on `ENSEMBLE_BIN`.

## Out of scope at v0.1

- Lockstep / deterministic simulation
- Reconnect after disconnect
- Spectator support
- Mobile / web targets
- Bots / backfill — the matchmaker has the seam but the sample doesn't exercise it
