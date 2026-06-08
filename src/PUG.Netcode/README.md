# PUG.Netcode

In-match netcode for PUG: the reusable layer that runs **after** matchmaking
hands two peers a connection. Where `PUG.Core` is matchmaking abstractions and
`PUG.Ensemble` is the transport adapter, `PUG.Netcode` is what turns a raw
peer-to-peer byte pipe into smooth, predicted, server-reconciled gameplay —
without knowing anything about your game.

## Transport-agnostic by design

`PUG.Netcode` is **ascetic** — zero dependencies outside the BCL, exactly like
`PUG.Core`. Everything is written against a single seam, `IPeerLink`: a duplex
byte pipe to one matched peer that *advertises what the underlying transport
guarantees* (reliable/ordered? RTT?). Today that link is Ensemble's QUIC/TLS
connection (via a `QueueHandle → IPeerLink` adapter in `PUG.Ensemble`); it could
be something else later. The netcode layer never assumes — it adapts to the
capabilities the link reports.

## Architecture (built in tiers)

| Tier | What it does |
|------|--------------|
| **Seam** — `IPeerLink` | Send/receive bytes to one peer; advertise transport guarantees. |
| **A — channels + clock** | Sequencing, acks, channel multiplexing (unreliable / sequenced / reliable-ordered), RTT/loss estimation, a shared fixed-timestep clock with peer clock-sync. |
| **B — entities** | `NetworkEntity` — id, owner, game-defined state blob. Game objects attach via write/apply-state adapters; authority ships tick-stamped snapshots. PUG never knows it's a paddle or a ball. |
| **C — smoothing & prediction** | Snapshot interpolation, client-side prediction of the owned entity, server reconciliation, optional lag compensation. |

A game opts in by tier: pure replication needs only write/apply state;
interpolation comes free on top; prediction additionally needs a
`Simulate(state, input, dt)` step.

> **Status:** scaffolding. The tiers above are being built one at a time, each
> independently testable over an in-memory `IPeerLink` before the next leans on
> it. See the PUG project board, phase *"Netcode layer (PUG.Netcode)"*.
