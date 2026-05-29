# PUG.Redis

Concrete Redis implementations of the [`PUG.Core`](../PUG.Core) interfaces.

## Why ZSET, not list?

`RedisQueue<TTicket>` stores tickets in a Redis ZSET (sorted set), with the
score being the ticket's `EnqueuedAt` in Unix milliseconds and the member
being the player id. A side hash holds the JSON payload keyed by the same
member.

A naïve "list of JSON" pattern would force `O(N)` deserialisation for every
queue operation that needs to make a time-based decision: timeout sweeps,
"oldest N" peeks, and index-by-id removes all turn into linear scans of the
list, parsing every element to read its `EnqueuedAt`.

ZSETs let us issue `ZRANGEBYSCORE` for time-window operations in `O(log N +
window-size)`, `ZRANGE` for "oldest N" in `O(log N + N)` without parsing
anything along the way, and `ZREM` for indexed removes in `O(log N)` by
member key. Pairing the ZSET (the ordering and identity layer) with a side
hash (the payload layer) means we never deserialise to make ordering
decisions, and a side-hash lookup is itself `O(1)`. Enqueue and remove are
wrapped in `MULTI/EXEC` so the two keys stay consistent under concurrent
writers.

## Running the integration tests

The Testcontainers-based suite needs a container runtime. On boxsie:

```bash
systemctl --user start podman.socket
export DOCKER_HOST="unix:///run/user/$UID/podman/podman.sock"
export TESTCONTAINERS_RYUK_DISABLED=true
dotnet test tests/PUG.Redis.Tests
```

Unit-only runs (no containers) filter the integration category out:

```bash
dotnet test --filter "Category!=Integration"
```
