using System.Buffers;
using System.Buffers.Binary;

namespace PUG.Netcode;

/// <summary>
/// Tier B's <b>state track</b>: mirror the authoritative world to every peer as a
/// stream of <b>full-world snapshots</b> on the KeepLatest channel. The replicator
/// does not simulate (the game does) or smooth (Tier C does) — it serializes what
/// the authority holds and reconstructs it on each client by set-diff.
///
/// <para><b>One class, role from <see cref="NetSession"/>:</b></para>
/// <list type="bullet">
/// <item><b>Authority</b> — <see cref="Register(INetEntityState, PeerId)"/> an
///   <see cref="INetEntityState"/> (gets a stable id), <see cref="Despawn"/> it, and
///   once per snapshot tick call
///   <see cref="CaptureAndBroadcastAsync"/> to serialize every registered entity
///   and broadcast it to all links.</item>
/// <item><b>Client</b> — call <see cref="Apply"/> each frame to drain the channel,
///   decode the newest snapshot, and set-diff it against what's held: an id not
///   seen before is <b>spawned</b> via the game's factory, a known id has its state
///   applied (through the <see cref="ISnapshotApplyStrategy"/>), and an id absent
///   from the snapshot is <b>despawned</b>. Spawn and despawn fall out of the diff —
///   there is no separate reliable spawn/despawn protocol to lose.</item>
/// </list>
///
/// <para>
/// <b>Why full-world is the correctness model, not the byte count.</b> Because
/// KeepLatest already discards stale snapshots, a lost or reordered snapshot simply
/// skips to a newer <i>complete</i> world — there is no "spawn packet lost ⇒ ghost
/// forever" failure mode to engineer around. Size is controlled orthogonally
/// (field quantization in <see cref="INetEntityState.WriteState"/>; snapshot rate
/// below sim rate; later per-peer relevancy culling), none of which breaks the
/// self-healing property.
/// </para>
///
/// <para>
/// <b>Pumped, single-threaded</b> like the clock it serves — the game drives both
/// roles from its frame callback; no background thread, no locking.
/// </para>
/// </summary>
public sealed class NetworkReplicator
{
    /// <summary>Per-entity snapshot header: id(2) + kind(1) + owner(1) + stateLen(2).</summary>
    private const int EntityHeaderBytes = 6;

    /// <summary>Snapshot header: tick(4) + count(2).</summary>
    private const int SnapshotHeaderBytes = 6;

    private readonly NetSession _session;
    private readonly byte _channel;

    // Authority-only state.
    private readonly SortedDictionary<ushort, Registered> _registry = new();
    private readonly ArrayBufferWriter<byte> _snapshot = new();
    private readonly ArrayBufferWriter<byte> _scratch = new();
    private ushort _nextEntityId = 1; // 0 stays free as a "none" sentinel

    // Client-only state.
    private readonly ISnapshotApplyStrategy _apply;
    private readonly Func<byte, INetEntityState>? _spawn;
    private readonly Action<ushort, INetEntityState>? _onDespawn;
    private readonly Func<uint>? _localTick;
    private readonly Dictionary<ushort, ReplicatedEntity> _entities = new();
    private readonly List<PeerInbound> _incoming = new();
    private readonly HashSet<ushort> _seen = new();
    private readonly List<ushort> _toRemove = new();

    // Shared counters (pumped from one thread; no interlock needed).
    private long _snapshotsProcessed;
    private long _spawns;
    private long _despawns;
    private long _malformed;
    private int _lastSnapshotBytes;
    private uint _lastSnapshotTick;

    private NetworkReplicator(
        NetSession session,
        byte channel,
        ISnapshotApplyStrategy apply,
        Func<byte, INetEntityState>? spawn,
        Action<ushort, INetEntityState>? onDespawn,
        Func<uint>? localTick)
    {
        _session = session;
        _channel = channel;
        _apply = apply;
        _spawn = spawn;
        _onDespawn = onDespawn;
        _localTick = localTick;
    }

    /// <summary>True on the authority (serializes + broadcasts); false on a client
    /// (drains + reconstructs).</summary>
    public bool IsAuthority => _session.IsAuthority;

    /// <summary>The client's reconstructed world, keyed by entity id. Empty on the
    /// authority (which holds its own registry, not a replica).</summary>
    public IReadOnlyDictionary<ushort, ReplicatedEntity> Entities => _entities;

    /// <summary>
    /// (Client) The reconstructed entities controlled by <paramref name="owner"/> —
    /// pass <see cref="NetSession.SelfId"/> to find "mine". A client predicts its own
    /// entities (Tier C) and interpolates the rest; the world/server's entities carry
    /// <see cref="PeerId.Authority"/> and are owned by nobody to predict.
    /// </summary>
    public IEnumerable<ReplicatedEntity> EntitiesOwnedBy(PeerId owner)
    {
        foreach (var entity in _entities.Values)
        {
            if (entity.Owner == owner)
            {
                yield return entity;
            }
        }
    }

    /// <summary>
    /// Build the <b>authority</b> replicator over an authoritative
    /// <paramref name="session"/>. It serializes every registered entity into the
    /// snapshot stream on <paramref name="snapshotChannel"/> (declare it
    /// <see cref="ChannelMode.KeepLatest"/>).
    /// </summary>
    public static NetworkReplicator CreateAuthority(NetSession session, byte snapshotChannel)
    {
        ArgumentNullException.ThrowIfNull(session);
        if (!session.IsAuthority)
        {
            throw new ArgumentException("CreateAuthority needs an authoritative session.", nameof(session));
        }

        return new NetworkReplicator(session, snapshotChannel, new ImmediateApply(), spawn: null, onDespawn: null, localTick: null);
    }

    /// <summary>
    /// Build a <b>client</b> replicator over a client <paramref name="session"/>.
    /// New entity ids are constructed via <paramref name="spawn"/> (archetype →
    /// object); state is applied through <paramref name="applyStrategy"/>
    /// (defaults to <see cref="ImmediateApply"/>); vanished entities are handed to
    /// <paramref name="onDespawn"/> so the game can tear down its view.
    /// </summary>
    /// <param name="session">A non-authoritative session.</param>
    /// <param name="snapshotChannel">The KeepLatest channel snapshots arrive on.</param>
    /// <param name="spawn">Factory: archetype kind → a fresh
    ///   <see cref="INetEntityState"/> to receive that entity's state.</param>
    /// <param name="applyStrategy">How authoritative state lands on an entity;
    ///   <see cref="ImmediateApply"/> if null (the Tier C insertion point).</param>
    /// <param name="onDespawn">Optional: invoked with (id, entity) when an entity
    ///   disappears from the world, so the game can dispose its object.</param>
    /// <param name="localTick">Optional local-tick source for the snapshot-age
    ///   diagnostic (local tick − snapshot tick). Age reads 0 without it.</param>
    public static NetworkReplicator CreateClient(
        NetSession session,
        byte snapshotChannel,
        Func<byte, INetEntityState> spawn,
        ISnapshotApplyStrategy? applyStrategy = null,
        Action<ushort, INetEntityState>? onDespawn = null,
        Func<uint>? localTick = null)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(spawn);
        if (session.IsAuthority)
        {
            throw new ArgumentException("CreateClient needs a non-authoritative session.", nameof(session));
        }

        return new NetworkReplicator(session, snapshotChannel, applyStrategy ?? new ImmediateApply(), spawn, onDespawn, localTick);
    }

    /// <summary>
    /// (Authority) Register a world/server-owned <paramref name="entity"/> for
    /// replication and return its new stable id — shorthand for
    /// <see cref="Register(INetEntityState, PeerId)"/> with
    /// <see cref="PeerId.Authority"/>. Use this for the ball, AI, scenery: things no
    /// client controls.
    /// </summary>
    public ushort Register(INetEntityState entity) => Register(entity, PeerId.Authority);

    /// <summary>
    /// (Authority) Register <paramref name="entity"/> for replication under a
    /// controlling <paramref name="owner"/> and return its new stable id. The owner
    /// is written into every snapshot's per-entity <c>owner</c> byte; a client reads
    /// it back and compares with its own <see cref="NetSession.SelfId"/> to find the
    /// entity it controls (<see cref="EntitiesOwnedBy"/>). Pass
    /// <see cref="PeerId.Authority"/> for world-owned entities, a client's
    /// <see cref="PeerId"/> for one it drives. The entity stays in every snapshot
    /// until <see cref="Despawn"/>.
    /// </summary>
    public ushort Register(INetEntityState entity, PeerId owner)
    {
        ArgumentNullException.ThrowIfNull(entity);
        EnsureAuthority();

        var id = _nextEntityId++;
        _registry[id] = new Registered(entity, owner);
        return id;
    }

    /// <summary>(Authority) Remove an entity from replication; it disappears from
    /// the next snapshot and clients despawn it by set-diff.</summary>
    public bool Despawn(ushort id)
    {
        EnsureAuthority();
        return _registry.Remove(id);
    }

    /// <summary>
    /// (Authority) Serialize a full-world snapshot stamped <paramref name="tick"/>
    /// (the authority's <see cref="TickClock.CurrentTick"/> truncated to u32) and
    /// broadcast it to every link on the snapshot channel.
    /// </summary>
    public ValueTask CaptureAndBroadcastAsync(uint tick, CancellationToken ct = default)
    {
        EnsureAuthority();

        _snapshot.Clear();
        var header = _snapshot.GetSpan(SnapshotHeaderBytes);
        BinaryPrimitives.WriteUInt32BigEndian(header, tick);
        BinaryPrimitives.WriteUInt16BigEndian(header.Slice(4), (ushort)_registry.Count);
        _snapshot.Advance(SnapshotHeaderBytes);

        foreach (var (id, reg) in _registry)
        {
            _scratch.Clear();
            reg.Entity.WriteState(_scratch);
            var state = _scratch.WrittenSpan;
            if (state.Length > ushort.MaxValue)
            {
                throw new InvalidOperationException($"Entity {id} state {state.Length} B exceeds the {ushort.MaxValue} B per-entity cap.");
            }

            var eh = _snapshot.GetSpan(EntityHeaderBytes);
            BinaryPrimitives.WriteUInt16BigEndian(eh, id);
            eh[2] = reg.Entity.Kind;
            eh[3] = reg.Owner.Value;
            BinaryPrimitives.WriteUInt16BigEndian(eh.Slice(4), (ushort)state.Length);
            _snapshot.Advance(EntityHeaderBytes);

            var dst = _snapshot.GetSpan(state.Length);
            state.CopyTo(dst);
            _snapshot.Advance(state.Length);
        }

        _lastSnapshotBytes = _snapshot.WrittenCount;
        _lastSnapshotTick = tick;
        _snapshotsProcessed++;

        return _session.BroadcastAsync(_channel, _snapshot.WrittenMemory, ct);
    }

    /// <summary>
    /// (Client) Drain the snapshot channel, decode the newest snapshot, and
    /// reconcile the world against it (spawn / apply / despawn). Older snapshots in
    /// the queue are skipped — KeepLatest means the freshest complete world wins.
    /// Returns the number of entities the applied snapshot carried, or 0 if none
    /// arrived.
    /// </summary>
    public int Apply()
    {
        EnsureClient();

        _incoming.Clear();
        _session.DrainInto(_channel, _incoming);
        if (_incoming.Count == 0)
        {
            return 0;
        }

        // The client has a single peer (the authority) and KeepLatest delivers in
        // forward order, so the last item drained is the newest world.
        return ApplySnapshot(_incoming[_incoming.Count - 1].Payload.Span);
    }

    private int ApplySnapshot(ReadOnlySpan<byte> snapshot)
    {
        if (snapshot.Length < SnapshotHeaderBytes)
        {
            _malformed++;
            return 0;
        }

        var tick = BinaryPrimitives.ReadUInt32BigEndian(snapshot);
        var count = BinaryPrimitives.ReadUInt16BigEndian(snapshot.Slice(4));

        _seen.Clear();
        var offset = SnapshotHeaderBytes;
        for (var i = 0; i < count; i++)
        {
            if (offset + EntityHeaderBytes > snapshot.Length)
            {
                _malformed++;
                return 0;
            }

            var id = BinaryPrimitives.ReadUInt16BigEndian(snapshot.Slice(offset));
            var kind = snapshot[offset + 2];
            var owner = new PeerId(snapshot[offset + 3]);
            var stateLen = BinaryPrimitives.ReadUInt16BigEndian(snapshot.Slice(offset + 4));
            offset += EntityHeaderBytes;

            if (offset + stateLen > snapshot.Length)
            {
                _malformed++;
                return 0;
            }

            var state = snapshot.Slice(offset, stateLen);
            offset += stateLen;
            _seen.Add(id);

            if (_entities.TryGetValue(id, out var existing))
            {
                _apply.Apply(existing.State, tick, state);
            }
            else
            {
                var instance = _spawn!(kind);
                _entities[id] = new ReplicatedEntity(id, kind, owner, instance);
                _apply.Apply(instance, tick, state);
                _spawns++;
            }
        }

        DespawnVanished();
        _lastSnapshotTick = tick;
        _lastSnapshotBytes = snapshot.Length;
        _snapshotsProcessed++;
        return count;
    }

    private void DespawnVanished()
    {
        _toRemove.Clear();
        foreach (var id in _entities.Keys)
        {
            if (!_seen.Contains(id))
            {
                _toRemove.Add(id);
            }
        }

        foreach (var id in _toRemove)
        {
            var removed = _entities[id];
            _entities.Remove(id);
            _despawns++;
            _onDespawn?.Invoke(removed.Id, removed.State);
        }
    }

    /// <summary>A pull-model snapshot of this replicator's counters, for
    /// <see cref="NetDiagnostics"/> or a debug overlay.</summary>
    public ReplicatorStats Stats
    {
        get
        {
            var entityCount = IsAuthority ? _registry.Count : _entities.Count;
            long age = 0;
            if (!IsAuthority && _localTick is not null)
            {
                // uint subtraction wraps cleanly; local tick normally leads the snapshot.
                age = _localTick() - _lastSnapshotTick;
            }

            return new ReplicatorStats(
                IsAuthority,
                entityCount,
                _snapshotsProcessed,
                _lastSnapshotBytes,
                _lastSnapshotTick,
                age,
                _spawns,
                _despawns,
                _malformed);
        }
    }

    private void EnsureAuthority()
    {
        if (!IsAuthority)
        {
            throw new InvalidOperationException("This operation is authority-only.");
        }
    }

    private void EnsureClient()
    {
        if (IsAuthority)
        {
            throw new InvalidOperationException("This operation is client-only.");
        }
    }

    private readonly record struct Registered(INetEntityState Entity, PeerId Owner);
}

/// <summary>
/// One entity in a client's reconstructed world: its id, archetype
/// <see cref="Kind"/>, <see cref="Owner"/> (the controlling peer —
/// <see cref="PeerId.Authority"/> for world/server-owned, a client's
/// <see cref="PeerId"/> for one that client drives), and the game's
/// <see cref="INetEntityState"/> object that snapshots are applied to.
/// </summary>
/// <param name="Id">The authority-assigned entity id.</param>
/// <param name="Kind">The archetype the entity was spawned from.</param>
/// <param name="Owner">The controlling peer.</param>
/// <param name="State">The game object receiving replicated state.</param>
public readonly record struct ReplicatedEntity(ushort Id, byte Kind, PeerId Owner, INetEntityState State);

/// <summary>
/// A <see cref="NetworkReplicator"/>'s counters. The authority reports entities
/// registered / snapshots sent / last snapshot bytes; a client additionally
/// reports spawns, despawns, and snapshot age (how many ticks behind the authority
/// the last applied world is).
/// </summary>
/// <param name="IsAuthority">Which role produced these counters.</param>
/// <param name="EntityCount">Registered entities (authority) or replicated
///   entities (client).</param>
/// <param name="SnapshotsProcessed">Snapshots broadcast (authority) or applied
///   (client).</param>
/// <param name="LastSnapshotBytes">Size of the most recent snapshot payload.</param>
/// <param name="LastSnapshotTick">Tick stamped on the most recent snapshot.</param>
/// <param name="SnapshotAgeTicks">Client only: local tick − last snapshot tick (0
///   on the authority or without a local-tick source).</param>
/// <param name="Spawns">Client only: entities spawned by set-diff.</param>
/// <param name="Despawns">Client only: entities despawned by set-diff.</param>
/// <param name="MalformedSnapshots">Snapshots rejected as malformed.</param>
public readonly record struct ReplicatorStats(
    bool IsAuthority,
    int EntityCount,
    long SnapshotsProcessed,
    int LastSnapshotBytes,
    uint LastSnapshotTick,
    long SnapshotAgeTicks,
    long Spawns,
    long Despawns,
    long MalformedSnapshots);
