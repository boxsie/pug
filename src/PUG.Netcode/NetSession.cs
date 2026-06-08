namespace PUG.Netcode;

/// <summary>
/// The topology + authority seam between Tier A (links/channels) and Tier B
/// (entity replication). A session owns <b>1..N</b> <see cref="IPeerLink"/>s, each
/// wrapped in its own A1 <see cref="ChannelMux"/>, plus the single fact that makes
/// client / listen-server / dedicated-server differ: <b>who is authoritative.</b>
///
/// <para>
/// <b>LAW: authority is a designation, not an election.</b> The core never picks a
/// host. Whoever constructs the session states the authority — a dedicated server
/// is authoritative by config; a P2P host is elected <i>above</i> this layer (in
/// the Ensemble/matchmaking glue, e.g. lowest service address wins) and the result
/// is injected here. That keeps the same code path usable for a fixed-authority
/// dedicated server with zero P2P assumptions baked in.
/// </para>
///
/// <para><b>Three shapes, one type:</b></para>
/// <list type="bullet">
/// <item><b>Client</b> — one link to the authority; <see cref="IsAuthority"/> is
///   false; <see cref="AuthorityPeer"/> resolves to that one remote (id
///   <see cref="PeerId.Authority"/>). <see cref="SelfId"/> is the client's own
///   assigned id, injected by the glue that set up the match.</item>
/// <item><b>Listen-server / P2P host</b> and <b>dedicated server</b> — N client
///   links; <see cref="IsAuthority"/> is true; <see cref="SelfId"/> is
///   <see cref="PeerId.Authority"/>; each attached client is assigned a stable
///   <see cref="PeerId"/> from 1 up.</item>
/// </list>
///
/// <para>
/// <b>Peer ids.</b> The authority assigns each client a session-stable
/// <see cref="PeerId"/> (1, 2, 3…) as it attaches; itself it is
/// <see cref="PeerId.Authority"/> (0). Tier B3 puts that id on the wire as an
/// entity's <c>owner</c> so a client can find "mine". Tier B1/B2 don't need ids
/// (per-link sender attribution via <see cref="DrainInto"/> is enough), which is
/// why the id scheme can ride here without blocking them. A client learns its own
/// id by injection for now; B3 adds the authoritative wire assignment.
/// </para>
///
/// <para>
/// <b>Scope.</b> The 1-link 1v1 path is the one that runs on Ensemble today and is
/// the one fully exercised. The N-link server path is structurally present —
/// broadcast over a list, per-link attribution, dynamic <see cref="AttachClient"/>
/// — but a socket-backed <see cref="IPeerLink"/> and real server orchestration are
/// a later "bring a socket link and use the N-link shape" job, not a refactor.
/// </para>
///
/// <para>
/// <b>Ownership.</b> Unlike <see cref="ChannelMux"/> (which leaves its link alone),
/// the session is the top of the stack: it owns the links it is handed and disposes
/// them along with their muxes. Dispose the session and the whole peer set tears
/// down.
/// </para>
/// </summary>
public sealed class NetSession : IAsyncDisposable
{
    private readonly ChannelSpec[] _channels;
    private readonly NetDiagnostics? _diagnostics;
    private readonly object _gate = new();
    private readonly List<NetPeer> _peers = new();
    private byte _nextClientId = 1;
    private int _disposed;

    private NetSession(bool isAuthority, PeerId selfId, IEnumerable<ChannelSpec> channels, NetDiagnostics? diagnostics)
    {
        ArgumentNullException.ThrowIfNull(channels);

        _channels = channels.ToArray();
        if (_channels.Length == 0)
        {
            throw new ArgumentException("At least one channel is required.", nameof(channels));
        }

        IsAuthority = isAuthority;
        SelfId = selfId;
        _diagnostics = diagnostics;
    }

    /// <summary>True when this session is the authoritative end — it simulates the
    /// world and broadcasts state. False on a client, which receives state and is
    /// corrected by the authority.</summary>
    public bool IsAuthority { get; }

    /// <summary>This session's own id. <see cref="PeerId.Authority"/> on the
    /// authority; the injected client id on a client.</summary>
    public PeerId SelfId { get; }

    /// <summary>The connected remote peers, each with its own A1 mux. One element
    /// on a client (the authority); N on a server. A point-in-time copy — safe to
    /// enumerate while peers attach.</summary>
    public IReadOnlyList<NetPeer> Peers
    {
        get
        {
            lock (_gate)
            {
                return _peers.ToArray();
            }
        }
    }

    /// <summary>On a client, the single remote peer that is the authority; on the
    /// authority itself, <c>null</c> (it has no remote authority — it is one).</summary>
    public NetPeer? AuthorityPeer
    {
        get
        {
            if (IsAuthority)
            {
                return null;
            }

            lock (_gate)
            {
                return _peers.Count > 0 ? _peers[0] : null;
            }
        }
    }

    /// <summary>
    /// Build a <b>client</b> session: one link to the authority. The session is not
    /// authoritative; its single peer is the authority (id
    /// <see cref="PeerId.Authority"/>). <paramref name="selfId"/> is this client's
    /// own assigned id — injected by the match glue that already knows the topology
    /// (the same place that decided this end is a guest, not the host).
    /// </summary>
    /// <param name="authorityLink">The link to the server/host. Owned by the
    ///   session from here on.</param>
    /// <param name="channels">The channel set, declared identically on both ends.</param>
    /// <param name="selfId">This client's id (≥ 1; must not be the reserved
    ///   authority id).</param>
    /// <param name="authorityLabel">Diagnostics label for the authority peer.</param>
    /// <param name="diagnostics">Optional sink the session registers its mux with
    ///   and logs link state to.</param>
    public static NetSession CreateClient(
        IPeerLink authorityLink,
        IEnumerable<ChannelSpec> channels,
        PeerId selfId,
        string? authorityLabel = null,
        NetDiagnostics? diagnostics = null)
    {
        ArgumentNullException.ThrowIfNull(authorityLink);
        if (selfId.IsAuthority)
        {
            throw new ArgumentException("A client's self id must not be the reserved authority id (0).", nameof(selfId));
        }

        var session = new NetSession(isAuthority: false, selfId, channels, diagnostics);
        session.Attach(authorityLink, PeerId.Authority, authorityLabel ?? "authority");
        session._diagnostics?.Info($"client session up: self={selfId}, authority link attached");
        return session;
    }

    /// <summary>
    /// Build an <b>authority</b> session (listen-server, P2P host, or dedicated
    /// server). It is authoritative and its <see cref="SelfId"/> is
    /// <see cref="PeerId.Authority"/>. Start with zero or more client links;
    /// <see cref="AttachClient"/> adds more as they join. Each client is assigned a
    /// stable <see cref="PeerId"/> from 1 up in attach order.
    /// </summary>
    /// <param name="channels">The channel set, declared identically on both ends.</param>
    /// <param name="initialClients">Client links present at construction (may be
    ///   empty). Owned by the session.</param>
    /// <param name="diagnostics">Optional sink the session registers each mux with
    ///   and logs link state to.</param>
    public static NetSession CreateAuthority(
        IEnumerable<ChannelSpec> channels,
        IEnumerable<IPeerLink>? initialClients = null,
        NetDiagnostics? diagnostics = null)
    {
        var session = new NetSession(isAuthority: true, PeerId.Authority, channels, diagnostics);
        session._diagnostics?.Info("authority session up");
        if (initialClients is not null)
        {
            foreach (var link in initialClients)
            {
                session.AttachClient(link);
            }
        }

        return session;
    }

    /// <summary>
    /// Attach a newly-connected client link to an <b>authority</b> session,
    /// assigning it the next stable <see cref="PeerId"/> (1, 2, 3…) and returning
    /// the attached peer. The session owns the link.
    /// </summary>
    /// <param name="clientLink">The client's link. Owned by the session.</param>
    /// <param name="label">Diagnostics label; defaults to the assigned id.</param>
    /// <exception cref="InvalidOperationException">Called on a non-authority
    ///   session — clients don't attach peers, they have exactly one (the
    ///   authority), set at construction.</exception>
    public NetPeer AttachClient(IPeerLink clientLink, string? label = null)
    {
        ArgumentNullException.ThrowIfNull(clientLink);
        if (!IsAuthority)
        {
            throw new InvalidOperationException("Only an authority session attaches clients.");
        }

        PeerId id;
        lock (_gate)
        {
            id = new PeerId(_nextClientId++);
        }

        var peer = Attach(clientLink, id, label ?? id.ToString());
        _diagnostics?.Info($"client attached: {id} (now {Peers.Count} link(s))");
        return peer;
    }

    /// <summary>
    /// Send a payload on <paramref name="channelId"/> to <b>every</b> connected
    /// peer. The Tier B snapshot fan-out: on a client (one peer) it sends to the
    /// authority; on the authority it broadcasts to all clients.
    /// </summary>
    public async ValueTask BroadcastAsync(byte channelId, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        foreach (var peer in Peers)
        {
            await peer.Mux.SendAsync(channelId, payload, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Send a payload on <paramref name="channelId"/> to one peer by id — an
    /// authority replying to a specific client, say.
    /// </summary>
    /// <exception cref="ArgumentException">No peer with that id is attached.</exception>
    public ValueTask SendToAsync(PeerId target, byte channelId, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        NetPeer? match = null;
        foreach (var peer in Peers)
        {
            if (peer.Id == target)
            {
                match = peer;
                break;
            }
        }

        if (match is null)
        {
            throw new ArgumentException($"No attached peer with id {target}.", nameof(target));
        }

        return match.Mux.SendAsync(channelId, payload, ct);
    }

    /// <summary>
    /// Send a payload on <paramref name="channelId"/> to the authority — the client
    /// path (input up, events up). Equivalent to <see cref="BroadcastAsync"/> at
    /// N=1 but reads for what it is.
    /// </summary>
    /// <exception cref="InvalidOperationException">Called on an authority session,
    ///   which has no upstream authority.</exception>
    public ValueTask SendToAuthorityAsync(byte channelId, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var authority = AuthorityPeer;
        if (authority is null)
        {
            throw new InvalidOperationException("An authority session has no upstream authority to send to.");
        }

        return authority.Mux.SendAsync(channelId, payload, ct);
    }

    /// <summary>
    /// Drain every ready payload on <paramref name="channelId"/> from <b>all</b>
    /// peers into <paramref name="sink"/>, each tagged with the
    /// <see cref="PeerId"/> it came from, and return how many were added. This is
    /// how an authority attributes inbound input/events to the client that sent
    /// them; on a client it drains the authority's traffic tagged
    /// <see cref="PeerId.Authority"/>.
    /// </summary>
    public int DrainInto(byte channelId, ICollection<PeerInbound> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        var count = 0;
        foreach (var peer in Peers)
        {
            while (peer.Mux.TryReceive(channelId, out var payload))
            {
                sink.Add(new PeerInbound(peer.Id, payload));
                count++;
            }
        }

        return count;
    }

    private NetPeer Attach(IPeerLink link, PeerId id, string label)
    {
        var mux = new ChannelMux(link, _channels);
        var peer = new NetPeer(id, link, mux, label);
        lock (_gate)
        {
            _peers.Add(peer);
        }

        _diagnostics?.RegisterMux(peer.Label, mux);
        return peer;
    }

    /// <summary>
    /// Tear the session down: dispose every peer's mux (stopping its drain), then
    /// the underlying links the session owns.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        NetPeer[] peers;
        lock (_gate)
        {
            peers = _peers.ToArray();
            _peers.Clear();
        }

        foreach (var peer in peers)
        {
            await peer.Mux.DisposeAsync().ConfigureAwait(false);
        }

        foreach (var peer in peers)
        {
            await peer.Link.DisposeAsync().ConfigureAwait(false);
        }
    }
}

/// <summary>
/// One connected peer in a <see cref="NetSession"/>: its session-stable
/// <see cref="Id"/> and its A1 <see cref="ChannelMux"/>. Tier B sends and drains
/// through <see cref="Mux"/>; the underlying link is the session's to own.
/// </summary>
public sealed class NetPeer
{
    internal NetPeer(PeerId id, IPeerLink link, ChannelMux mux, string label)
    {
        Id = id;
        Link = link;
        Mux = mux;
        Label = label;
    }

    /// <summary>The peer's session-stable id. <see cref="PeerId.Authority"/> when
    /// this peer is the authority (the remote on a client session).</summary>
    public PeerId Id { get; }

    /// <summary>The channel mux carrying this peer's traffic.</summary>
    public ChannelMux Mux { get; }

    /// <summary>The diagnostics label this peer was attached under.</summary>
    public string Label { get; }

    internal IPeerLink Link { get; }
}

/// <summary>
/// One inbound payload tagged with the peer it arrived from, as produced by
/// <see cref="NetSession.DrainInto"/>. The attribution an authority needs to know
/// <i>which</i> client an input belongs to.
/// </summary>
/// <param name="From">The peer the payload came from.</param>
/// <param name="Payload">The channel payload (header already stripped by the mux).</param>
public readonly record struct PeerInbound(PeerId From, ReadOnlyMemory<byte> Payload);
