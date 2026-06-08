using System.Buffers.Binary;

namespace PUG.Netcode;

/// <summary>
/// Tier B3's <b>input track</b>: opaque, tick-stamped input blobs that ride a
/// KeepLatest channel <i>up</i> from each client to the authority. This is the
/// upstream half of the authoritative loop — B1 sends authoritative state down,
/// this sends the input the authority needs to simulate the clients.
///
/// <para>
/// <b>Latest-wins, by design.</b> Input goes on a <see cref="ChannelMode.KeepLatest"/>
/// channel: a dropped or reordered input tick simply yields to a newer one, and the
/// authority always acts on each peer's freshest intent. The authority does not
/// queue history — it keeps the <b>latest input per peer</b> and the game sim reads
/// it for that peer's owned entities each tick. (Tier C's reconciliation may later
/// want an Ordered/buffered input stream for exact replay; that is a Tier C decision
/// — B3 ships the simple KeepLatest form.)
/// </para>
///
/// <para>
/// <b>Attribution is the link's.</b> A client <see cref="SendToAuthorityAsync"/>s up;
/// the authority <see cref="Drain"/>s and the receiving link identifies the sender
/// (<see cref="NetSession.DrainInto"/> tags each payload with its <see cref="PeerId"/>),
/// so input needs no sender id on the wire. The peer-id matters for the <i>owner</i>
/// tag a client reads on the way down (<see cref="NetworkReplicator"/>), not here.
/// </para>
///
/// <para>
/// <b>Opaque + tick-stamped.</b> The payload is bytes PUG never interprets
/// (buttons/axes are the game's business); each input carries the tick the client
/// sampled it on so the authority — and later Tier C reconciliation — can line it up
/// with the snapshot stream.
/// </para>
///
/// <para><b>Pumped</b> — the client sends inline each tick; the authority
/// <see cref="Drain"/>s once per frame and reads <see cref="TryGetLatest"/>.</para>
/// </summary>
public sealed class NetInputChannel
{
    /// <summary>Input header: the u32 tick stamp prefixed to every payload.</summary>
    private const int TickHeaderBytes = 4;

    private readonly NetSession _session;
    private readonly byte _channel;
    private readonly List<PeerInbound> _scratch = new();
    private readonly Dictionary<PeerId, PlayerInput> _latest = new();

    private long _inputsSent;
    private long _inputsReceived;

    /// <summary>
    /// Wrap <paramref name="session"/>'s <paramref name="inputChannel"/> for client
    /// input. Declare that channel <see cref="ChannelMode.KeepLatest"/> on both ends
    /// — latest-wins is what makes a dropped input tick harmless.
    /// </summary>
    public NetInputChannel(NetSession session, byte inputChannel)
    {
        ArgumentNullException.ThrowIfNull(session);
        _session = session;
        _channel = inputChannel;
    }

    /// <summary>Cumulative inputs sent (client) / received (authority, before
    /// the latest-per-peer fold).</summary>
    public NetInputStats Stats => new(_inputsSent, _inputsReceived);

    /// <summary>The authority's current latest-known input per peer, keyed by the
    /// peer it came from. Empty on a client. A point-in-time view the sim reads
    /// after <see cref="Drain"/>.</summary>
    public IReadOnlyDictionary<PeerId, PlayerInput> Latest => _latest;

    /// <summary>
    /// (Client) Send this client's input stamped <paramref name="tick"/> up to the
    /// authority. The frame is built before any await, so passing a stack
    /// <paramref name="input"/> is fine.
    /// </summary>
    /// <exception cref="InvalidOperationException">Called on an authority session,
    ///   which has no upstream authority to send input to.</exception>
    public ValueTask SendToAuthorityAsync(uint tick, ReadOnlySpan<byte> input, CancellationToken ct = default)
    {
        var frame = Frame(tick, input);
        _inputsSent++;
        return _session.SendToAuthorityAsync(_channel, frame, ct);
    }

    /// <summary>
    /// (Authority) Drain every input that arrived since the last call and fold it
    /// into <see cref="Latest"/>, keeping the newest tick per peer. Returns how many
    /// peers' latest input was updated. The sim then reads each owned entity's
    /// controller via <see cref="TryGetLatest"/>.
    /// </summary>
    public int Drain()
    {
        _scratch.Clear();
        _session.DrainInto(_channel, _scratch);

        var updated = 0;
        foreach (var inbound in _scratch)
        {
            if (inbound.Payload.Length < TickHeaderBytes)
            {
                continue; // too short to carry a tick stamp — not one of ours; skip
            }

            var tick = BinaryPrimitives.ReadUInt32BigEndian(inbound.Payload.Span);
            _inputsReceived++;

            // Keep only the freshest per peer. KeepLatest already drops stale-by-seq
            // at the mux, but fold by tick too so a same-frame batch resolves to the
            // newest intent (wrap-aware compare).
            if (_latest.TryGetValue(inbound.From, out var existing) && !IsNewer(tick, existing.Tick))
            {
                continue;
            }

            _latest[inbound.From] = new PlayerInput(inbound.From, tick, inbound.Payload.Slice(TickHeaderBytes));
            updated++;
        }

        return updated;
    }

    /// <summary>
    /// (Authority) Get <paramref name="peer"/>'s latest known input, if any has
    /// arrived. The input is held across frames — it stays the last value until a
    /// newer one lands — so the sim always has something to apply.
    /// </summary>
    public bool TryGetLatest(PeerId peer, out PlayerInput input) => _latest.TryGetValue(peer, out input);

    /// <summary>True if <paramref name="candidate"/> is a strictly later tick than
    /// <paramref name="current"/>, accounting for u32 wraparound.</summary>
    private static bool IsNewer(uint candidate, uint current) => (int)(candidate - current) > 0;

    private static byte[] Frame(uint tick, ReadOnlySpan<byte> input)
    {
        var frame = new byte[TickHeaderBytes + input.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, tick);
        input.CopyTo(frame.AsSpan(TickHeaderBytes));
        return frame;
    }
}

/// <summary>
/// One peer's latest input as the authority holds it: who it came from, the tick
/// the client sampled it on, and the opaque input bytes (the tick header already
/// stripped).
/// </summary>
/// <param name="Peer">The peer that sent the input (link identity).</param>
/// <param name="Tick">The tick the client stamped the input with.</param>
/// <param name="Payload">The game's opaque input bytes (buttons/axes).</param>
public readonly record struct PlayerInput(PeerId Peer, uint Tick, ReadOnlyMemory<byte> Payload);

/// <summary>Cumulative counters for a <see cref="NetInputChannel"/>.</summary>
/// <param name="InputsSent">Inputs sent up by a client.</param>
/// <param name="InputsReceived">Inputs the authority drained (before the
///   latest-per-peer fold).</param>
public readonly record struct NetInputStats(long InputsSent, long InputsReceived);
