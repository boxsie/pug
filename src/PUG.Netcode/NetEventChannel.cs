using System.Buffers.Binary;

namespace PUG.Netcode;

/// <summary>
/// Tier B's <b>event track</b>: discrete, tick-stamped game events that ride the
/// A1 <see cref="ChannelMode.Ordered"/> channel — delivered reliably and in send
/// order, never dropped. This is the answer to "a missed tick held something
/// crucial we can't smooth away": a goal scored, a shot fired, a death, a pickup.
/// Such a thing was never continuous state — you can't interpolate <i>to</i> it and
/// can't afford to lose it — so it does not belong in the lossy snapshot stream
/// (<see cref="NetworkReplicator"/>). The game chooses the track per datum: "ball
/// position" → snapshot, "goal scored" → event.
///
/// <para>
/// <b>Opaque + tick-stamped.</b> The payload is bytes PUG never interprets; each
/// event carries the tick it happened on so the game can apply it consistently
/// with the snapshot stream (e.g. a death event lined up against the snapshot where
/// the entity despawns).
/// </para>
///
/// <para>
/// <b>Direction-agnostic.</b> Routing is <see cref="NetSession"/>'s: an authority
/// <see cref="BroadcastAsync"/>es to every client; a client
/// <see cref="SendToAuthorityAsync"/>s up ("I fired"). On receive, the link
/// identifies the sender (<see cref="GameEvent.From"/>) — no peer-id needed, which
/// is why this is independent of B3 and parallel with B1.
/// </para>
///
/// <para><b>Pumped</b> — the game sends inline and <see cref="Drain"/>s received
/// events once per frame.</para>
/// </summary>
public sealed class NetEventChannel
{
    /// <summary>Event header: the u32 tick stamp prefixed to every payload.</summary>
    private const int TickHeaderBytes = 4;

    private readonly NetSession _session;
    private readonly byte _channel;
    private readonly List<PeerInbound> _scratch = new();

    private long _eventsSent;
    private long _eventsReceived;

    /// <summary>
    /// Wrap <paramref name="session"/>'s <paramref name="eventChannel"/> for
    /// discrete events. Declare that channel <see cref="ChannelMode.Ordered"/> on
    /// both ends — the reliability/ordering guarantee is what makes an event safe
    /// to send exactly once.
    /// </summary>
    public NetEventChannel(NetSession session, byte eventChannel)
    {
        ArgumentNullException.ThrowIfNull(session);
        _session = session;
        _channel = eventChannel;
    }

    /// <summary>Cumulative events sent / received.</summary>
    public NetEventStats Stats => new(_eventsSent, _eventsReceived);

    /// <summary>
    /// (Authority) Broadcast an event stamped <paramref name="tick"/> to every
    /// client link. The frame is built before any await, so passing a stack
    /// <paramref name="payload"/> is fine.
    /// </summary>
    public ValueTask BroadcastAsync(uint tick, ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        var frame = Frame(tick, payload);
        _eventsSent++;
        return _session.BroadcastAsync(_channel, frame, ct);
    }

    /// <summary>
    /// (Client) Send an event stamped <paramref name="tick"/> up to the authority.
    /// </summary>
    public ValueTask SendToAuthorityAsync(uint tick, ReadOnlySpan<byte> payload, CancellationToken ct = default)
    {
        var frame = Frame(tick, payload);
        _eventsSent++;
        return _session.SendToAuthorityAsync(_channel, frame, ct);
    }

    /// <summary>
    /// Drain every event received since the last call into <paramref name="sink"/>,
    /// in delivery order (which the Ordered channel guarantees equals send order),
    /// each tagged with its sender and tick stamp. Returns how many were added.
    /// </summary>
    public int Drain(ICollection<GameEvent> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        _scratch.Clear();
        _session.DrainInto(_channel, _scratch);

        var added = 0;
        foreach (var inbound in _scratch)
        {
            if (inbound.Payload.Length < TickHeaderBytes)
            {
                continue; // too short to carry a tick stamp — not one of ours; skip
            }

            var tick = BinaryPrimitives.ReadUInt32BigEndian(inbound.Payload.Span);
            sink.Add(new GameEvent(inbound.From, tick, inbound.Payload.Slice(TickHeaderBytes)));
            _eventsReceived++;
            added++;
        }

        return added;
    }

    private static byte[] Frame(uint tick, ReadOnlySpan<byte> payload)
    {
        var frame = new byte[TickHeaderBytes + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, tick);
        payload.CopyTo(frame.AsSpan(TickHeaderBytes));
        return frame;
    }
}

/// <summary>
/// One received discrete event: who sent it, the tick it happened on, and its
/// opaque payload (the tick header already stripped).
/// </summary>
/// <param name="From">The sending peer (link identity; <see cref="PeerId.Authority"/>
///   for an authority broadcast).</param>
/// <param name="Tick">The tick the sender stamped the event with.</param>
/// <param name="Payload">The game's opaque event bytes.</param>
public readonly record struct GameEvent(PeerId From, uint Tick, ReadOnlyMemory<byte> Payload);

/// <summary>Cumulative counters for a <see cref="NetEventChannel"/>.</summary>
/// <param name="EventsSent">Events sent.</param>
/// <param name="EventsReceived">Events drained to the game.</param>
public readonly record struct NetEventStats(long EventsSent, long EventsReceived);
