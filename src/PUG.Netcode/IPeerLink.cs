namespace PUG.Netcode;

/// <summary>
/// The single transport seam the entire netcode layer is built on. Models a
/// duplex byte pipe to <b>one</b> matched peer: send bytes, receive bytes, and
/// advertise what the underlying transport guarantees. Everything above this
/// interface — channels, entity replication, prediction, smoothing — is written
/// against <see cref="IPeerLink"/> and is therefore transport-agnostic.
///
/// <para>
/// Implementations adapt a concrete transport. <c>PUG.Ensemble</c> wraps a
/// matched <c>QueueHandle</c>'s post-match P2P pipe; tests use the in-memory
/// <c>FakePeerLink</c> loopback. The netcode layer never assumes a delivery
/// guarantee — it reads <see cref="Capabilities"/> and layers on only the
/// reliability/sequencing the transport doesn't already provide.
/// </para>
///
/// <para>
/// <b>Single-peer by design.</b> One link is one peer; an N-peer mesh is N
/// links. Channels, sequence numbers, acks, and tick timing are <i>not</i>
/// modelled here — those are tier-A concerns that compose on top.
/// </para>
/// </summary>
public interface IPeerLink : IAsyncDisposable
{
    /// <summary>
    /// What the underlying transport guarantees about delivery and framing.
    /// Static traits of the link — not a live metric. Live values like RTT and
    /// loss are derived by the tier-A channel layer from its own acks, not read
    /// from here.
    /// </summary>
    PeerLinkCapabilities Capabilities { get; }

    /// <summary>
    /// Send one payload to the peer. The implementation takes ownership of the
    /// bytes for the duration of the call only — callers MAY reuse
    /// <paramref name="payload"/>'s backing buffer once the returned
    /// <see cref="ValueTask"/> completes (implementations copy if they defer).
    ///
    /// <para>No framing, ordering, or retry is promised beyond what
    /// <see cref="Capabilities"/> advertises. On an unreliable link a send may
    /// be silently dropped; that is not an error.</para>
    /// </summary>
    /// <param name="payload">The bytes to send. Must not exceed
    ///   <see cref="PeerLinkCapabilities.MaxPayloadBytes"/> when that is set.</param>
    /// <param name="ct">Cancels the send.</param>
    ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default);

    /// <summary>
    /// Stream inbound payloads from the peer, one item per received datagram /
    /// message. Single-consumer: enumerate this once. The enumeration ends
    /// cleanly when the link is disposed or <paramref name="ct"/> fires.
    ///
    /// <para><b>Buffer lifetime:</b> each yielded <see cref="ReadOnlyMemory{T}"/>
    /// is valid only until the next <c>MoveNextAsync</c>; a consumer that
    /// retains bytes past the next iteration MUST copy them. (The fake gives
    /// each item its own array, but a pooled-buffer implementation may recycle —
    /// code to the conservative contract.)</para>
    /// </summary>
    /// <param name="ct">Ends the enumeration when cancelled.</param>
    IAsyncEnumerable<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default);
}
