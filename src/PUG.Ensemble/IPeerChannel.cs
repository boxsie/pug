namespace PUG.Ensemble;

/// <summary>
/// The minimal post-match peer messaging surface: send bytes to a peer, and
/// stream inbound messages from peers. <see cref="QueueHandle{TPayload}"/>
/// implements this directly (its <c>SendToPeerAsync</c> / <c>PeerMessages</c>
/// members match member-for-member), so it exists purely to <b>decouple</b>
/// the netcode adapter (<see cref="QueueHandlePeerLink"/>) from the concrete,
/// generic, daemon-bound handle.
///
/// <para>
/// That decoupling is what lets the adapter be (a) non-generic — it never
/// needs <c>TPayload</c> — and (b) unit-testable with an in-memory fake, since
/// constructing a real <see cref="QueueHandle{TPayload}"/> requires a live
/// Ensemble daemon.
/// </para>
/// </summary>
public interface IPeerChannel
{
    /// <summary>
    /// Send game-time bytes to a matched peer. No framing, ordering, or retry
    /// beyond what the transport provides. See
    /// <see cref="QueueHandle{TPayload}.SendToPeerAsync"/> for the full contract.
    /// </summary>
    Task SendToPeerAsync(string peerAddr, ReadOnlyMemory<byte> bytes, CancellationToken ct = default);

    /// <summary>
    /// Stream inbound game-time messages from matched peers. Carries traffic
    /// from <i>any</i> peer; consumers filter by sender as needed. See
    /// <see cref="QueueHandle{TPayload}.PeerMessages"/>.
    /// </summary>
    IAsyncEnumerable<PeerMessage> PeerMessages(CancellationToken ct = default);
}
