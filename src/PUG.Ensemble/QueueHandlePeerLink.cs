using System.Runtime.CompilerServices;
using PUG.Netcode;

namespace PUG.Ensemble;

/// <summary>
/// Adapts a matched <see cref="IPeerChannel"/> (i.e. a
/// <see cref="QueueHandle{TPayload}"/> after a match has formed) to
/// <see cref="IPeerLink"/>, so <c>PUG.Netcode</c> can run its channel /
/// replication / prediction tiers over an Ensemble peer connection. This is
/// the <b>only</b> place the netcode stack touches Ensemble.
///
/// <para>
/// <b>Transport guarantees.</b> Ensemble delivers <c>SendBytesAsync</c> over
/// libp2p QUIC streams — reliable (retransmitted, no silent loss short of
/// connection failure) but <b>not ordered across sends</b>: each send opens a
/// fresh QUIC stream, and QUIC only orders <i>within</i> a stream, so two
/// back-to-back sends may be delivered out of order. Hence this link reports
/// <see cref="PeerLinkGuarantees.Reliable"/> <i>without</i>
/// <see cref="PeerLinkGuarantees.Ordered"/> — the tier-A layer can skip its own
/// reliability machinery but must add sequence numbers if it needs ordering.
/// </para>
///
/// <para>
/// <b>Payload limit.</b> Ensemble caps a single service payload at 256 KiB by
/// default (4 MiB hard ceiling), surfaced as
/// <see cref="PeerLinkCapabilities.MaxPayloadBytes"/>. Tier A fragments above
/// it; this adapter does not enforce the cap itself (the daemon rejects
/// oversize sends).
/// </para>
///
/// <para>
/// <b>Ownership.</b> The adapter does NOT own the underlying channel — the
/// game's match owns the <see cref="QueueHandle{TPayload}"/> and disposes it.
/// <see cref="DisposeAsync"/> here is a no-op, mirroring
/// <c>EnsemblePlayerClient</c>'s "wrapper doesn't dispose what it wraps"
/// convention.
/// </para>
///
/// <example>
/// Wiring <c>PUG.Netcode</c> over a freshly matched handle:
/// <code>
/// var match = await handle.WaitForMatchAsync(ct);
/// await using IPeerLink link = handle.AsPeerLink(match);
/// // hand `link` to the PUG.Netcode channel/replication tiers...
/// </code>
/// </example>
/// </summary>
public sealed class QueueHandlePeerLink : IPeerLink
{
    /// <summary>
    /// Ensemble's default per-payload size cap (256 KiB). Applies when the
    /// service manifest doesn't declare its own limit — which PUG's player
    /// service doesn't. A service that opts into a larger manifest limit (up to
    /// the 4 MiB daemon ceiling) can pass it to the constructor.
    /// </summary>
    public const int DefaultMaxPayloadBytes = 256 * 1024;

    private readonly IPeerChannel _channel;
    private readonly string _peerAddr;

    /// <summary>
    /// Wrap a matched channel and pin the one peer this link talks to.
    /// </summary>
    /// <param name="channel">The post-match messaging surface (a matched
    ///   <see cref="QueueHandle{TPayload}"/>).</param>
    /// <param name="peerAddr">The matched peer's Ensemble service address —
    ///   the link sends only here and surfaces inbound traffic only from here.</param>
    /// <param name="maxPayloadBytes">The service's per-payload cap; defaults to
    ///   <see cref="DefaultMaxPayloadBytes"/>.</param>
    public QueueHandlePeerLink(
        IPeerChannel channel,
        string peerAddr,
        int maxPayloadBytes = DefaultMaxPayloadBytes)
    {
        ArgumentNullException.ThrowIfNull(channel);
        if (string.IsNullOrWhiteSpace(peerAddr))
            throw new ArgumentException("peerAddr is required", nameof(peerAddr));

        _channel = channel;
        _peerAddr = peerAddr;
        Capabilities = new PeerLinkCapabilities(PeerLinkGuarantees.Reliable, maxPayloadBytes);
    }

    /// <inheritdoc />
    public PeerLinkCapabilities Capabilities { get; }

    /// <inheritdoc />
    public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
        new(_channel.SendToPeerAsync(_peerAddr, payload, ct));

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ReceiveAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        // The channel carries traffic from any peer (and the matchmaker is
        // already excluded upstream); keep only our matched peer's bytes, the
        // same filter MatchSession.ReadLoopAsync applies. Readiness-barrier
        // frames (PeerReadiness) share this channel pre-game; a straggler
        // resend arriving after the barrier completed must not reach the
        // channel mux, so they are filtered here too.
        await foreach (var msg in _channel.PeerMessages(ct).ConfigureAwait(false))
        {
            if (msg.FromAddr != _peerAddr)
                continue;
            if (PeerReadiness.IsReadinessFrame(msg.Bytes.Span))
                continue;
            yield return msg.Bytes;
        }
    }

    /// <summary>No-op: the adapter does not own the underlying channel.</summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// Ergonomic constructors for <see cref="QueueHandlePeerLink"/> off a matched
/// <see cref="QueueHandle{TPayload}"/>, with <c>TPayload</c> inferred.
/// </summary>
public static class QueueHandlePeerLinkExtensions
{
    /// <summary>
    /// Wrap this handle as an <see cref="IPeerLink"/> talking to
    /// <paramref name="peerAddr"/>. Valid only after the handle's
    /// <see cref="QueueHandle{TPayload}.WaitForMatchAsync"/> has succeeded.
    /// </summary>
    public static IPeerLink AsPeerLink<TPayload>(
        this QueueHandle<TPayload> handle,
        string peerAddr,
        int maxPayloadBytes = QueueHandlePeerLink.DefaultMaxPayloadBytes) =>
        new QueueHandlePeerLink(handle, peerAddr, maxPayloadBytes);

    /// <summary>
    /// Wrap this handle as an <see cref="IPeerLink"/> to the first peer in a
    /// <paramref name="match"/> — the 1v1 / introduce-to-host common case.
    /// </summary>
    public static IPeerLink AsPeerLink<TPayload>(
        this QueueHandle<TPayload> handle,
        MatchFound match,
        int maxPayloadBytes = QueueHandlePeerLink.DefaultMaxPayloadBytes)
    {
        ArgumentNullException.ThrowIfNull(match);
        if (match.Peers.Count == 0)
            throw new ArgumentException("MatchFound has no peers", nameof(match));
        return handle.AsPeerLink(match.Peers[0].EnsembleAddr, maxPayloadBytes);
    }
}
