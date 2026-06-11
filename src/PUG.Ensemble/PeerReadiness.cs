namespace PUG.Ensemble;

/// <summary>
/// Post-match connection barrier: proves the peer link works in BOTH
/// directions before gameplay starts, so a game never begins against a peer
/// it can't actually reach yet.
///
/// <para>
/// <b>Why this exists.</b> <see cref="MatchFound"/> means the matchmaker
/// introduced the peers and the daemon <i>enqueued</i> a dial — over Tor the
/// actual connection takes ~10–20 seconds (hidden-service dial + handshake
/// each way). Starting the match scene on the introduction alone plays the
/// host against a void until the link lands (the "guest is frozen for ten
/// seconds" lobby bug). Game code should hold its "connecting…" screen on
/// <see cref="WaitForPeerReadyAsync"/> and only enter the match when it
/// completes — both sides complete within one round-trip of each other, so it
/// doubles as the synchronized "game start" signal.
/// </para>
///
/// <para>
/// <b>Protocol.</b> Each side repeatedly sends a tiny READY frame (the daemon
/// retries the underlying dial on every failed send, so the resends are also
/// what drives connection establishment). On receiving the peer's READY it
/// replies ACK. The barrier completes when it has BOTH received the peer's
/// READY (proves peer→us) AND received an ACK for one of its own READYs
/// (proves us→peer). Receiving any non-barrier frame from the peer also
/// completes it: the peer only sends game traffic after ITS barrier finished,
/// which implies both directions are live (that frame is consumed by the
/// barrier — acceptable, because the netcode's snapshot channels are
/// latest-wins and resend continuously).
/// </para>
///
/// <para>
/// <b>Frames.</b> 9 bytes: the ASCII magic <c>PUG-RDY1</c> + one type byte.
/// They flow over the same <see cref="IPeerChannel"/> as game traffic, so the
/// netcode adapter (<see cref="QueueHandlePeerLink"/>) filters them out of
/// <c>ReceiveAsync</c> — a straggler resend arriving after the barrier
/// completed must not reach the channel mux.
/// </para>
/// </summary>
public static class PeerReadiness
{
    /// <summary>Leading magic of every readiness frame.</summary>
    internal static readonly byte[] Magic = "PUG-RDY1"u8.ToArray();

    private const byte TypeReady = 0;
    private const byte TypeAck = 1;

    private static readonly byte[] ReadyFrame = BuildFrame(TypeReady);
    private static readonly byte[] AckFrame = BuildFrame(TypeAck);

    /// <summary>Default interval between READY resends while waiting.</summary>
    public static readonly TimeSpan DefaultResendInterval = TimeSpan.FromSeconds(1);

    private static byte[] BuildFrame(byte type)
    {
        var frame = new byte[Magic.Length + 1];
        Magic.CopyTo(frame, 0);
        frame[^1] = type;
        return frame;
    }

    /// <summary>
    /// True when <paramref name="frame"/> is a readiness-barrier frame (READY
    /// or ACK). Used by transports layered on the same channel to filter
    /// barrier traffic out of the game stream.
    /// </summary>
    public static bool IsReadinessFrame(ReadOnlySpan<byte> frame) =>
        frame.Length == Magic.Length + 1 && frame.StartsWith(Magic);

    /// <summary>
    /// Run the readiness barrier against <paramref name="peerAddr"/> over a
    /// matched channel. Completes when the link is proven in both directions
    /// (see the class doc); throws <see cref="OperationCanceledException"/>
    /// when <paramref name="ct"/> fires (wrap with a timeout token to bound
    /// the wait) and <see cref="MatchmakingFailedException"/> if the channel's
    /// message stream ends before the barrier completes (handle disposed).
    /// </summary>
    /// <param name="channel">The matched messaging surface (a
    ///   <see cref="QueueHandle{TPayload}"/> after a match has formed).</param>
    /// <param name="peerAddr">The matched peer's service address.</param>
    /// <param name="resendInterval">READY resend cadence; defaults to
    ///   <see cref="DefaultResendInterval"/>.</param>
    /// <param name="ct">Cancels the wait.</param>
    public static async Task WaitForPeerReadyAsync(
        IPeerChannel channel,
        string peerAddr,
        TimeSpan? resendInterval = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(channel);
        if (string.IsNullOrWhiteSpace(peerAddr))
            throw new ArgumentException("peerAddr is required", nameof(peerAddr));

        var interval = resendInterval ?? DefaultResendInterval;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // READY resender. Send failures are expected while the daemon is
        // still dialing the peer (each failed send also triggers the daemon's
        // dial-as-service retry, so this loop is what drives the connection
        // into existence) — swallow and resend on cadence.
        var sender = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    await channel.SendToPeerAsync(peerAddr, ReadyFrame, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch
                {
                    // Daemon-side "not connected" or transient stream error —
                    // resend on the next tick.
                }
                try
                {
                    await Task.Delay(interval, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }, CancellationToken.None);

        var peerReady = false; // peer's READY (or game frame) seen — peer→us works
        var acked = false;     // peer ACKed one of our READYs — us→peer works
        try
        {
            await foreach (var msg in channel.PeerMessages(cts.Token).ConfigureAwait(false))
            {
                if (msg.FromAddr != peerAddr)
                    continue;

                if (IsReadinessFrame(msg.Bytes.Span))
                {
                    switch (msg.Bytes.Span[Magic.Length])
                    {
                        case TypeReady:
                            peerReady = true;
                            try
                            {
                                await channel.SendToPeerAsync(peerAddr, AckFrame, cts.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch
                            {
                                // The peer's READY reached us, so the route
                                // exists; a transient ACK failure is covered
                                // by the next READY it resends.
                            }
                            break;
                        case TypeAck:
                            acked = true;
                            break;
                    }
                }
                else
                {
                    // Game traffic: the peer only sends it after its own
                    // barrier completed, which implies both directions work.
                    peerReady = true;
                    acked = true;
                }

                if (peerReady && acked)
                    break;
            }
        }
        finally
        {
            cts.Cancel();
            try { await sender.ConfigureAwait(false); } catch { /* best effort */ }
        }

        ct.ThrowIfCancellationRequested();
        if (!(peerReady && acked))
            throw new MatchmakingFailedException(
                "peer channel closed before the connection barrier completed");
    }
}
