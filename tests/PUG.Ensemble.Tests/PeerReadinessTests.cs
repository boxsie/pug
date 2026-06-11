using System.Threading.Channels;

namespace PUG.Ensemble.Tests;

/// <summary>
/// Unit coverage for the <see cref="PeerReadiness"/> connection barrier. Runs
/// with NO daemon — the barrier depends only on the <see cref="IPeerChannel"/>
/// seam, so an in-memory fake drives both directions deterministically.
/// </summary>
public class PeerReadinessTests
{
    private const string PeerAddr = "Epeer";
    private const string OtherAddr = "Eother";

    private static readonly TimeSpan FastResend = TimeSpan.FromMilliseconds(20);

    private static byte[] Frame(byte type)
    {
        var f = new byte[PeerReadiness.Magic.Length + 1];
        PeerReadiness.Magic.CopyTo(f, 0);
        f[^1] = type;
        return f;
    }

    private static byte[] ReadyFrame => Frame(0);
    private static byte[] AckFrame => Frame(1);

    [Fact]
    public void IsReadinessFrame_MatchesBarrierFramesOnly()
    {
        Assert.True(PeerReadiness.IsReadinessFrame(ReadyFrame));
        Assert.True(PeerReadiness.IsReadinessFrame(AckFrame));
        Assert.False(PeerReadiness.IsReadinessFrame(new byte[] { 1, 2, 3 }));
        Assert.False(PeerReadiness.IsReadinessFrame(PeerReadiness.Magic)); // missing type byte
        Assert.False(PeerReadiness.IsReadinessFrame(Array.Empty<byte>()));
    }

    [Fact]
    public async Task CompletesOnPeerReadyPlusAck_AndAcksThePeer()
    {
        var channel = new FakePeerChannel();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend, cts.Token);

        // Peer's READY arrives → we must ACK it; then the peer ACKs ours.
        channel.Inject(PeerAddr, ReadyFrame);
        await channel.WaitForSendAsync(b => b[^1] == 1, cts.Token); // our ACK went out
        channel.Inject(PeerAddr, AckFrame);

        await wait; // completes — both directions proven

        // The barrier sent at least one READY (the resender) and exactly the
        // ACK we observed.
        Assert.Contains(channel.Sent, s => s.PeerAddr == PeerAddr && s.Bytes[^1] == 0);
        Assert.Contains(channel.Sent, s => s.PeerAddr == PeerAddr && s.Bytes[^1] == 1);
    }

    [Fact]
    public async Task PeerReadyAlone_DoesNotComplete()
    {
        var channel = new FakePeerChannel();
        using var cts = new CancellationTokenSource();

        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend, cts.Token);
        channel.Inject(PeerAddr, ReadyFrame); // peer→us proven, us→peer NOT

        var finished = await Task.WhenAny(wait, Task.Delay(200));
        Assert.NotSame(wait, finished);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task GameFrameFromPeer_ImpliesCompletion()
    {
        var channel = new FakePeerChannel();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend, cts.Token);

        // The peer only sends game traffic after ITS barrier completed, which
        // implies both directions are live — a snapshot overtaking the ACK
        // (sends are unordered across streams) must not stall us.
        channel.Inject(PeerAddr, new byte[] { 42, 1, 2, 3 });

        await wait;
    }

    [Fact]
    public async Task FramesFromOtherPeers_AreIgnored()
    {
        var channel = new FakePeerChannel();
        using var cts = new CancellationTokenSource();

        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend, cts.Token);
        channel.Inject(OtherAddr, ReadyFrame);
        channel.Inject(OtherAddr, AckFrame);
        channel.Inject(OtherAddr, new byte[] { 9 });

        var finished = await Task.WhenAny(wait, Task.Delay(200));
        Assert.NotSame(wait, finished);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task ResendsReadyUntilCompletion()
    {
        var channel = new FakePeerChannel();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend, cts.Token);

        // Wait until several READYs have gone out (the dial-driving resend).
        await channel.WaitForSendCountAsync(b => b[^1] == 0, 3, cts.Token);

        channel.Inject(PeerAddr, ReadyFrame);
        channel.Inject(PeerAddr, AckFrame);
        await wait;
    }

    [Fact]
    public async Task ChannelClosingBeforeCompletion_Throws()
    {
        var channel = new FakePeerChannel();
        var wait = PeerReadiness.WaitForPeerReadyAsync(channel, PeerAddr, FastResend);

        channel.Complete();

        await Assert.ThrowsAsync<MatchmakingFailedException>(() => wait);
    }

    [Fact]
    public async Task TwoBarriers_BackToBack_BothComplete()
    {
        // The full symmetric shape: two fakes cross-wired so each side's sends
        // become the other's inbound stream — exactly the production topology.
        var a = new FakePeerChannel();
        var b = new FakePeerChannel();
        a.OnSend = (_, bytes) => b.Inject("Ea", bytes);
        b.OnSend = (_, bytes) => a.Inject("Eb", bytes);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var waitA = PeerReadiness.WaitForPeerReadyAsync(a, "Eb", FastResend, cts.Token);
        var waitB = PeerReadiness.WaitForPeerReadyAsync(b, "Ea", FastResend, cts.Token);

        await Task.WhenAll(waitA, waitB);
    }

    /// <summary>In-memory <see cref="IPeerChannel"/> recording sends and
    /// replaying injected messages; optionally cross-wired via OnSend.</summary>
    private sealed class FakePeerChannel : IPeerChannel
    {
        private readonly Channel<PeerMessage> _inbound = Channel.CreateUnbounded<PeerMessage>();
        private readonly object _gate = new();

        public List<(string PeerAddr, byte[] Bytes)> Sent { get; } = new();

        public Action<string, byte[]>? OnSend { get; set; }

        public Task SendToPeerAsync(string peerAddr, ReadOnlyMemory<byte> bytes, CancellationToken ct = default)
        {
            var copy = bytes.ToArray();
            lock (_gate)
            {
                Sent.Add((peerAddr, copy));
            }
            OnSend?.Invoke(peerAddr, copy);
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<PeerMessage> PeerMessages(CancellationToken ct = default) =>
            _inbound.Reader.ReadAllAsync(ct);

        public void Inject(string fromAddr, byte[] bytes) =>
            _inbound.Writer.TryWrite(new PeerMessage(fromAddr, bytes, DateTimeOffset.UtcNow));

        public void Complete() => _inbound.Writer.TryComplete();

        public async Task WaitForSendAsync(Func<byte[], bool> match, CancellationToken ct)
        {
            while (true)
            {
                lock (_gate)
                {
                    if (Sent.Any(s => match(s.Bytes)))
                        return;
                }
                await Task.Delay(5, ct);
            }
        }

        public async Task WaitForSendCountAsync(Func<byte[], bool> match, int count, CancellationToken ct)
        {
            while (true)
            {
                lock (_gate)
                {
                    if (Sent.Count(s => match(s.Bytes)) >= count)
                        return;
                }
                await Task.Delay(5, ct);
            }
        }
    }
}
