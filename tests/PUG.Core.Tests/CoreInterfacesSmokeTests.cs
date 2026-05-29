namespace PUG.Core.Tests;

/// <summary>
/// "Hello-world consumer" smoke test required by the Phase 1 interfaces ticket:
/// pin a concrete <c>TPayload</c> (here, <see cref="byte"/> array) and reference
/// every interface in <see cref="PUG.Core"/> so a future rename / signature change
/// surfaces here at compile time, not in a downstream adapter.
/// </summary>
public sealed class CoreInterfacesSmokeTests
{
    [Fact]
    public void EveryInterfaceCompilesWithBytePayload()
    {
        // A concrete TPayload (byte[]) drives the generic plumbing across every type.
        Ticket<byte[]> ticket = new(
            PlayerId: Guid.NewGuid(),
            EnqueuedAt: DateTime.UtcNow,
            Payload: new byte[] { 1, 2, 3 },
            PrivateGameId: null);

        // ITicket variance: covariant TPayload lets us up-cast to ITicket<object>.
        ITicket<byte[]> asTicket = ticket;
        ITicket<object> asObjectTicket = ticket;

        Assert.NotNull(asTicket);
        Assert.NotNull(asObjectTicket);

        // MatchResult / Team round-trip with a single team of one.
        var team = new Team<Ticket<byte[]>>(0, new[] { ticket });
        var result = new MatchResult<Ticket<byte[]>>(new[] { team });
        Assert.Single(result.Teams);
        Assert.Equal(ticket, result.Teams[0].Members[0]);

        // Stub implementations: prove the interfaces *can* be implemented against
        // a TPayload that's a real type. We don't exercise behaviour here —
        // FifoMatcher / InMemoryPrivateLobby tickets do that next.
        IMatcher<Ticket<byte[]>> matcher = new NullMatcher<Ticket<byte[]>>();
        IQueue<Ticket<byte[]>> queue = new NullQueue<Ticket<byte[]>>();
        IPrivateLobby lobby = new NullPrivateLobby();
        ISessionStore<string> sessions = new NullSessionStore<string>();
        IDistributedLock locks = new NullDistributedLock();
        IBackfillProvider<Ticket<byte[]>> backfill = new NullBackfill<Ticket<byte[]>>();
        IPayloadVerifier<byte[]> verifier = new NullVerifier<byte[]>();

        Assert.NotNull(matcher);
        Assert.NotNull(queue);
        Assert.NotNull(lobby);
        Assert.NotNull(sessions);
        Assert.NotNull(locks);
        Assert.NotNull(backfill);
        Assert.NotNull(verifier);
    }

    private sealed class NullMatcher<TTicket> : IMatcher<TTicket>
    {
        public Task<MatchResult<TTicket>?> TryMatchAsync(CancellationToken ct) =>
            Task.FromResult<MatchResult<TTicket>?>(null);
    }

    private sealed class NullQueue<TTicket> : IQueue<TTicket>
    {
        public Task EnqueueAsync(TTicket ticket, CancellationToken ct) => Task.CompletedTask;
        public Task<IReadOnlyList<TTicket>> PeekOldestAsync(int count, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<TTicket>>(Array.Empty<TTicket>());
        public Task<int> CountAsync(CancellationToken ct) => Task.FromResult(0);
        public Task RemoveAsync(Guid playerId, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class NullPrivateLobby : IPrivateLobby
    {
        public Task<(string Code, Guid PrivateGameId)> GenerateCodeAsync(CancellationToken ct) =>
            Task.FromResult(("ABC234", Guid.NewGuid()));
        public Task<Guid?> ResolveCodeAsync(string code, CancellationToken ct) =>
            Task.FromResult<Guid?>(null);
        public Task ExpireCodeAsync(string code, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class NullSessionStore<TSession> : ISessionStore<TSession>
    {
        public Task<TSession?> GetAsync(string id, CancellationToken ct) =>
            Task.FromResult<TSession?>(default);
        public Task SaveAsync(TSession session, CancellationToken ct) => Task.CompletedTask;
        public Task<TSession?> UpdateAsync(string id, Func<TSession, Task<bool>> update, CancellationToken ct) =>
            Task.FromResult<TSession?>(default);
        public Task RemoveAsync(string id, CancellationToken ct) => Task.CompletedTask;
    }

    private sealed class NullDistributedLock : IDistributedLock
    {
        public Task<T?> ExecuteAsync<T>(
            string key,
            Func<Task<T>> action,
            TimeSpan? timeout = null,
            int retryCount = 3,
            int retryDelayMs = 100) => Task.FromResult<T?>(default);
    }

    private sealed class NullBackfill<TTicket> : IBackfillProvider<TTicket>
    {
        public Task<bool> TryBackfillAsync(TTicket waitingPlayer, CancellationToken ct) =>
            Task.FromResult(false);
    }

    private sealed class NullVerifier<TPayload> : IPayloadVerifier<TPayload>
    {
        public Task<bool> VerifyAsync(Guid playerId, TPayload payload, CancellationToken ct) =>
            Task.FromResult(true);
    }
}
