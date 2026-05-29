using PUG.Core;

namespace PugPong.Matchmaker;

/// <summary>
/// Single-process in-memory <see cref="IQueue{TTicket}"/> used by the
/// PugPong sample matchmaker. The sample is deliberately stateless — match
/// ends, matchmaker forgets — so a List-backed implementation is enough.
/// For studio deployments that need durability across restarts, swap in
/// <c>PUG.Redis.RedisQueue</c>.
/// </summary>
internal sealed class InMemoryQueue<TTicket> : IQueue<TTicket>
    where TTicket : ITicket<object?>
{
    private readonly List<TTicket> _items = new();
    private readonly object _lock = new();

    public Task EnqueueAsync(TTicket ticket, CancellationToken ct)
    {
        lock (_lock) _items.Add(ticket);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<TTicket>> PeekOldestAsync(int count, CancellationToken ct)
    {
        lock (_lock)
        {
            IReadOnlyList<TTicket> snap = _items.Take(count).ToList();
            return Task.FromResult(snap);
        }
    }

    public Task<int> CountAsync(CancellationToken ct)
    {
        lock (_lock) return Task.FromResult(_items.Count);
    }

    public Task RemoveAsync(Guid playerId, CancellationToken ct)
    {
        lock (_lock) _items.RemoveAll(t => t.PlayerId == playerId);
        return Task.CompletedTask;
    }
}
