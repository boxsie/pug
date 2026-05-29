namespace PUG.Core.Tests;

/// <summary>
/// Test-only <see cref="IQueue{TTicket}"/> backed by a List. Preserves enqueue
/// order so <see cref="PeekOldestAsync"/> is oldest-first by construction. Not
/// thread-safe — tests drive it from a single async sequence.
/// </summary>
internal sealed class InMemoryQueue<TTicket> : IQueue<TTicket>
    where TTicket : ITicket<object?>
{
    private readonly List<TTicket> _items = new();

    public Task EnqueueAsync(TTicket ticket, CancellationToken ct)
    {
        _items.Add(ticket);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<TTicket>> PeekOldestAsync(int count, CancellationToken ct)
    {
        var take = Math.Min(count, _items.Count);
        IReadOnlyList<TTicket> result = _items.Take(take).ToList();
        return Task.FromResult(result);
    }

    public Task<int> CountAsync(CancellationToken ct) => Task.FromResult(_items.Count);

    public Task RemoveAsync(Guid playerId, CancellationToken ct)
    {
        _items.RemoveAll(t => t.PlayerId == playerId);
        return Task.CompletedTask;
    }
}
