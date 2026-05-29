using System.Collections.Concurrent;

namespace PUG.Core;

/// <summary>
/// In-memory reference <see cref="IPrivateLobby"/>. Suitable for tests and
/// single-process matchmaker deployments. Codes survive only as long as the
/// process; for restart durability, plug in the Redis-backed implementation
/// shipped from <c>PUG.Redis</c> (Phase 2).
/// </summary>
public sealed class InMemoryPrivateLobby : IPrivateLobby
{
    private const int CodeLength = 6;
    private const int MaxAttempts = 8;

    private readonly ConcurrentDictionary<string, Guid> _codes = new(StringComparer.Ordinal);

    /// <inheritdoc/>
    public async Task<(string Code, Guid PrivateGameId)> GenerateCodeAsync(CancellationToken ct)
    {
        var gameId = Guid.NewGuid();

        // Tight loop: ShortCodeGenerator.GenerateUniqueAsync calls our
        // ContainsKey-based predicate. TryAdd then guards the rare check-then-act
        // race where another writer added the same code between predicate and
        // insert. On any race, propagate ShortCodeGenerator's exhaustion error.
        for (var attempt = 0; attempt < MaxAttempts; attempt++)
        {
            ct.ThrowIfCancellationRequested();

            var code = await ShortCodeGenerator.GenerateUniqueAsync(
                CodeLength,
                c => Task.FromResult(_codes.ContainsKey(c)),
                MaxAttempts).ConfigureAwait(false);

            if (_codes.TryAdd(code, gameId))
            {
                return (code, gameId);
            }
        }

        throw new InvalidOperationException(
            $"InMemoryPrivateLobby could not insert a unique code after {MaxAttempts} attempts " +
            "— concurrent writers are exhausting the namespace.");
    }

    /// <inheritdoc/>
    public Task<Guid?> ResolveCodeAsync(string code, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(code);
        return Task.FromResult(_codes.TryGetValue(code, out var id) ? id : (Guid?)null);
    }

    /// <inheritdoc/>
    public Task ExpireCodeAsync(string code, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(code);
        _codes.TryRemove(code, out _);
        return Task.CompletedTask;
    }
}
