namespace PUG.Core;

/// <summary>
/// Stores per-session host state — match metadata, reconnect tokens, expiring
/// handles. Phase 2 ships a Redis-backed implementation with optimistic
/// concurrency and TTL eviction; <see cref="UpdateAsync"/> wraps a read-modify-write
/// inside whatever locking primitive the implementation provides.
/// </summary>
public interface ISessionStore<TSession>
{
    /// <summary>Fetch a session by id, or <c>null</c> if absent.</summary>
    Task<TSession?> GetAsync(string id, CancellationToken ct);

    /// <summary>Insert or overwrite a session. Last-writer-wins semantics; for
    /// concurrent updates prefer <see cref="UpdateAsync"/>.</summary>
    Task SaveAsync(TSession session, CancellationToken ct);

    /// <summary>
    /// Read, apply <paramref name="update"/>, and conditionally write the result.
    /// </summary>
    /// <param name="id">Session id to update.</param>
    /// <param name="update">Returns <c>true</c> to persist the (possibly mutated)
    /// session, <c>false</c> to abandon the update.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The session as it exists after the operation
    /// (post-write if persisted, pre-write if abandoned, <c>null</c> if absent).</returns>
    Task<TSession?> UpdateAsync(string id, Func<TSession, Task<bool>> update, CancellationToken ct);

    /// <summary>Remove a session. No-op if it doesn't exist.</summary>
    Task RemoveAsync(string id, CancellationToken ct);
}
