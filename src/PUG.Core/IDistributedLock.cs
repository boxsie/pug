namespace PUG.Core;

/// <summary>
/// A best-effort mutual-exclusion primitive across processes / replicas. Phase 2
/// ships a Redis SET-NX implementation; consumers that run in-process can compose
/// a trivial <see cref="SemaphoreSlim"/>-backed version.
/// </summary>
/// <remarks>
/// Distributed locks are advisory, not authoritative. Holders must tolerate the
/// scenario where their lock is silently lost (process pause exceeding the
/// timeout, network partition). Use them to serialise common-case writes, not
/// to enforce correctness invariants.
/// </remarks>
public interface IDistributedLock
{
    /// <summary>
    /// Acquire the lock for <paramref name="key"/>, run <paramref name="action"/>,
    /// release the lock. If acquisition fails after <paramref name="retryCount"/>
    /// attempts, the call returns the default of <typeparamref name="T"/>.
    /// </summary>
    /// <param name="key">Lock identity (deployment-wide unique).</param>
    /// <param name="action">Work to perform under the lock.</param>
    /// <param name="timeout">Maximum time the lock may be held; <c>null</c> = implementation default.</param>
    /// <param name="retryCount">Acquisition retries before giving up.</param>
    /// <param name="retryDelayMs">Pause between acquisition retries.</param>
    Task<T?> ExecuteAsync<T>(
        string key,
        Func<Task<T>> action,
        TimeSpan? timeout = null,
        int retryCount = 3,
        int retryDelayMs = 100);
}
