namespace PUG.Redis;

/// <summary>
/// Snapshot of a <see cref="RedisQueue{TTicket}"/>'s state. Cheap to compute
/// (two ZSET commands) so safe to poll from a stats dashboard.
/// </summary>
/// <param name="Count">Current ticket count.</param>
/// <param name="OldestWait">How long the oldest ticket has been waiting, or
///   <c>null</c> if the queue is empty.</param>
public sealed record QueueStats(int Count, TimeSpan? OldestWait);
