namespace PUG.Redis;

/// <summary>
/// Implemented by session types stored in <see cref="RedisSessionStore{T}"/>.
/// The session carries its own identity (so <see cref="PUG.Core.ISessionStore{T}.SaveAsync"/>
/// doesn't need a separate id parameter) and a version counter that the
/// session store increments on every successful update.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Version"/> is settable so the store can bump it after a
/// successful update. Hosts read it for optimistic-concurrency checks if
/// they want to layer their own conflict detection on top — the store's
/// lock already serialises writes, but exposing Version lets a client
/// detect "did anyone else touch this between my Get and my Save?".
/// </para>
/// <para>
/// Lives in <c>PUG.Redis</c> rather than <c>PUG.Core</c> because version
/// counting is a Redis-impl concern, not a Core contract. A future
/// EventStore- or SQL-backed <c>ISessionStore</c> might use a different
/// concurrency primitive (event-source revision, row version) and shouldn't
/// be forced to expose <see cref="Version"/>.
/// </para>
/// </remarks>
public interface IVersioned
{
    /// <summary>Stable session identity. Composes into the Redis key:
    /// <c>pug:session:{typeName}:{Id}</c>.</summary>
    string Id { get; }

    /// <summary>Bumped by the store on every successful update.</summary>
    int Version { get; set; }
}
