namespace PUG.Netcode;

/// <summary>
/// How a client turns an authoritative state update into a change on a replicated
/// entity. This is the seam between Tier B (what's true) and Tier C (how it
/// <i>feels</i>): B1 ships <see cref="ImmediateApply"/>, which hard-snaps the
/// entity to the newest authoritative bytes — correct but jittery. Tier C drops a
/// richer strategy into the very same call to interpolate remote entities and
/// predict the local one, with no change to <see cref="NetworkReplicator"/>.
/// </summary>
public interface ISnapshotApplyStrategy
{
    /// <summary>
    /// Apply authoritative <paramref name="state"/> (stamped
    /// <paramref name="snapshotTick"/>) to <paramref name="entity"/>. A hard-snap
    /// strategy calls <see cref="INetEntityState.ApplyState"/> straight through; an
    /// interpolating strategy may instead record the sample and blend toward it
    /// over later frames.
    /// </summary>
    void Apply(INetEntityState entity, uint snapshotTick, ReadOnlySpan<byte> state);
}

/// <summary>
/// The trivial strategy: write the authoritative bytes onto the entity now. The
/// B1 default — provably correct, deliberately unsmoothed. Replace with a Tier C
/// strategy when you want interpolation/prediction.
/// </summary>
public sealed class ImmediateApply : ISnapshotApplyStrategy
{
    /// <inheritdoc />
    public void Apply(INetEntityState entity, uint snapshotTick, ReadOnlySpan<byte> state)
    {
        ArgumentNullException.ThrowIfNull(entity);
        entity.ApplyState(state);
    }
}
