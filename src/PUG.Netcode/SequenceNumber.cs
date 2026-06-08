namespace PUG.Netcode;

/// <summary>
/// Wrap-aware comparison for the 16-bit per-channel sequence numbers the mux
/// stamps on every packet. Sequence numbers count up and wrap at 2^16, so a raw
/// integer compare breaks across the boundary (65535 → 0 looks like a huge
/// decrease). This treats two values as "near" on a circular number line: the
/// one within half the space <i>ahead</i> is the greater.
/// </summary>
internal static class SequenceNumber
{
    private const int Half = 32768; // 2^15 — half the 16-bit sequence space.

    /// <summary>
    /// True when <paramref name="a"/> is sequentially after <paramref name="b"/>,
    /// accounting for wrap-around. Glenn Fiedler's canonical formulation: a value
    /// is "greater" when it leads the other by less than half the space going
    /// forward (or by more than half going backward, i.e. it wrapped).
    /// </summary>
    public static bool GreaterThan(ushort a, ushort b) =>
        ((a > b) && (a - b <= Half)) ||
        ((a < b) && (b - a > Half));
}
