namespace PUG.Netcode.Prediction;

/// <summary>
/// The publish side of the core's diagnostics seam, living entirely in this package.
/// A small mutable bag of named gauges that a Tier C lane updates each frame and registers
/// once via <see cref="NetDiagnostics.RegisterSource"/>; <see cref="NetDiagnostics.Snapshot"/>
/// then pulls them with no idea what they mean. This is how <c>PUG.Netcode.Prediction</c>
/// contributes interp/prediction/reconciliation counters without a single field being added
/// to the core's diagnostics records — the proof the seam is genuinely open across the
/// assembly boundary.
///
/// <para>
/// Stats keep first-set insertion order so an overlay reads stably frame to frame.
/// Lock-guarded to match <see cref="NetDiagnostics"/>'s own posture: updated from the
/// game/pump thread, sampled from the same thread each frame, but cheap and safe if the
/// host polls from another.
/// </para>
/// </summary>
public sealed class MutableStatSource : INetStatSource
{
    private readonly object _gate = new();
    private readonly List<string> _order = new();
    private readonly Dictionary<string, double> _values = new(StringComparer.Ordinal);

    /// <summary>
    /// Set (or add) the gauge <paramref name="name"/> to <paramref name="value"/>. The
    /// first time a name is seen it is appended to the stable sample order.
    /// </summary>
    public void Set(string name, double value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        lock (_gate)
        {
            if (!_values.ContainsKey(name))
            {
                _order.Add(name);
            }

            _values[name] = value;
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<NetStat> SampleStats()
    {
        lock (_gate)
        {
            var stats = new NetStat[_order.Count];
            for (var i = 0; i < _order.Count; i++)
            {
                stats[i] = new NetStat(_order[i], _values[_order[i]]);
            }

            return stats;
        }
    }
}
