namespace PUG.Netcode.Prediction.Tests;

/// <summary>
/// Proves the scaffold's load-bearing claim: <c>PUG.Netcode.Prediction</c> contributes
/// diagnostics to the core's <see cref="NetDiagnostics"/> from <i>outside</i> the core
/// assembly, through the generic <see cref="INetStatSource"/> seam, with no core type
/// knowing what the stats are. If this passes, the diagnostics surface is genuinely open
/// across the package boundary — the whole reason for the split.
/// </summary>
public sealed class DiagnosticsSeamTests
{
    [Fact]
    public void RegisteredSource_AppearsInSnapshot_WithItsNamedStats()
    {
        var diagnostics = new NetDiagnostics();
        var source = new MutableStatSource();
        source.Set("bufferDepth", 3);
        source.Set("interpDelayTicks", 6);

        diagnostics.RegisterSource("interp:guest-paddle", source);

        var snapshot = diagnostics.Snapshot();

        var group = Assert.Single(snapshot.Sources);
        Assert.Equal("interp:guest-paddle", group.Label);
        var stats = group.Stats.ToDictionary(s => s.Name, s => s.Value);
        Assert.Equal(2, stats.Count);
        Assert.Equal(3d, stats["bufferDepth"]);
        Assert.Equal(6d, stats["interpDelayTicks"]);
    }

    [Fact]
    public void Snapshot_ReflectsLiveMutation_PullModelNotCachedAtRegistration()
    {
        var diagnostics = new NetDiagnostics();
        var source = new MutableStatSource();
        source.Set("underruns", 0);
        diagnostics.RegisterSource("interp:ball", source);

        source.Set("underruns", 5);

        var group = Assert.Single(diagnostics.Snapshot().Sources);
        var stat = Assert.Single(group.Stats);
        Assert.Equal("underruns", stat.Name);
        Assert.Equal(5, stat.Value);
    }

    [Fact]
    public void Describe_IncludesSourceStats()
    {
        var diagnostics = new NetDiagnostics();
        var source = new MutableStatSource();
        source.Set("interpDelayTicks", 6);
        diagnostics.RegisterSource("interp:guest-paddle", source);

        var text = diagnostics.Describe();

        Assert.Contains("interp:guest-paddle", text);
        Assert.Contains("interpDelayTicks=6", text);
    }

    [Fact]
    public void MutableStatSource_KeepsFirstSetInsertionOrder()
    {
        var source = new MutableStatSource();
        source.Set("b", 1);
        source.Set("a", 2);
        source.Set("b", 3); // re-set must not reorder

        var stats = source.SampleStats();

        Assert.Equal("b", stats[0].Name);
        Assert.Equal(3, stats[0].Value);
        Assert.Equal("a", stats[1].Name);
    }

    [Fact]
    public void PredictionAssembly_ReferencesOnlyNetcodeCore_NoInfraDeps()
    {
        var referenced = typeof(MutableStatSource).Assembly.GetReferencedAssemblies()
            .Select(a => a.Name)
            .ToArray();

        Assert.Contains("PUG.Netcode", referenced);
        Assert.DoesNotContain("Ensemble.Client", referenced);
        Assert.DoesNotContain("StackExchange.Redis", referenced);
        Assert.DoesNotContain("Grpc.Net.Client", referenced);
        Assert.DoesNotContain("PUG.Core", referenced);
    }
}
