using System.Reflection;

namespace PUG.Netcode.Tests;

/// <summary>
/// Placeholder smoke coverage so CI has something green while the netcode
/// tiers are built out. Asserts the ascetic <c>PUG.Netcode</c> assembly
/// builds, is referenced, and loads — and that it carries no infra
/// dependencies (the whole point of the seam-based design). Real coverage
/// arrives with <c>IPeerLink</c> + the tier A/B/C work.
/// </summary>
public class ScaffoldSmokeTests
{
    [Fact]
    public void NetcodeAssembly_Loads()
    {
        var asm = Assembly.Load("PUG.Netcode");
        Assert.NotNull(asm);
    }

    [Fact]
    public void NetcodeAssembly_HasNoInfraDependencies()
    {
        var referenced = Assembly.Load("PUG.Netcode")
            .GetReferencedAssemblies()
            .Select(a => a.Name);

        // The core must never reach for transport/infra. If any of these
        // shows up, something tier-specific leaked into the ascetic package.
        Assert.DoesNotContain("Ensemble.Client", referenced);
        Assert.DoesNotContain("StackExchange.Redis", referenced);
        Assert.DoesNotContain("Grpc.Net.Client", referenced);
    }
}
