using Ensemble.Client.Testing;

namespace PUG.Ensemble.Tests;

/// <summary>
/// A <see cref="FactAttribute"/> that marks itself skipped (rather than failed)
/// when no <c>ensemble</c> daemon binary is available — i.e. when
/// <c>$ENSEMBLE_BIN</c> isn't set to an existing binary (PUG has no in-repo
/// daemon to walk to). Use on daemon-backed integration tests so a checkout
/// without a built daemon reports skips, not failures. Availability is
/// evaluated once at test discovery.
/// </summary>
public sealed class DaemonFactAttribute : FactAttribute
{
    public DaemonFactAttribute()
    {
        if (!EnsembleDaemonHarness.IsDaemonAvailable)
            Skip = "ensemble daemon binary not available — set $ENSEMBLE_BIN to a built ensemble daemon.";
    }
}
