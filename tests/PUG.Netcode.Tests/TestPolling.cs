namespace PUG.Netcode.Tests;

/// <summary>Shared async polling for tests that bridge the mux's background drain
/// (which processes inbound packets on its own task) to synchronous assertions.</summary>
internal static class TestPolling
{
    /// <summary>Poll <paramref name="condition"/> until true or a 5s deadline,
    /// then fail loudly naming <paramref name="what"/> was awaited.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, string what)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(2);
        }

        throw new TimeoutException($"timed out waiting for: {what}");
    }
}
