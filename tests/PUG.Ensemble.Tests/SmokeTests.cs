using Ensemble.Client.Testing;
using Google.Protobuf;
using PUG.Ensemble.Proto;

namespace PUG.Ensemble.Tests;

/// <summary>
/// End-to-end smoke test for the PUG.Ensemble scaffold. Spawns a real
/// <c>ensembled</c> daemon via <see cref="EnsembleDaemonHarness"/>, calls
/// <c>GetIdentityAsync</c> against it, and round-trips a
/// <see cref="MatchmakerRequest"/> through Protobuf serialisation to prove
/// the Grpc.Tools codegen wired up correctly.
/// </summary>
[Trait("Category", "Integration")]
public sealed class SmokeTests
{
    [DaemonFact]
    public async Task DaemonIdentity_AndProtoRoundTrip_Work()
    {
        // The daemon binary is resolved from $ENSEMBLE_BIN (PUG has no in-repo
        // Makefile to walk to). [DaemonFact] skips this test when no binary is
        // available, so we can construct the harness unconditionally here.
        var fixture = new EnsembleDaemonHarness();
        await fixture.InitializeAsync();
        try
        {
            await using var client = new global::Ensemble.Client.EnsembleClient(fixture.GrpcAddress);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            // GetIdentity is served before Tor bootstrap completes, so we
            // don't pay for WaitForRegistryReadyAsync here.
            var id = await client.GetIdentityAsync(cts.Token);
            Assert.False(string.IsNullOrEmpty(id.Address));

            // Proto round-trip: build, serialise, parse, compare. Proves
            // Grpc.Tools generated the message types into
            // PUG.Ensemble.Proto and that the oneof wiring works.
            var payload = ByteString.CopyFrom(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF });
            var req = new MatchmakerRequest
            {
                JoinQueue = new JoinQueueRequest
                {
                    Payload = payload,
                    PrivateGameId = "",
                },
            };

            var bytes = req.ToByteArray();
            var parsed = MatchmakerRequest.Parser.ParseFrom(bytes);

            Assert.Equal(MatchmakerRequest.MsgOneofCase.JoinQueue, parsed.MsgCase);
            Assert.Equal(payload, parsed.JoinQueue.Payload);
            Assert.Equal("", parsed.JoinQueue.PrivateGameId);
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }
}
