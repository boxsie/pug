using PUG.Core;

namespace PUG.Ensemble;

/// <summary>
/// Configuration for a <see cref="MatchmakerServiceHost{TPayload}"/>.
/// </summary>
/// <remarks>
/// <para>
/// Carries the service-manifest knobs (ACL is hard-coded to
/// <c>Public</c>; transport is hard-coded to <c>Rpc</c>) plus host-side
/// behaviour: match-loop tick interval, introduction expiry, and the
/// payload (de)serialisation contract.
/// </para>
/// <para>
/// <b>Payload serialisation contract.</b> The host round-trips
/// <typeparamref name="TPayload"/> through the proto <c>bytes payload</c>
/// field on <c>JoinQueueRequest</c>. PUG.Ensemble deliberately does not
/// pick a serialisation format for you — JSON, Protobuf, MessagePack,
/// raw bytes all work. Set <see cref="SerializePayload"/> and
/// <see cref="DeserializePayload"/> to a matching pair; the host calls
/// <see cref="DeserializePayload"/> on inbound <c>JoinQueueRequest</c>
/// payload bytes before enqueueing. If left null the host treats
/// <typeparamref name="TPayload"/> as <c>byte[]</c> via a cast — only
/// safe when <typeparamref name="TPayload"/> is literally <c>byte[]</c>.
/// </para>
/// <para>
/// <b>Optional payload verifier.</b> See
/// <see cref="MatchmakerServiceHost{TPayload}"/> for how
/// <see cref="PUG.Core.IPayloadVerifier{TPayload}"/> integrates — pass
/// one to the host constructor; the host runs verification before
/// enqueueing and replies with <c>ErrorResponse{ Code = "rejected" }</c>
/// on failure.
/// </para>
/// </remarks>
/// <param name="ServiceName">Service-name slug used as the Ensemble
///   registry key. Must be unique per daemon and slug-safe (lowercase
///   alphanumeric + dash + underscore — per Ensemble's registry rules).</param>
/// <param name="TeamSizes">Per-team member counts, forwarded to the
///   matcher's own validation. Used by the host to drive introduction
///   pairing (when <see cref="IntroduceTeammatesOnly"/> is false the host
///   introduces all-pairs across teams; when true, only within each team).</param>
/// <param name="MaxPayloadBytes">Manifest payload cap. Default 256 KiB.</param>
/// <param name="RateLimitPerMinute">Manifest per-source rate limit. Default 600.</param>
/// <param name="RateLimitBurst">Manifest burst capacity. Default 60.</param>
/// <param name="IntroductionExpiry">How long an issued introduction is
///   considered valid (becomes the daemon's <c>expires_at</c> ms-since-epoch).
///   Default 30s.</param>
/// <param name="MatchTickInterval">Match-loop tick interval. Default 1s.</param>
/// <param name="IntroduceTeammatesOnly">When true the host only introduces
///   ticket pairs within the same team; when false (default) the host
///   introduces every pair across all teams so each player learns every
///   teammate AND opponent.</param>
/// <param name="SerializePayload">Optional payload serialiser. See remarks.</param>
/// <param name="DeserializePayload">Optional payload deserialiser. See remarks.</param>
/// <param name="PrivateLobby">Optional <see cref="IPrivateLobby"/> for the
///   private-code domain. When <c>null</c>, the host materialises a fresh
///   <see cref="InMemoryPrivateLobby"/> on construction — suitable for tests
///   and single-process matchmakers. For restart-durable codes inject a
///   Redis-backed implementation.</param>
/// <param name="PrivateCodeTtl">How long a freshly-generated private code
///   remains valid when no second player joins. Default 30 minutes. Host
///   prunes lazily on every match-loop tick. Set to
///   <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> to opt out of
///   host-side TTL (use when the lobby implementation itself handles
///   expiry, e.g. a Redis lobby with <c>EXPIRE</c>).</param>
public sealed record MatchmakerOptions<TPayload>(
    string ServiceName,
    IReadOnlyList<int> TeamSizes,
    int MaxPayloadBytes = 256 * 1024,
    int RateLimitPerMinute = 600,
    int RateLimitBurst = 60,
    TimeSpan? IntroductionExpiry = null,
    TimeSpan? MatchTickInterval = null,
    bool IntroduceTeammatesOnly = false,
    Func<TPayload, byte[]>? SerializePayload = null,
    Func<byte[], TPayload>? DeserializePayload = null,
    IPrivateLobby? PrivateLobby = null,
    TimeSpan? PrivateCodeTtl = null)
{
    /// <summary>Effective introduction expiry — defaults to 30 seconds.</summary>
    public TimeSpan EffectiveIntroductionExpiry => IntroductionExpiry ?? TimeSpan.FromSeconds(30);

    /// <summary>Effective match-loop tick — defaults to 1 second.</summary>
    public TimeSpan EffectiveMatchTickInterval => MatchTickInterval ?? TimeSpan.FromSeconds(1);

    /// <summary>Effective private-code TTL — defaults to 30 minutes.</summary>
    public TimeSpan EffectivePrivateCodeTtl => PrivateCodeTtl ?? TimeSpan.FromMinutes(30);
}
