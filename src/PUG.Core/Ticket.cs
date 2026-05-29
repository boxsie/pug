namespace PUG.Core;

/// <summary>
/// A reference <see cref="ITicket{TPayload}"/> implementation suitable for in-memory
/// queues, tests, and hosts that don't need a custom ticket shape. Adapters or
/// extension matchers that need extra fields (rank, region, etc.) define their own
/// implementations of <see cref="ITicket{TPayload}"/>.
/// </summary>
public sealed record Ticket<TPayload>(
    Guid PlayerId,
    DateTime EnqueuedAt,
    TPayload Payload,
    Guid? PrivateGameId = null) : ITicket<TPayload>;
