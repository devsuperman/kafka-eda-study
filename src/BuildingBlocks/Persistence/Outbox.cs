using System.Text.Json;
using KafkaEdaStudy.Contracts;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.BuildingBlocks.Persistence;

/// <summary>
/// Transactional outbox (step 6). Instead of producing to Kafka directly (a "dual write" that can save the
/// state but lose the event, or the other way round), a handler ADDS the event to its own database.
/// </summary>
public interface IOutbox
{
    /// <summary>
    /// Adds the event to the current DbContext. Nothing is written yet: the row is inserted by the caller's
    /// next <c>SaveChangesAsync</c>, in the same transaction as the state change.
    /// </summary>
    void Add(string topic, EventEnvelope envelope);
}

public sealed class Outbox<TDbContext>(TDbContext db, TimeProvider clock) : IOutbox
    where TDbContext : DbContext
{
    public void Add(string topic, EventEnvelope envelope) =>
        db.Set<OutboxMessage>().Add(new OutboxMessage
        {
            EventId = envelope.EventId,
            EventType = envelope.EventType,
            Topic = topic,
            Key = envelope.OrderId.ToString(),
            Envelope = JsonSerializer.Serialize(envelope, EventJson.Options),
            OccurredAt = envelope.OccurredAt,
            CreatedAt = clock.GetUtcNow(),
        });
}
