using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.BuildingBlocks.Persistence;

/// <summary>
/// Inbox / idempotent consumer (step 4). Kafka delivers at least once (a crash before the offset commit,
/// a rebalance, a producer retry, a re-sent event...), so every handler must survive seeing an event twice.
/// The pattern in every handler:
/// <code>
/// await using var tx = await db.Database.BeginTransactionAsync(ct);
/// if (!await inbox.TryMarkProcessedAsync(envelope.EventId, "my-consumer", ct)) return; // duplicate
/// ... domain change ...; outbox.Add(...);
/// await db.SaveChangesAsync(ct);
/// await tx.CommitAsync(ct);          // inbox row + state + outgoing events commit together
/// // then the consumer base commits the Kafka offset
/// </code>
/// </summary>
public interface IInbox
{
    /// <summary>
    /// Records that <paramref name="consumer"/> processed <paramref name="eventId"/>. Returns false when it was
    /// already recorded (a duplicate): the caller must then skip the handler. Must run inside the handler's
    /// transaction, so a failed handler also "un-marks" the event (rollback) and the redelivery is processed.
    /// </summary>
    Task<bool> TryMarkProcessedAsync(Guid eventId, string consumer, CancellationToken cancellationToken = default);
}

public sealed class Inbox<TDbContext>(TDbContext db, TimeProvider clock) : IInbox
    where TDbContext : DbContext
{
    public async Task<bool> TryMarkProcessedAsync(
        Guid eventId, string consumer, CancellationToken cancellationToken = default)
    {
        if (db.Database.CurrentTransaction is null)
            throw new InvalidOperationException(
                "Call TryMarkProcessedAsync inside the handler's transaction (db.Database.BeginTransactionAsync).");

        // INSERT ... ON CONFLICT DO NOTHING runs right now (not at SaveChanges) and reports 0 rows when the
        // pair already exists. If another replica is processing the SAME event at this moment (possible right
        // after a rebalance), this INSERT waits on the primary key until that transaction ends: if it commits
        // we get 0 (duplicate), if it rolls back we get 1 and process the event ourselves.
        var inserted = await db.Database.ExecuteSqlInterpolatedAsync(
            $"""
            INSERT INTO processed_messages (event_id, consumer, processed_at)
            VALUES ({eventId}, {consumer}, {clock.GetUtcNow()})
            ON CONFLICT (event_id, consumer) DO NOTHING
            """,
            cancellationToken);
        return inserted == 1;
    }
}
