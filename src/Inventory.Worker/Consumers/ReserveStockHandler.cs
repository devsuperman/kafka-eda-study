using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Inventory.Data;
using KafkaEdaStudy.Inventory.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Npgsql;

namespace KafkaEdaStudy.Inventory.Consumers;

/// <summary>Handles <see cref="OrderPlaced"/>: reserve all lines or none, then answer with an inventory event.</summary>
public sealed class ReserveStockHandler(
    InventoryDbContext db,
    IInbox inbox,
    IOutbox outbox,
    TimeProvider clock,
    IOptions<InventoryOptions> options,
    ILogger<ReserveStockHandler> logger)
{
    public const int MaxAttempts = 5;

    /// <summary>Name of this handler in <c>processed_messages</c>.</summary>
    public const string InboxConsumer = OrderEventsConsumer.GroupId;

    // WHY A RETRY LOOP: Kafka only orders messages with the same key (orderId) inside one partition, and
    // each partition is read by one replica. That protects one order's events from racing each other, but
    // two DIFFERENT orders for the same SKU usually sit on different partitions, read by different replicas
    // at the same moment. Both read stock_items "KB-001" (available = 3), both decide 2 fits, both write 1:
    // a lost update that over-reserves stock. The shared row is not protected by any Kafka ordering.
    // Optimistic concurrency fixes it: the UPDATE carries "WHERE xmin = <value we read>", so the second
    // writer updates 0 rows, EF throws DbUpdateConcurrencyException, the whole transaction rolls back, and
    // we reload the fresh stock and decide again (maybe failing this time, which is correct).
    public async Task HandleAsync(EventEnvelope envelope, OrderPlaced placed, CancellationToken ct)
    {
        // Poison message (step 5): Orders.Api only accepts known SKUs, so an unknown one means bad or injected
        // data. Retrying can never fix it, so skip the retries and let the consumer base dead-letter it.
        // (A KNOWN SKU with zero stock is a normal business outcome: StockReservationFailed below.)
        var unknown = placed.Lines.Select(l => l.Sku).Where(sku => Products.Find(sku) is null).Distinct().ToList();
        if (unknown.Count > 0)
            throw new PoisonMessageException($"OrderPlaced {envelope.EventId} has unknown SKUs: {string.Join(", ", unknown)}");

        var timeout = options.Value.ReservationTimeout;

        for (var attempt = 1; ; attempt++)
        {
            db.ChangeTracker.Clear();

            // One transaction for: inbox row + stock updates + reservation + outgoing event (outbox row).
            // Disposing it without CommitAsync (any exception, or `continue`) rolls ALL of it back.
            await using var tx = await db.Database.BeginTransactionAsync(ct);

            // Inbox (step 4): a redelivered or re-sent event with the same eventId stops here.
            if (!await inbox.TryMarkProcessedAsync(envelope.EventId, InboxConsumer, ct))
            {
                logger.LogInformation(
                    "Instance {Instance}: duplicate {EventId} ignored ({EventType} for order {OrderId})",
                    Environment.MachineName, envelope.EventId, envelope.EventType, envelope.OrderId);
                return;
            }

            var skus = placed.Lines.Select(l => l.Sku).Distinct().ToList();
            var stock = await db.StockItems.Where(s => skus.Contains(s.Sku)).ToDictionaryAsync(s => s.Sku, ct);

            var result = Reservation.TryReserve(envelope.OrderId, placed.Lines, stock, clock.GetUtcNow(), timeout);
            if (result.Reservation is not { } reservation)
            {
                outbox.Add(Topics.Inventory, EventEnvelope.CausedBy(envelope, new StockReservationFailed(result.MissingSkus)));
                await db.SaveChangesAsync(ct);
                await tx.CommitAsync(ct);
                logger.LogInformation(
                    "Instance {Instance}: stock reservation failed for order {OrderId}, missing {MissingSkus}",
                    Environment.MachineName, envelope.OrderId, string.Join(", ", result.MissingSkus));
                return;
            }

            db.Reservations.Add(reservation);
            outbox.Add(Topics.Inventory, EventEnvelope.CausedBy(envelope, new StockReserved(reservation.Id, reservation.ExpiresAt)));
            try
            {
                await db.SaveChangesAsync(ct);
                await tx.CommitAsync(ct);
            }
            catch (DbUpdateConcurrencyException) when (attempt < MaxAttempts)
            {
                logger.LogWarning(
                    "Instance {Instance}: concurrent stock update while reserving order {OrderId} (attempt {Attempt}/{MaxAttempts}); reloading and retrying",
                    Environment.MachineName, envelope.OrderId, attempt, MaxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(10, 50) * attempt), ct);
                continue;
            }
            catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
            {
                // Second guard behind the inbox: the unique index on reservations.order_id. It fires when the
                // same order arrives again under a DIFFERENT eventId (e.g. a re-published OrderPlaced).
                // Everything in this transaction is rolled back; the existing reservation stands.
                logger.LogInformation(
                    "Instance {Instance}: reservation for order {OrderId} already exists (event {EventId}); nothing to do",
                    Environment.MachineName, envelope.OrderId, envelope.EventId);
                return;
            }

            logger.LogInformation(
                "Instance {Instance}: reserved stock for order {OrderId}: reservation {ReservationId}, expires {ExpiresAt} (attempt {Attempt})",
                Environment.MachineName, envelope.OrderId, reservation.Id, reservation.ExpiresAt, attempt);
            return;
        }
    }
}
