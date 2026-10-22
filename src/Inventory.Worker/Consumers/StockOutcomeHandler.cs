using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Inventory.Data;
using KafkaEdaStudy.Inventory.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Inventory.Consumers;

/// <summary>
/// Settles an order's reservation once the order's fate is known (step 7): releases it when the order is cancelled
/// (rule I4, flows 2, 3 and 5) and commits it when the order ships (rule I4, flow 1).
/// </summary>
/// <remarks>
/// Both paths rely on the key ordering of orders.events: the OrderPlaced that created the reservation is handled
/// before the OrderCancelled or OrderShipped of the same order, so the reservation exists by then. A concurrent
/// write to the same stock row throws DbUpdateConcurrencyException, and the consumer base retries the message.
/// </remarks>
public sealed class StockOutcomeHandler(InventoryDbContext db, IInbox inbox, IOutbox outbox, ILogger<StockOutcomeHandler> logger)
{
    public Task ReleaseAsync(EventEnvelope envelope, OrderCancelled cancelled, CancellationToken ct) =>
        SettleAsync(envelope, "release", ct,
            (reservation, stock) => reservation.Release(stock),
            reservation => new StockReleased(reservation.Id, cancelled.Reason));

    public Task CommitAsync(EventEnvelope envelope, CancellationToken ct) =>
        SettleAsync(envelope, "commit", ct,
            (reservation, stock) => reservation.Commit(stock),
            reservation => new StockCommitted(reservation.Id));

    private async Task SettleAsync<TAnnouncement>(
        EventEnvelope envelope,
        string action,
        CancellationToken ct,
        Func<Reservation, IReadOnlyDictionary<string, StockItem>, bool> settle,
        Func<Reservation, TAnnouncement> announce)
        where TAnnouncement : notnull
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, ReserveStockHandler.InboxConsumer, ct))
        {
            logger.LogInformation("Duplicate {EventId} ignored ({EventType} for order {OrderId})",
                envelope.EventId, envelope.EventType, envelope.OrderId);
            return;
        }

        var reservation = await db.Reservations.Include(r => r.Lines).FirstOrDefaultAsync(r => r.OrderId == envelope.OrderId, ct);
        if (reservation is null)
        {
            // The order never reserved stock (e.g. it was out of stock): there is nothing to give back.
            logger.LogInformation("No reservation for order {OrderId}; nothing to {Action}", envelope.OrderId, action);
            await tx.CommitAsync(ct);
            return;
        }

        var skus = reservation.Lines.Select(l => l.Sku).ToList();
        var stock = await db.StockItems.Where(s => skus.Contains(s.Sku)).ToDictionaryAsync(s => s.Sku, ct);

        if (settle(reservation, stock))
        {
            outbox.Add(Topics.Inventory, EventEnvelope.CausedBy(envelope, announce(reservation)));
            logger.LogInformation("Order {OrderId}: {Action} of reservation {ReservationId}", envelope.OrderId, action, reservation.Id);
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }
}
