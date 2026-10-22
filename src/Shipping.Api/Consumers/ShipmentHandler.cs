using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Shipping.Data;
using KafkaEdaStudy.Shipping.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Shipping.Consumers;

/// <summary>Creates the shipment of a confirmed order (rule S1, flow 1).</summary>
public sealed class ShipmentHandler(
    ShippingDbContext db,
    IInbox inbox,
    IOutbox outbox,
    TimeProvider clock,
    ILogger<ShipmentHandler> logger)
{
    /// <summary>Name of this handler in <c>processed_messages</c>.</summary>
    public const string InboxConsumer = "shipping";

    public async Task CreateForConfirmedOrderAsync(EventEnvelope envelope, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, InboxConsumer, ct))
        {
            logger.LogInformation("Duplicate {EventId} ignored (OrderConfirmed for order {OrderId})", envelope.EventId, envelope.OrderId);
            return;
        }

        if (await db.Shipments.AnyAsync(s => s.OrderId == envelope.OrderId, ct))
        {
            logger.LogInformation("Order {OrderId} already has a shipment; nothing to do", envelope.OrderId);
        }
        else
        {
            var shipment = Shipment.Create(envelope.OrderId, envelope.CorrelationId, clock.GetUtcNow());
            db.Shipments.Add(shipment);
            outbox.Add(Topics.Shipping, EventEnvelope.CausedBy(envelope, new ShipmentCreated(shipment.Id)));
            logger.LogInformation("Order {OrderId}: shipment {ShipmentId} created", envelope.OrderId, shipment.Id);
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }
}
