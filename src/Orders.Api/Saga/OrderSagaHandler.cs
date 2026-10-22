using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Orders.Data;
using KafkaEdaStudy.Orders.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Orders.Saga;

/// <summary>
/// The order's side of the choreography (step 7): reacts to the answers of Inventory, Payments and Shipping.
/// Confirms the order when both stock and payment answered (O4), cancels it when either says no (flows 2 and 3),
/// and moves it to Shipped and Delivered when the shipment reports it (flow 1).
/// </summary>
public sealed class OrderSagaHandler(
    OrdersDbContext db,
    IInbox inbox,
    IOutbox outbox,
    TimeProvider clock,
    ILogger<OrderSagaHandler> logger)
{
    /// <summary>Name of this handler in <c>processed_messages</c>.</summary>
    public const string InboxConsumer = "orders";

    public async Task HandleAsync(EventEnvelope envelope, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);

        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, InboxConsumer, ct))
        {
            logger.LogInformation("Duplicate {EventId} ignored ({EventType} for order {OrderId})",
                envelope.EventId, envelope.EventType, envelope.OrderId);
            return;
        }

        var order = await db.Orders.FirstOrDefaultAsync(o => o.Id == envelope.OrderId, ct);
        if (order is null)
        {
            logger.LogWarning("{EventType} for unknown order {OrderId}; nothing to do", envelope.EventType, envelope.OrderId);
            await tx.CommitAsync(ct);
            return;
        }

        var now = clock.GetUtcNow();
        switch (envelope.EventType)
        {
            case nameof(StockReserved) when order.RecordStockReserved(now):
                ConfirmIfReady(order, envelope, now);
                break;

            case nameof(PaymentAuthorized) when order.RecordPaymentAuthorized(now):
                ConfirmIfReady(order, envelope, now);
                break;

            case nameof(StockReservationFailed):
                Cancel(order, envelope, CancelReasons.OutOfStock, now);
                break;

            case nameof(PaymentDeclined):
                Cancel(order, envelope, CancelReasons.PaymentDeclined, now);
                break;

            case nameof(ShipmentDispatched) when order.Ship(now):
                var dispatched = envelope.GetPayload<ShipmentDispatched>();
                outbox.Add(Topics.Orders, EventEnvelope.CausedBy(envelope, new OrderShipped(dispatched.ShipmentId)));
                break;

            case nameof(ShipmentDelivered) when order.Deliver(now):
                var delivered = envelope.GetPayload<ShipmentDelivered>();
                outbox.Add(Topics.Orders, EventEnvelope.CausedBy(envelope, new OrderDelivered(delivered.DeliveredAt)));
                break;

            default:
                // An answer that does not change the order any more (e.g. stock arriving after a cancel): ignored.
                logger.LogInformation("{EventType} for order {OrderId} in status {Status} changes nothing",
                    envelope.EventType, envelope.OrderId, order.Status);
                break;
        }

        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }

    private void ConfirmIfReady(Order order, EventEnvelope cause, DateTimeOffset now)
    {
        if (!order.IsReadyToConfirm)
            return;

        order.Confirm(now);
        outbox.Add(Topics.Orders, EventEnvelope.CausedBy(cause, new OrderConfirmed()));
        logger.LogInformation("Order {OrderId} confirmed (rule O4: stock and payment both answered)", order.Id);
    }

    private void Cancel(Order order, EventEnvelope cause, string reason, DateTimeOffset now)
    {
        if (!order.Cancel(reason, now))
        {
            // Rule O6 / flow 6: the order was cancelled already (e.g. by the other failing answer) or has shipped.
            logger.LogInformation("Order {OrderId} is {Status}; {Reason} changes nothing", order.Id, order.Status, reason);
            return;
        }

        outbox.Add(Topics.Orders, EventEnvelope.CausedBy(cause, new OrderCancelled(reason)));
        logger.LogInformation("Order {OrderId} cancelled: {Reason}", order.Id, reason);
    }
}
