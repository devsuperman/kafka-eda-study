using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Notifications.Data;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Notifications;

/// <summary>
/// Pub/sub subscriber: its own consumer group ("notifications") gets its own copy of every order event,
/// independently of any other service reading the same topic.
/// </summary>
public sealed class OrderEventsConsumer(
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger,
    IServiceScopeFactory scopeFactory,
    TimeProvider clock)
    : KafkaConsumerService(options, logger, ConsumerGroup, [Topics.Orders])
{
    public const string ConsumerGroup = "notifications";

    protected override async Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        // Rule N1: which events the customer hears about, and what we tell them.
        var (kind, message) = envelope.EventType switch
        {
            nameof(OrderPlaced) => ("placed", $"Your order {envelope.OrderId} was placed, total {envelope.GetPayload<OrderPlaced>().Total}."),
            nameof(OrderConfirmed) => ("confirmed", $"Your order {envelope.OrderId} is confirmed."),
            nameof(OrderCancelled) => ("cancelled", $"Your order {envelope.OrderId} was cancelled ({envelope.GetPayload<OrderCancelled>().Reason})."),
            nameof(OrderShipped) => ("shipped", $"Your order {envelope.OrderId} has shipped."),
            nameof(OrderDelivered) => ("delivered", $"Your order {envelope.OrderId} was delivered."),
            _ => (null, null),
        };
        if (kind is null || message is null)
        {
            Logger.LogDebug("Ignoring {EventType} for order {OrderId}", envelope.EventType, envelope.OrderId);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<NotificationsDbContext>();
        var inbox = scope.ServiceProvider.GetRequiredService<IInbox>();

        // Same pattern as every handler: tx -> inbox check -> change -> SaveChanges -> commit -> (base) Kafka commit.
        await using var tx = await db.Database.BeginTransactionAsync(cancellationToken);
        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, ConsumerGroup, cancellationToken))
        {
            // Rule N2: never notify twice for the same event.
            Logger.LogInformation("Duplicate {EventId} ignored ({EventType} for order {OrderId})",
                envelope.EventId, envelope.EventType, envelope.OrderId);
            return;
        }

        db.Notifications.Add(new Notification
        {
            Id = Guid.NewGuid(),
            OrderId = envelope.OrderId,
            EventId = envelope.EventId,
            Kind = kind,
            Message = message,
            CreatedAt = clock.GetUtcNow(),
        });
        await db.SaveChangesAsync(cancellationToken);
        await tx.CommitAsync(cancellationToken);

        // The "send" happens after the commit. With a real email provider this would itself need care
        // (an outbox of emails, or an idempotency key at the provider): a crash right here = no email.
        Logger.LogInformation("Notify {Kind}: {Message} (event {EventId}, correlation {CorrelationId})",
            kind, message, envelope.EventId, envelope.CorrelationId);
    }
}
