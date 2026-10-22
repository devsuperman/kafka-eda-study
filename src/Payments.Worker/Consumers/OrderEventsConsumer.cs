using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Payments.Consumers;

/// <summary>
/// Consumer group "payments": one replica per partition of orders.events. Same key (orderId) means the events of one
/// order are read in order: OrderPlaced is always handled before the OrderCancelled or OrderShipped of that order.
/// </summary>
public sealed class OrderEventsConsumer(
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger,
    IServiceScopeFactory scopeFactory)
    : KafkaConsumerService(options, logger, ConsumerGroup, [Topics.Orders])
{
    public const string ConsumerGroup = "payments";

    protected override async Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredService<PaymentHandler>();

        if (envelope.Is<OrderPlaced>())
            await handler.AuthorizeAsync(envelope, envelope.GetPayload<OrderPlaced>(), cancellationToken);
        else if (envelope.Is<OrderCancelled>())
            await handler.VoidAsync(envelope, cancellationToken);
        else if (envelope.Is<OrderShipped>())
            await handler.CaptureAsync(envelope, cancellationToken);
        else
            Logger.LogDebug("Ignoring {EventType} for order {OrderId}", envelope.EventType, envelope.OrderId);
    }
}
