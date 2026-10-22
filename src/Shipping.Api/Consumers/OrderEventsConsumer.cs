using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Shipping.Consumers;

/// <summary>Consumer group "shipping": reads orders.events and creates a shipment for each OrderConfirmed.</summary>
public sealed class OrderEventsConsumer(
    IOptions<KafkaOptions> options,
    ILogger<OrderEventsConsumer> logger,
    IServiceScopeFactory scopeFactory)
    : KafkaConsumerService(options, logger, ConsumerGroup, [Topics.Orders])
{
    public const string ConsumerGroup = "shipping";

    protected override async Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        if (!envelope.Is<OrderConfirmed>())
        {
            Logger.LogDebug("Ignoring {EventType} for order {OrderId}", envelope.EventType, envelope.OrderId);
            return;
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<ShipmentHandler>()
            .CreateForConfirmedOrderAsync(envelope, cancellationToken);
    }
}
