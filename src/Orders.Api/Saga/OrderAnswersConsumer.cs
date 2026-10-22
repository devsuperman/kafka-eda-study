using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Orders.Saga;

/// <summary>
/// Consumer group "orders": reads the answers other contexts give to an order (stock, payment, shipment).
/// Partitions are shared by key, so each order's answers are handled in the order they were written.
/// </summary>
public sealed class OrderAnswersConsumer(
    IOptions<KafkaOptions> options,
    ILogger<OrderAnswersConsumer> logger,
    IServiceScopeFactory scopeFactory)
    : KafkaConsumerService(options, logger, ConsumerGroup, [Topics.Inventory, Topics.Payments, Topics.Shipping])
{
    public const string ConsumerGroup = "orders";

    protected override async Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<OrderSagaHandler>().HandleAsync(envelope, cancellationToken);
    }
}
