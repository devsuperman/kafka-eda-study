using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Inventory.Consumers;

/// <summary>
/// Consumer group "inventory": every replica joins the same group, so Kafka splits the partitions of
/// orders.events between them (3 partitions, 3 replicas = one partition each). Each event is handled by
/// exactly one replica of the group.
/// </summary>
public sealed class OrderEventsConsumer(
    IOptions<KafkaOptions> options,
    ILoggerFactory loggerFactory,
    IServiceScopeFactory scopeFactory)
    : KafkaConsumerService(options, loggerFactory.CreateLogger(LoggerCategory), GroupId, [Topics.Orders])
{
    public const string GroupId = "inventory";

    // The base class logs partition assignments/revocations with this logger. The console log line shows
    // the category, so putting the instance name in it tells you which replica got which partition.
    private static readonly string LoggerCategory =
        $"{typeof(OrderEventsConsumer).FullName}@{Environment.MachineName}";

    protected override async Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken)
    {
        Logger.LogInformation(
            "Instance {Instance} handling {EventType} for order {OrderId} from {TopicPartitionOffset}",
            Environment.MachineName, envelope.EventType, envelope.OrderId, result.TopicPartitionOffset);

        await using var scope = scopeFactory.CreateAsyncScope();
        if (envelope.Is<OrderPlaced>())
        {
            await scope.ServiceProvider.GetRequiredService<ReserveStockHandler>()
                .HandleAsync(envelope, envelope.GetPayload<OrderPlaced>(), cancellationToken);
        }
        else if (envelope.Is<OrderCancelled>())
        {
            await scope.ServiceProvider.GetRequiredService<StockOutcomeHandler>()
                .ReleaseAsync(envelope, envelope.GetPayload<OrderCancelled>(), cancellationToken);
        }
        else if (envelope.Is<OrderShipped>())
        {
            await scope.ServiceProvider.GetRequiredService<StockOutcomeHandler>()
                .CommitAsync(envelope, cancellationToken);
        }
        else
        {
            Logger.LogDebug("Ignoring {EventType} for order {OrderId}", envelope.EventType, envelope.OrderId);
        }
    }
}
