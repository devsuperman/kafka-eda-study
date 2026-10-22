using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Playground.Observer;

/// <summary>
/// Read-only observer of every topic (and its DLQ). It is deliberately NOT a KafkaConsumerService:
/// that base class skips messages it cannot deserialize, while the playground must show poison
/// messages too.
/// </summary>
/// <remarks>
/// The group id (<see cref="PlaygroundOptions.ObserverGroupId"/>, default "playground") has auto-commit
/// off and no manual commits, which means the group never stores an
/// offset. With AutoOffsetReset=Earliest every start therefore begins at the beginning of each
/// partition and replays the topic history into memory — the log is the source of truth, and an
/// observer can rebuild its view from it at any time. Not committing also means the observer never
/// affects the business consumers, which have their own consumer groups and offsets.
/// </remarks>
public sealed class KafkaObserver(
    IOptions<KafkaOptions> options,
    IOptions<PlaygroundOptions> playground,
    EventStore store,
    ILogger<KafkaObserver> logger) : BackgroundService
{
    /// <summary>Observer groups are named <c>playground*</c> by convention (plus whatever is configured).</summary>
    public static bool IsObserverGroup(string groupId, string configuredGroupId) =>
        groupId == configuredGroupId || groupId.StartsWith("playground", StringComparison.Ordinal);

    private string GroupId => playground.Value.ObserverGroupId;

    public static readonly IReadOnlyList<string> ObservedTopics =
        [.. Topics.All, .. Topics.All.Select(Topics.Dlq)];

    // Consume() is a blocking call, so the loop runs on its own thread instead of blocking startup.
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        })
        .SetErrorHandler((_, error) => logger.LogWarning("[{Group}] Kafka error: {Reason}", GroupId, error.Reason))
        .Build();

        consumer.Subscribe(ObservedTopics);
        logger.LogInformation("[{Group}] observing {Topics}", GroupId, string.Join(", ", ObservedTopics));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                if (result?.Message is null)
                    continue;
                store.Add(ObservedEvent.From(result));
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                // e.g. a DLQ topic that does not exist yet; keep observing the others.
                logger.LogWarning("[{Group}] consume error: {Reason}", GroupId, ex.Error.Reason);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{Group}] unexpected error while observing", GroupId);
            }
        }

        consumer.Close();
    }
}
