using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.BuildingBlocks.Kafka;

/// <summary>
/// A message that can never be processed, however often it is retried (e.g. an <c>OrderPlaced</c> with an
/// unknown SKU). Throwing it skips the retries and sends the message straight to the dead-letter topic.
/// </summary>
public sealed class PoisonMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>
/// Base consumer loop: subscribe, deserialize the envelope, handle, then commit the offset (at-least-once).
/// Auto-commit is off, so a crash before the commit means the message is delivered again.
/// <para>
/// Errors (step 5): a failing handler is retried in-process with backoff (<see cref="RetryDelays"/>), because
/// most errors are transient (database restart, deadlock, broker hiccup). When every attempt fails, or the
/// message can never succeed (cannot be deserialized, or <see cref="PoisonMessageException"/>), the ORIGINAL
/// message is copied to <c>&lt;topic&gt;.dlq</c> with error headers and the offset is committed, so one bad
/// message does not block its partition forever. While a message is retried, the messages behind it in the
/// same partition wait: that is the price of keeping per-key order.
/// </para>
/// </summary>
public abstract class KafkaConsumerService(
    IOptions<KafkaOptions> options,
    ILogger logger,
    string groupId,
    IReadOnlyCollection<string> topics) : BackgroundService
{
    private static readonly TimeSpan DeadLetterFailureDelay = TimeSpan.FromSeconds(2);

    protected ILogger Logger { get; } = logger;

    /// <summary>
    /// Wait before each retry. The handler runs <c>RetryDelays.Count + 1</c> times at most
    /// (default: 3 attempts, waiting 200 ms and then 1 s).
    /// </summary>
    protected virtual IReadOnlyList<TimeSpan> RetryDelays { get; } =
        [TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1)];

    protected abstract Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken);

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        // The client id shows up in consumer-group tools (Kafka UI, Playground), so replicas are distinguishable.
        var clientId = $"{groupId}@{Environment.MachineName}";
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = groupId,
            ClientId = clientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        })
        .SetPartitionsAssignedHandler((_, partitions) =>
            Logger.LogInformation("[{Group}@{Instance}] assigned {Partitions}", groupId, Environment.MachineName, string.Join(", ", partitions)))
        .SetPartitionsRevokedHandler((_, partitions) =>
            Logger.LogInformation("[{Group}@{Instance}] revoked {Partitions}", groupId, Environment.MachineName, string.Join(", ", partitions)))
        .Build();

        // A plain producer for the dead-letter topics: it copies the original bytes, not a new envelope.
        using var dlqProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = $"{clientId}-dlq",
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 30_000,
        }).Build();

        consumer.Subscribe(topics);
        Logger.LogInformation("[{Group}] subscribed to {Topics}", groupId, string.Join(", ", topics));

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string> result;
            try
            {
                result = consumer.Consume(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                Logger.LogWarning(ex, "[{Group}] consume error: {Reason}", groupId, ex.Error.Reason);
                continue;
            }

            bool done;
            try
            {
                done = await ProcessAsync(result, dlqProducer, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break; // shutting down mid-message: no commit, the message is redelivered after restart
            }

            if (done)
            {
                consumer.Commit(result);
                continue;
            }

            // Not handled AND not dead-lettered (the DLQ publish failed too). Committing a LATER offset of this
            // partition would silently skip this message, so rewind to it and try the whole thing again.
            try
            {
                consumer.Seek(result.TopicPartitionOffset);
                await Task.Delay(DeadLetterFailureDelay, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (KafkaException ex)
            {
                // E.g. the partition was revoked meanwhile: its new owner starts from the last committed offset.
                Logger.LogWarning(ex, "[{Group}] could not rewind to {Offset}", groupId, result.TopicPartitionOffset);
            }
        }

        consumer.Close();
    }

    /// <summary>Returns true when the offset may be committed (handled, or safely in the DLQ).</summary>
    private async Task<bool> ProcessAsync(
        ConsumeResult<string, string> result, IProducer<string, string> dlqProducer, CancellationToken stoppingToken)
    {
        EventEnvelope envelope;
        try
        {
            envelope = KafkaMessage.ToEnvelope(result.Message);
        }
        catch (Exception ex)
        {
            // Retrying would deserialize the same bytes the same way: straight to the DLQ.
            Logger.LogError(ex, "[{Group}] could not deserialize message at {Offset}", groupId, result.TopicPartitionOffset);
            return await DeadLetterAsync(dlqProducer, result, ex, attempts: 0, stoppingToken);
        }

        var parent = KafkaMessage.Header(result.Message, KafkaHeaderNames.TraceParent);
        using var activity = Telemetry.Source.StartActivity(
            $"consume {envelope.EventType}", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.consumer.group.name", groupId);
        activity?.SetTag("order.id", envelope.OrderId);

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await HandleAsync(envelope, result, stoppingToken);
                return true;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ex is PoisonMessageException or JsonException)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                Logger.LogError(ex, "[{Group}] poison {EventType} {EventId}: {Error}; no retries",
                    groupId, envelope.EventType, envelope.EventId, ex.Message);
                return await DeadLetterAsync(dlqProducer, result, ex, attempt, stoppingToken);
            }
            catch (Exception ex) when (attempt <= RetryDelays.Count)
            {
                var delay = RetryDelays[attempt - 1];
                Logger.LogWarning(ex, "[{Group}] attempt {Attempt}/{MaxAttempts} for {EventType} {EventId} failed: {Error}; retrying in {Delay}",
                    groupId, attempt, RetryDelays.Count + 1, envelope.EventType, envelope.EventId, ex.Message, delay);
                await Task.Delay(delay, stoppingToken);
            }
            catch (Exception ex)
            {
                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
                Logger.LogError(ex, "[{Group}] {EventType} {EventId} failed after {Attempts} attempts",
                    groupId, envelope.EventType, envelope.EventId, attempt);
                return await DeadLetterAsync(dlqProducer, result, ex, attempt, stoppingToken);
            }
        }
    }

    /// <summary>
    /// Copies the original message (same key, value and headers) to <c>&lt;topic&gt;.dlq</c>, adding headers that
    /// say why and where from. Never throws: returns false when even the DLQ publish failed.
    /// </summary>
    private async Task<bool> DeadLetterAsync(
        IProducer<string, string> dlqProducer, ConsumeResult<string, string> result, Exception error, int attempts,
        CancellationToken stoppingToken)
    {
        var dlqTopic = Topics.Dlq(result.Topic);
        var headers = new Headers();
        foreach (var header in result.Message.Headers ?? [])
            headers.Add(header.Key, header.GetValueBytes());
        void Add(string name, string value) => headers.Add(name, Encoding.UTF8.GetBytes(value));
        Add(KafkaHeaderNames.Error, $"{error.GetType().Name}: {error.Message}");
        Add(KafkaHeaderNames.OriginalTopic, result.Topic);
        Add(KafkaHeaderNames.OriginalPartition, result.Partition.Value.ToString(CultureInfo.InvariantCulture));
        Add(KafkaHeaderNames.OriginalOffset, result.Offset.Value.ToString(CultureInfo.InvariantCulture));
        Add(KafkaHeaderNames.ConsumerGroup, groupId);
        Add(KafkaHeaderNames.FailedAt, DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture));
        Add(KafkaHeaderNames.Attempts, attempts.ToString(CultureInfo.InvariantCulture));

        try
        {
            var delivery = await dlqProducer.ProduceAsync(dlqTopic, new Message<string, string>
            {
                Key = result.Message.Key,
                Value = result.Message.Value,
                Headers = headers,
            }, stoppingToken);
            Logger.LogWarning("[{Group}] dead-lettered {Offset} to {DlqTopic} [partition {Partition}, offset {DlqOffset}]",
                groupId, result.TopicPartitionOffset, dlqTopic, delivery.Partition.Value, delivery.Offset.Value);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Logger.LogError(ex, "[{Group}] could not publish {Offset} to {DlqTopic}; not committing, will retry",
                groupId, result.TopicPartitionOffset, dlqTopic);
            return false;
        }
    }
}
