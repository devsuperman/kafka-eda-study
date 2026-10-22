using System.Diagnostics;
using Confluent.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.BuildingBlocks.Kafka;

public interface IEventPublisher
{
    Task PublishAsync(string topic, EventEnvelope envelope, CancellationToken cancellationToken = default);
}

public sealed class KafkaEventPublisher : IEventPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaEventPublisher> _logger;

    public KafkaEventPublisher(IOptions<KafkaOptions> options, ILogger<KafkaEventPublisher> logger)
    {
        _logger = logger;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            // Shown in broker/consumer-group tools instead of the default "rdkafka".
            ClientId = $"{System.Reflection.Assembly.GetEntryAssembly()?.GetName().Name ?? "app"}@{Environment.MachineName}",
            Acks = Acks.All,
            EnableIdempotence = true,
            // Fail a publish after 30 s instead of the 5 min default, so the outbox relay records the error sooner.
            MessageTimeoutMs = 30_000,
        }).Build();
    }

    public async Task PublishAsync(string topic, EventEnvelope envelope, CancellationToken cancellationToken = default)
    {
        using var activity = Telemetry.Source.StartActivity($"publish {envelope.EventType}", ActivityKind.Producer);
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("order.id", envelope.OrderId);

        var result = await _producer.ProduceAsync(topic, KafkaMessage.From(envelope, activity?.Id), cancellationToken);
        _logger.LogInformation(
            "Published {EventType} for order {OrderId} to {Topic} [partition {Partition}, offset {Offset}]",
            envelope.EventType, envelope.OrderId, topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
