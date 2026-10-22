using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// Raw Kafka producer used ONLY by the fault-injection scenarios (duplicate events, poison messages).
/// </summary>
/// <remarks>
/// This is a development/study tool, not a pattern to copy: real services publish through
/// <see cref="IEventPublisher"/> (and later the outbox), never by writing arbitrary bytes onto another
/// context's topic. The playground does it on purpose to show how consumers cope with the faults that
/// at-least-once delivery and buggy producers cause in real life. One singleton instance: a producer
/// holds broker connections and a background thread, so it is created once and disposed with the app.
/// </remarks>
public sealed class FaultInjectionProducer : IDisposable
{
    private readonly IProducer<string?, string> _producer;

    public FaultInjectionProducer(IOptions<KafkaOptions> options)
    {
        _producer = new ProducerBuilder<string?, string>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = "playground-fault-injector",
            Acks = Acks.All,
            MessageTimeoutMs = 10_000, // fail the HTTP request reasonably fast when Kafka is down
        }).Build();
    }

    /// <summary>Produces the record exactly as given and returns where it landed.</summary>
    /// <exception cref="DownstreamException">Kafka rejected or timed out the write.</exception>
    public async Task<TopicPartitionOffset> ProduceAsync(
        string topic, Message<string?, string> message, CancellationToken cancellationToken)
    {
        try
        {
            var result = await _producer.ProduceAsync(topic, message, cancellationToken);
            return result.TopicPartitionOffset;
        }
        catch (ProduceException<string?, string> ex)
        {
            throw new DownstreamException($"Kafka did not accept the record: {ex.Error.Reason}", ex);
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
