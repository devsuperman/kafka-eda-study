using System.Collections.Concurrent;
using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.IntegrationTests;

// Event types for this test only: the envelope's eventType is the record name.
public sealed record AlwaysFails;
public sealed record Poison;
public sealed record Fine;

/// <summary>Step 5: retries with backoff, then the dead-letter topic; poison messages skip the retries.</summary>
public sealed class DeadLetterTests(InfraFixture infra)
{
    private static string Header(Message<string, string> message, string name) =>
        KafkaMessage.Header(message, name) ?? throw new Xunit.Sdk.XunitException($"Missing header '{name}'.");

    [Fact]
    public async Task Failing_and_poison_messages_end_in_the_dlq_and_the_consumer_keeps_going()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = $"dlq-test-{Guid.NewGuid():N}";
        await MessagingTestSupport.CreateTopicsAsync(infra, topic, Topics.Dlq(topic));

        var orderId = Guid.NewGuid();
        var failing = EventEnvelope.Create(new AlwaysFails(), orderId, orderId);
        var poison = EventEnvelope.Create(new Poison(), orderId, orderId);
        var fine = EventEnvelope.Create(new Fine(), orderId, orderId);

        // One partition, published in this order: the consumer must get past the first three to reach "fine".
        using (var publisher = new KafkaEventPublisher(
            Options.Create(new KafkaOptions { BootstrapServers = infra.BootstrapServers }),
            NullLogger<KafkaEventPublisher>.Instance))
        {
            await publisher.PublishAsync(topic, failing, ct);
            await publisher.PublishAsync(topic, poison, ct);
        }
        using (var raw = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = infra.BootstrapServers }).Build())
        {
            await raw.ProduceAsync(topic, new Message<string, string> { Key = orderId.ToString(), Value = "this is not json" }, ct);
            await raw.ProduceAsync(topic, KafkaMessage.From(fine), ct);
        }

        var attempts = new ConcurrentDictionary<string, int>();
        var handled = new ConcurrentQueue<Guid>();
        var consumer = new DelegateConsumer(infra, topic, (envelope, _) =>
        {
            attempts.AddOrUpdate(envelope.EventType, 1, (_, n) => n + 1);
            return envelope.EventType switch
            {
                nameof(AlwaysFails) => throw new InvalidOperationException("database is down (simulated)"),
                nameof(Poison) => throw new PoisonMessageException("unknown SKU XX-999 (simulated)"),
                _ => Task.Run(() => handled.Enqueue(envelope.EventId)),
            };
        }, retryDelays: [TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(100)]);

        await consumer.StartAsync(ct);
        try
        {
            await MessagingTestSupport.WaitUntilAsync(() => !handled.IsEmpty, TimeSpan.FromSeconds(30), ct);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        // The consumer kept going: the message behind the bad ones was handled.
        Assert.Equal([fine.EventId], handled);
        Assert.Equal(3, attempts[nameof(AlwaysFails)]); // retried: 3 attempts in total
        Assert.Equal(1, attempts[nameof(Poison)]);      // poison: no retries

        var dead = MessagingTestSupport.ReadTopic(infra, Topics.Dlq(topic), _ => true, expected: 3);
        Assert.Equal(3, dead.Count);

        // 1. Retries exhausted: the ORIGINAL message (key, value, headers) plus the error headers.
        var failed = dead[0].Message;
        Assert.Equal(orderId.ToString(), failed.Key);
        Assert.Equal(failing.EventId, KafkaMessage.ToEnvelope(failed).EventId);
        Assert.Equal(nameof(AlwaysFails), Header(failed, KafkaHeaderNames.EventType)); // original header kept
        Assert.Contains("database is down", Header(failed, KafkaHeaderNames.Error));
        Assert.Equal(topic, Header(failed, KafkaHeaderNames.OriginalTopic));
        Assert.Equal("0", Header(failed, KafkaHeaderNames.OriginalPartition));
        Assert.Equal("0", Header(failed, KafkaHeaderNames.OriginalOffset));
        Assert.StartsWith("test-", Header(failed, KafkaHeaderNames.ConsumerGroup));
        Assert.True(DateTimeOffset.TryParse(Header(failed, KafkaHeaderNames.FailedAt), out _));
        Assert.Equal("3", Header(failed, KafkaHeaderNames.Attempts));

        // 2. Poison: straight to the DLQ after one attempt.
        var poisoned = dead[1].Message;
        Assert.Equal(poison.EventId, KafkaMessage.ToEnvelope(poisoned).EventId);
        Assert.Contains(nameof(PoisonMessageException), Header(poisoned, KafkaHeaderNames.Error));
        Assert.Equal("1", Header(poisoned, KafkaHeaderNames.OriginalOffset));
        Assert.Equal("1", Header(poisoned, KafkaHeaderNames.Attempts));

        // 3. Not even an envelope: dead-lettered without calling the handler.
        var garbage = dead[2].Message;
        Assert.Equal("this is not json", garbage.Value);
        Assert.Equal("2", Header(garbage, KafkaHeaderNames.OriginalOffset));
        Assert.Equal("0", Header(garbage, KafkaHeaderNames.Attempts));
    }
}
