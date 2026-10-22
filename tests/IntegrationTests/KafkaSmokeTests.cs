using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.IntegrationTests;

public sealed class KafkaSmokeTests(InfraFixture infra)
{
    [Fact]
    public async Task Published_envelope_round_trips_with_order_id_as_key()
    {
        var ct = TestContext.Current.CancellationToken;
        var orderId = Guid.NewGuid();
        var placed = new OrderPlaced(Guid.NewGuid(), [new OrderLine("KB-001", 1, 120m)], 120m);
        var envelope = EventEnvelope.Create(placed, orderId, correlationId: orderId);

        using (var publisher = new KafkaEventPublisher(
            Options.Create(new KafkaOptions { BootstrapServers = infra.BootstrapServers }),
            NullLogger<KafkaEventPublisher>.Instance))
        {
            await publisher.PublishAsync(Topics.Orders, envelope, ct);
        }

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = infra.BootstrapServers,
            GroupId = $"smoke-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(Topics.Orders);

        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result?.Message.Key != orderId.ToString())
                continue;

            var received = KafkaMessage.ToEnvelope(result.Message);
            Assert.Equal(envelope.EventId, received.EventId);
            Assert.Equal(nameof(OrderPlaced), KafkaMessage.Header(result.Message, KafkaHeaderNames.EventType));
            Assert.Equal(120m, received.GetPayload<OrderPlaced>().Total);
            return;
        }

        Assert.Fail("OrderPlaced was not consumed within 30 seconds.");
    }
}
