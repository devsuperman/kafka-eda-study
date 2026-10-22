using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.IntegrationTests;

/// <summary>Step 2: the key (orderId) decides the partition, so every event of one order keeps its order.</summary>
public sealed class PartitioningTests(InfraFixture infra)
{
    [Fact]
    public async Task All_events_of_one_order_land_in_the_same_partition_in_publish_order()
    {
        var ct = TestContext.Current.CancellationToken;
        var orderIds = Enumerable.Range(0, 12).Select(_ => Guid.NewGuid()).ToList();
        var published = new List<EventEnvelope>();

        using (var publisher = new KafkaEventPublisher(
            Options.Create(new KafkaOptions { BootstrapServers = infra.BootstrapServers }),
            NullLogger<KafkaEventPublisher>.Instance))
        {
            foreach (var orderId in orderIds)
            {
                var placed = EventEnvelope.Create(
                    new OrderPlaced(Guid.NewGuid(), [new OrderLine("KB-001", 1, 120m)], 120m), orderId, orderId);
                var confirmed = EventEnvelope.CausedBy(placed, new OrderConfirmed());
                var shipped = EventEnvelope.CausedBy(confirmed, new OrderShipped(Guid.NewGuid()));
                foreach (var envelope in new[] { placed, confirmed, shipped })
                {
                    await publisher.PublishAsync(Topics.Orders, envelope, ct);
                    published.Add(envelope);
                }
            }
        }

        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = infra.BootstrapServers,
            GroupId = $"partitioning-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).Build();
        consumer.Subscribe(Topics.Orders);

        var wanted = published.Select(e => e.EventId).ToHashSet();
        var received = new List<(EventEnvelope Envelope, int Partition)>();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (received.Count < wanted.Count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null)
                continue;
            var envelope = KafkaMessage.ToEnvelope(result.Message);
            if (wanted.Contains(envelope.EventId))
                received.Add((envelope, result.Partition.Value));
        }

        Assert.Equal(wanted.Count, received.Count);
        foreach (var orderId in orderIds)
        {
            var events = received.Where(r => r.Envelope.OrderId == orderId).ToList();
            Assert.Single(events.Select(e => e.Partition).Distinct());
            Assert.Equal(
                [nameof(OrderPlaced), nameof(OrderConfirmed), nameof(OrderShipped)],
                events.Select(e => e.Envelope.EventType));
        }

        // With 12 random keys over 3 partitions, the orders spread over more than one partition.
        Assert.True(received.Select(r => r.Partition).Distinct().Count() > 1);
    }
}
