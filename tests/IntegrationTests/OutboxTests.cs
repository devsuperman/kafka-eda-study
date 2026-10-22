using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaEdaStudy.IntegrationTests;

/// <summary>Step 6: the transactional outbox and its relay.</summary>
public sealed class OutboxTests(InfraFixture infra)
{
    private static EventEnvelope NewOrderPlaced() =>
        EventEnvelope.Create(
            new OrderPlaced(Guid.NewGuid(), [new OrderLine("KB-001", 1, 120m)], 120m),
            Guid.NewGuid(), Guid.NewGuid());

    [Fact]
    public async Task Event_saved_with_the_state_is_published_once_a_relay_runs()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var services = await MessagingTestSupport.CreateServicesAsync(infra, ct);
        var envelope = NewOrderPlaced();

        // "Request": state change + outbox row in ONE SaveChanges. Then the process "crashes": no relay ran.
        await using (var scope = services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            db.Counters.Add(new Counter { Name = "orders", Value = 1 });
            scope.ServiceProvider.GetRequiredService<IOutbox>().Add(Topics.Orders, envelope);
            await db.SaveChangesAsync(ct);
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var row = await scope.ServiceProvider.GetRequiredService<TestDbContext>()
                .Set<OutboxMessage>().SingleAsync(m => m.EventId == envelope.EventId, ct);
            Assert.Null(row.SentAt);
        }

        // "Restart": the relay starts as a hosted service and finds the unsent row.
        var relay = ActivatorUtilities.CreateInstance<OutboxRelay<TestDbContext>>(services);
        await relay.StartAsync(ct);
        try
        {
            var received = MessagingTestSupport.ReadTopic(
                infra, Topics.Orders, r => r.Message.Key == envelope.OrderId.ToString(), expected: 1);

            var message = Assert.Single(received);
            Assert.Equal(envelope.EventId, KafkaMessage.ToEnvelope(message.Message).EventId);
            Assert.Equal(nameof(OrderPlaced), KafkaMessage.Header(message.Message, KafkaHeaderNames.EventType));
        }
        finally
        {
            await relay.StopAsync(CancellationToken.None);
        }

        await using (var scope = services.CreateAsyncScope())
        {
            var row = await scope.ServiceProvider.GetRequiredService<TestDbContext>()
                .Set<OutboxMessage>().SingleAsync(m => m.EventId == envelope.EventId, ct);
            Assert.NotNull(row.SentAt);
            Assert.Equal(1, row.Attempts);
        }
    }

    [Fact]
    public async Task Two_relays_running_concurrently_publish_each_row_exactly_once()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var services = await MessagingTestSupport.CreateServicesAsync(infra, ct);
        var envelopes = Enumerable.Range(0, 300).Select(_ => NewOrderPlaced()).ToList();

        await using (var scope = services.CreateAsyncScope())
        {
            var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
            foreach (var envelope in envelopes)
                outbox.Add(Topics.Orders, envelope);
            await scope.ServiceProvider.GetRequiredService<TestDbContext>().SaveChangesAsync(ct);
        }

        // Two relays (think: two replicas) drain the same table at the same time.
        var relayA = ActivatorUtilities.CreateInstance<OutboxRelay<TestDbContext>>(services);
        var relayB = ActivatorUtilities.CreateInstance<OutboxRelay<TestDbContext>>(services);
        async Task<int> DrainAsync(OutboxRelay<TestDbContext> relay)
        {
            var total = 0;
            int published;
            while ((published = await relay.PublishPendingAsync(ct)) > 0)
                total += published;
            return total;
        }
        var counts = await Task.WhenAll(DrainAsync(relayA), DrainAsync(relayB));

        Assert.Equal(envelopes.Count, counts.Sum());

        var wanted = envelopes.Select(e => e.OrderId.ToString()).ToHashSet();
        var received = MessagingTestSupport.ReadTopic(
            infra, Topics.Orders, r => wanted.Contains(r.Message.Key), expected: envelopes.Count);
        var receivedIds = received.Select(r => KafkaMessage.ToEnvelope(r.Message).EventId).ToList();

        Assert.Equal(envelopes.Count, receivedIds.Count);
        Assert.Equal(envelopes.Select(e => e.EventId).Order(), receivedIds.Order());

        await using (var scope = services.CreateAsyncScope())
        {
            var unsent = await scope.ServiceProvider.GetRequiredService<TestDbContext>()
                .Set<OutboxMessage>().CountAsync(m => m.SentAt == null, ct);
            Assert.Equal(0, unsent);
        }
    }
}
