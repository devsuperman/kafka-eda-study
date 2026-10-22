using System.Collections.Concurrent;
using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.IntegrationTests;

/// <summary>Step 4: the inbox makes a handler idempotent.</summary>
public sealed class InboxTests(InfraFixture infra)
{
    private const string Consumer = "inbox-test";

    /// <summary>The handler pattern every service uses: tx, inbox check, state change, save, commit.</summary>
    private static async Task<bool> HandleAsync(IServiceProvider services, EventEnvelope envelope, CancellationToken ct)
    {
        await using var scope = services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var inbox = scope.ServiceProvider.GetRequiredService<IInbox>();

        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, Consumer, ct))
            return false; // duplicate

        var counter = await db.Counters.FindAsync([envelope.OrderId.ToString()], ct);
        if (counter is null)
            db.Counters.Add(new Counter { Name = envelope.OrderId.ToString(), Value = 1 });
        else
            counter.Value++;
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return true;
    }

    private static async Task<int> CounterAsync(IServiceProvider services, Guid orderId, CancellationToken ct)
    {
        await using var scope = services.CreateAsyncScope();
        var counter = await scope.ServiceProvider.GetRequiredService<TestDbContext>()
            .Counters.AsNoTracking().SingleOrDefaultAsync(c => c.Name == orderId.ToString(), ct);
        return counter?.Value ?? 0;
    }

    [Fact]
    public async Task Same_event_delivered_twice_through_Kafka_changes_state_once()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var services = await MessagingTestSupport.CreateServicesAsync(infra, ct);
        var topic = $"inbox-test-{Guid.NewGuid():N}";
        await MessagingTestSupport.CreateTopicsAsync(infra, topic, Topics.Dlq(topic));

        var envelope = EventEnvelope.Create(new OrderConfirmed(), Guid.NewGuid(), Guid.NewGuid());
        var publisher = services.GetRequiredService<IEventPublisher>();
        await publisher.PublishAsync(topic, envelope, ct);
        await publisher.PublishAsync(topic, envelope, ct); // the duplicate: same eventId

        var outcomes = new ConcurrentQueue<bool>();
        var consumer = new DelegateConsumer(infra, topic, async (e, token) =>
            outcomes.Enqueue(await HandleAsync(services, e, token)));
        await consumer.StartAsync(ct);
        try
        {
            await MessagingTestSupport.WaitUntilAsync(() => outcomes.Count == 2, TimeSpan.FromSeconds(30), ct);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
        }

        Assert.Equal([true, false], outcomes); // handled, then recognised as a duplicate
        Assert.Equal(1, await CounterAsync(services, envelope.OrderId, ct));
    }

    [Fact]
    public async Task Same_event_handled_concurrently_changes_state_once()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var services = await MessagingTestSupport.CreateServicesAsync(infra, ct);
        var envelope = EventEnvelope.Create(new OrderConfirmed(), Guid.NewGuid(), Guid.NewGuid());

        // E.g. two replicas right after a rebalance. The second INSERT into processed_messages waits for the
        // first transaction and then sees the conflict.
        var outcomes = await Task.WhenAll(
            Enumerable.Range(0, 5).Select(_ => HandleAsync(services, envelope, ct)));

        Assert.Single(outcomes, handled => handled);
        Assert.Equal(1, await CounterAsync(services, envelope.OrderId, ct));
    }

    [Fact]
    public async Task Marking_outside_a_transaction_is_rejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var services = await MessagingTestSupport.CreateServicesAsync(infra, ct);
        await using var scope = services.CreateAsyncScope();
        var inbox = scope.ServiceProvider.GetRequiredService<IInbox>();

        await Assert.ThrowsAsync<InvalidOperationException>(() => inbox.TryMarkProcessedAsync(Guid.NewGuid(), Consumer, ct));
    }
}

/// <summary>A <see cref="KafkaConsumerService"/> whose handler is a delegate, on its own consumer group.</summary>
public sealed class DelegateConsumer(
    InfraFixture infra, string topic, Func<EventEnvelope, CancellationToken, Task> handle,
    IReadOnlyList<TimeSpan>? retryDelays = null)
    : KafkaConsumerService(
        Options.Create(new KafkaOptions { BootstrapServers = infra.BootstrapServers }),
        NullLogger.Instance,
        $"test-{Guid.NewGuid():N}",
        [topic])
{
    protected override IReadOnlyList<TimeSpan> RetryDelays { get; } =
        retryDelays ?? [TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(1)];

    protected override Task HandleAsync(
        EventEnvelope envelope, ConsumeResult<string, string> result, CancellationToken cancellationToken) =>
        handle(envelope, cancellationToken);
}
