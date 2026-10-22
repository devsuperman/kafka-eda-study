using System.Text.Json;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaEdaStudy.BuildingBlocks.Persistence;

/// <summary>
/// Outbox relay (step 6): polls <c>outbox_messages</c> for unsent rows, publishes them to Kafka and marks them sent.
/// <para>
/// Delivery is at-least-once: if the process dies after Kafka acked but before <c>sent_at</c> is committed, the
/// row is published again on restart. That is fine because every consumer is idempotent (inbox, step 4).
/// </para>
/// <para>
/// Several replicas (Inventory runs 3) can run this relay against the same table. <c>FOR UPDATE SKIP LOCKED</c>
/// makes each replica lock the rows it picked and simply skip rows another replica holds, so a row is never
/// published by two relays at the same time. Trade-off: two relays work on different batches in parallel, so
/// strict per-key order holds within a batch but not across relays; a single leader (an advisory lock) would
/// give strict order at the cost of parallelism.
/// </para>
/// </summary>
public sealed class OutboxRelay<TDbContext>(
    IServiceScopeFactory scopeFactory,
    IEventPublisher publisher,
    TimeProvider clock,
    ILogger<OutboxRelay<TDbContext>> logger) : BackgroundService
    where TDbContext : DbContext
{
    public const int BatchSize = 100;
    public static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan ErrorDelay = TimeSpan.FromSeconds(2);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // let the host finish starting before the first poll
        logger.LogInformation("Outbox relay for {DbContext} started", typeof(TDbContext).Name);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Keep draining while there is work; only sleep when the table had nothing to send.
                if (await PublishPendingAsync(stoppingToken) == 0)
                    await Task.Delay(PollInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Database down, etc. Nothing is lost: the rows stay unsent and are picked up later.
                logger.LogError(ex, "Outbox relay for {DbContext} failed; retrying in {Delay}", typeof(TDbContext).Name, ErrorDelay);
                await Task.Delay(ErrorDelay, stoppingToken);
            }
        }
    }

    /// <summary>Publishes one batch of unsent rows. Returns how many were published.</summary>
    public async Task<int> PublishPendingAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TDbContext>();

        // The row locks live as long as this transaction, i.e. until the batch is marked sent.
        await using var tx = await db.Database.BeginTransactionAsync(cancellationToken);
        var batch = await db.Set<OutboxMessage>()
            .FromSql($"""
                SELECT * FROM outbox_messages
                WHERE sent_at IS NULL
                ORDER BY id
                LIMIT {BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);
        if (batch.Count == 0)
            return 0;

        var published = 0;
        foreach (var message in batch)
        {
            try
            {
                var envelope = JsonSerializer.Deserialize<EventEnvelope>(message.Envelope, EventJson.Options)
                    ?? throw new JsonException("Outbox row has an empty envelope.");
                await publisher.PublishAsync(message.Topic, envelope, cancellationToken);
                message.SentAt = clock.GetUtcNow();
                message.Attempts++;
                published++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Record the error and stop the batch: publishing the rows after this one now could put a later
                // event of the same order ahead of this one. The next poll retries from this row.
                message.Attempts++;
                message.LastError = ex.Message;
                logger.LogWarning(ex, "Outbox relay could not publish {EventType} {EventId} (attempt {Attempts})",
                    message.EventType, message.EventId, message.Attempts);
                break;
            }
        }

        await db.SaveChangesAsync(cancellationToken);
        await tx.CommitAsync(cancellationToken);
        return published;
    }
}
