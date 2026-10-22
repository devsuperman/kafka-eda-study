using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Payments.Data;
using KafkaEdaStudy.Payments.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Payments.Consumers;

/// <summary>Payment decisions for one order: authorize when placed (P1, P2), void when cancelled (P3), capture when shipped (P1).</summary>
public sealed class PaymentHandler(
    PaymentsDbContext db,
    IInbox inbox,
    IOutbox outbox,
    TimeProvider clock,
    ILogger<PaymentHandler> logger)
{
    /// <summary>Name of this handler in <c>processed_messages</c>.</summary>
    public const string InboxConsumer = "payments";

    public Task AuthorizeAsync(EventEnvelope envelope, OrderPlaced placed, CancellationToken ct) =>
        ChangeAsync(envelope, async () =>
        {
            // Rule P4: an order is authorized once, even when a re-published OrderPlaced arrives with a new eventId.
            if (await db.Authorizations.AnyAsync(a => a.OrderId == envelope.OrderId, ct))
            {
                logger.LogInformation("Order {OrderId} already has an authorization (rule P4); nothing to do", envelope.OrderId);
                return;
            }

            var authorization = Authorization.Decide(envelope.OrderId, placed.Total, clock.GetUtcNow());
            db.Authorizations.Add(authorization);

            if (authorization.Status == AuthorizationStatus.Authorized)
            {
                outbox.Add(Topics.Payments, EventEnvelope.CausedBy(envelope, new PaymentAuthorized(authorization.Id, authorization.Amount)));
                logger.LogInformation("Order {OrderId}: authorized {Amount}", envelope.OrderId, authorization.Amount);
            }
            else
            {
                outbox.Add(Topics.Payments, EventEnvelope.CausedBy(envelope, new PaymentDeclined(authorization.DeclineReason!)));
                logger.LogInformation("Order {OrderId}: declined ({Reason})", envelope.OrderId, authorization.DeclineReason);
            }
        }, ct);

    public Task VoidAsync(EventEnvelope envelope, CancellationToken ct) =>
        ChangeAsync(envelope, async () =>
        {
            var authorization = await db.Authorizations.FirstOrDefaultAsync(a => a.OrderId == envelope.OrderId, ct);
            if (authorization is not null && authorization.Void(clock.GetUtcNow()))
            {
                outbox.Add(Topics.Payments, EventEnvelope.CausedBy(envelope, new PaymentVoided(authorization.Id)));
                logger.LogInformation("Order {OrderId}: authorization voided (rule P3)", envelope.OrderId);
            }
            else
            {
                logger.LogInformation("Order {OrderId}: no open authorization to void", envelope.OrderId);
            }
        }, ct);

    public Task CaptureAsync(EventEnvelope envelope, CancellationToken ct) =>
        ChangeAsync(envelope, async () =>
        {
            var authorization = await db.Authorizations.FirstOrDefaultAsync(a => a.OrderId == envelope.OrderId, ct);
            if (authorization is not null && authorization.Capture(clock.GetUtcNow()))
            {
                outbox.Add(Topics.Payments, EventEnvelope.CausedBy(envelope, new PaymentCaptured(authorization.Amount)));
                logger.LogInformation("Order {OrderId}: captured {Amount}", envelope.OrderId, authorization.Amount);
            }
            else
            {
                logger.LogInformation("Order {OrderId}: nothing to capture", envelope.OrderId);
            }
        }, ct);

    /// <summary>The inbox pattern (step 4): inbox row, change, outgoing event and commit in one transaction.</summary>
    private async Task ChangeAsync(EventEnvelope envelope, Func<Task> change, CancellationToken ct)
    {
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        if (!await inbox.TryMarkProcessedAsync(envelope.EventId, InboxConsumer, ct))
        {
            logger.LogInformation("Duplicate {EventId} ignored ({EventType} for order {OrderId})",
                envelope.EventId, envelope.EventType, envelope.OrderId);
            return;
        }

        await change();
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
    }
}
