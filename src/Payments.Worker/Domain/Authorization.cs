using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Payments.Domain;

public enum AuthorizationStatus
{
    Authorized,
    Declined,
    Voided,
    Captured,
}

/// <summary>
/// The money held for one order (rules P1-P4). There is at most one per order: the unique order_id in the database
/// is the guard behind rule P4, next to the inbox that catches a redelivered event.
/// </summary>
public sealed class Authorization
{
    /// <summary>Rule P2: the simulated gateway declines any order total above this amount.</summary>
    public const decimal Limit = 1000m;

    private Authorization() { } // EF Core

    public Guid Id { get; private set; }
    public Guid OrderId { get; private set; }
    public decimal Amount { get; private set; }
    public AuthorizationStatus Status { get; private set; }
    public string? DeclineReason { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset UpdatedAt { get; private set; }

    /// <summary>Rule P1: authorize when the order is placed. Rule P2: a total above <see cref="Limit"/> is declined.</summary>
    public static Authorization Decide(Guid orderId, decimal total, DateTimeOffset now)
    {
        var declined = total > Limit;
        return new Authorization
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            Amount = total,
            Status = declined ? AuthorizationStatus.Declined : AuthorizationStatus.Authorized,
            DeclineReason = declined ? DeclineReasons.OverLimit : null,
            CreatedAt = now,
            UpdatedAt = now,
        };
    }

    /// <summary>Rule P3: cancelling before capture voids the authorization.</summary>
    public bool Void(DateTimeOffset now) => MoveFrom(AuthorizationStatus.Authorized, AuthorizationStatus.Voided, now);

    /// <summary>Rule P1: money is taken only when the order ships. Rule P4: a captured authorization is never captured again.</summary>
    public bool Capture(DateTimeOffset now) => MoveFrom(AuthorizationStatus.Authorized, AuthorizationStatus.Captured, now);

    private bool MoveFrom(AuthorizationStatus from, AuthorizationStatus to, DateTimeOffset now)
    {
        if (Status != from)
            return false;
        Status = to;
        UpdatedAt = now;
        return true;
    }
}
