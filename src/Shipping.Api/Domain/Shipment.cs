namespace KafkaEdaStudy.Shipping.Domain;

public enum ShipmentStatus
{
    Created,
    Dispatched,
    Delivered,
}

/// <summary>The physical delivery of one confirmed order (rules S1 and S2).</summary>
public sealed class Shipment
{
    private Shipment() { } // EF Core

    public Guid Id { get; private set; }
    public Guid OrderId { get; private set; }
    public Guid CorrelationId { get; private set; }
    public ShipmentStatus Status { get; private set; }
    public string? TrackingCode { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset? DispatchedAt { get; private set; }
    public DateTimeOffset? DeliveredAt { get; private set; }

    /// <summary>
    /// Optimistic concurrency token, mapped to Postgres' <c>xmin</c>: two staff clicks on "dispatch" at the same time
    /// cannot both succeed.
    /// </summary>
    public uint Version { get; private set; }

    /// <summary>Rule S1: a shipment is created only for a confirmed order; the caller guarantees that by listening to OrderConfirmed.</summary>
    public static Shipment Create(Guid orderId, Guid correlationId, DateTimeOffset now) => new()
    {
        Id = Guid.NewGuid(),
        OrderId = orderId,
        CorrelationId = correlationId,
        Status = ShipmentStatus.Created,
        CreatedAt = now,
    };

    /// <summary>Rule S2: dispatch is triggered by hand (simulated carrier). Only a created shipment can be dispatched.</summary>
    public bool Dispatch(DateTimeOffset now)
    {
        if (Status != ShipmentStatus.Created)
            return false;

        Status = ShipmentStatus.Dispatched;
        TrackingCode = $"TRK-{Guid.NewGuid():N}"[..12].ToUpperInvariant();
        DispatchedAt = now;
        return true;
    }

    /// <summary>Rule S2: delivery is triggered by hand (simulated carrier). Only a dispatched shipment can be delivered.</summary>
    public bool Deliver(DateTimeOffset now)
    {
        if (Status != ShipmentStatus.Dispatched)
            return false;

        Status = ShipmentStatus.Delivered;
        DeliveredAt = now;
        return true;
    }
}
