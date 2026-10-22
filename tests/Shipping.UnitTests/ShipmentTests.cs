using KafkaEdaStudy.Shipping.Domain;

namespace KafkaEdaStudy.Shipping.UnitTests;

/// <summary>Rule S2: dispatch and delivery happen in this order, once each.</summary>
public sealed class ShipmentTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void A_created_shipment_is_dispatched_then_delivered()
    {
        var shipment = Shipment.Create(Guid.NewGuid(), Guid.NewGuid(), Now);

        Assert.True(shipment.Dispatch(Now.AddHours(1)));
        Assert.StartsWith("TRK-", shipment.TrackingCode);
        Assert.True(shipment.Deliver(Now.AddHours(2)));

        Assert.Equal(ShipmentStatus.Delivered, shipment.Status);
        Assert.Equal(Now.AddHours(2), shipment.DeliveredAt);
    }

    [Fact]
    public void A_shipment_cannot_be_delivered_before_it_is_dispatched()
    {
        var shipment = Shipment.Create(Guid.NewGuid(), Guid.NewGuid(), Now);

        Assert.False(shipment.Deliver(Now));
        Assert.Equal(ShipmentStatus.Created, shipment.Status);
    }

    [Fact]
    public void A_shipment_is_dispatched_only_once()
    {
        var shipment = Shipment.Create(Guid.NewGuid(), Guid.NewGuid(), Now);
        Assert.True(shipment.Dispatch(Now));
        var tracking = shipment.TrackingCode;

        Assert.False(shipment.Dispatch(Now));
        Assert.Equal(tracking, shipment.TrackingCode);
    }
}
