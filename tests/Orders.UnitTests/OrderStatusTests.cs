using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Orders.Domain;

namespace KafkaEdaStudy.Orders.UnitTests;

/// <summary>Rule O3 (status machine) and O6 (cancel until Shipped).</summary>
public sealed class OrderStatusTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset Later = Now.AddMinutes(1);

    private static Order NewOrder() =>
        Order.Place(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("KB-001", 1, 120m)], Now).Order!;

    private static Order OrderIn(OrderStatus status)
    {
        var order = NewOrder();
        switch (status)
        {
            case OrderStatus.Confirmed:
                order.Confirm(Now);
                break;
            case OrderStatus.Shipped:
                order.Confirm(Now);
                order.Ship(Now);
                break;
            case OrderStatus.Delivered:
                order.Confirm(Now);
                order.Ship(Now);
                order.Deliver(Now);
                break;
            case OrderStatus.Cancelled:
                order.Cancel(CancelReasons.Customer, Now);
                break;
        }
        Assert.Equal(status, order.Status);
        return order;
    }

    [Fact]
    public void Happy_path_moves_pending_to_delivered()
    {
        var order = NewOrder();

        Assert.True(order.Confirm(Later));
        Assert.True(order.Ship(Later));
        Assert.True(order.Deliver(Later));

        Assert.Equal(OrderStatus.Delivered, order.Status);
        Assert.Equal(Later, order.UpdatedAt);
    }

    [Theory]
    [InlineData(OrderStatus.Pending)]
    [InlineData(OrderStatus.Confirmed)]
    public void Can_cancel_before_shipping(OrderStatus status)
    {
        var order = OrderIn(status);

        Assert.True(order.Cancel(CancelReasons.Timeout, Later));

        Assert.Equal(OrderStatus.Cancelled, order.Status);
        Assert.Equal(CancelReasons.Timeout, order.CancelReason);
    }

    [Theory]
    [InlineData(OrderStatus.Shipped)]
    [InlineData(OrderStatus.Delivered)]
    [InlineData(OrderStatus.Cancelled)]
    public void Cannot_cancel_after_shipping_or_twice(OrderStatus status)
    {
        var order = OrderIn(status);
        var reason = order.CancelReason;

        Assert.False(order.Cancel(CancelReasons.OutOfStock, Later));

        Assert.Equal(status, order.Status);
        Assert.Equal(reason, order.CancelReason);
    }

    [Fact]
    public void Late_confirmation_after_cancel_is_rejected()
    {
        var order = OrderIn(OrderStatus.Cancelled);

        Assert.False(order.Confirm(Later));

        Assert.Equal(OrderStatus.Cancelled, order.Status);
        Assert.Equal(Now, order.UpdatedAt);
    }

    [Fact]
    public void Duplicate_confirmation_is_rejected()
    {
        var order = OrderIn(OrderStatus.Confirmed);

        Assert.False(order.Confirm(Later));
    }

    [Theory]
    [InlineData(OrderStatus.Pending, OrderStatus.Shipped)]
    [InlineData(OrderStatus.Pending, OrderStatus.Delivered)]
    [InlineData(OrderStatus.Confirmed, OrderStatus.Delivered)]
    [InlineData(OrderStatus.Delivered, OrderStatus.Shipped)]
    public void Skipping_or_going_back_is_not_allowed(OrderStatus from, OrderStatus to)
    {
        Assert.False(Order.CanTransition(from, to));
    }
}
