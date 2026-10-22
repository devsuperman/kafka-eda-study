using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Orders.Domain;

namespace KafkaEdaStudy.Orders.UnitTests;

/// <summary>Rule O4 (confirm after both answers, in any order) and the cancel rules the saga relies on.</summary>
public sealed class OrderSagaTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset Later = Now.AddMinutes(1);

    private static Order NewOrder() =>
        Order.Place(Guid.NewGuid(), Guid.NewGuid(), [new OrderLine("KB-001", 1, 120m)], Now).Order!;

    [Fact]
    public void Stock_then_payment_confirms_the_order()
    {
        var order = NewOrder();

        Assert.True(order.RecordStockReserved(Later));
        Assert.False(order.IsReadyToConfirm);
        Assert.True(order.RecordPaymentAuthorized(Later));
        Assert.True(order.IsReadyToConfirm);
        Assert.True(order.Confirm(Later));

        Assert.Equal(OrderStatus.Confirmed, order.Status);
    }

    [Fact]
    public void Payment_then_stock_confirms_the_order()
    {
        var order = NewOrder();

        Assert.True(order.RecordPaymentAuthorized(Later));
        Assert.False(order.IsReadyToConfirm);
        Assert.True(order.RecordStockReserved(Later));
        Assert.True(order.IsReadyToConfirm);
    }

    [Fact]
    public void A_duplicate_answer_is_recorded_once()
    {
        var order = NewOrder();
        Assert.True(order.RecordStockReserved(Later));

        Assert.False(order.RecordStockReserved(Later.AddSeconds(1)));
        Assert.Equal(Later, order.UpdatedAt);
    }

    [Fact]
    public void Answers_after_cancellation_change_nothing()
    {
        var order = NewOrder();
        Assert.True(order.Cancel(CancelReasons.OutOfStock, Later));

        Assert.False(order.RecordStockReserved(Later));
        Assert.False(order.RecordPaymentAuthorized(Later));
        Assert.False(order.IsReadyToConfirm);
        Assert.Equal(OrderStatus.Cancelled, order.Status);
    }

    [Fact]
    public void A_second_failing_answer_does_not_cancel_twice()
    {
        var order = NewOrder();
        Assert.True(order.Cancel(CancelReasons.OutOfStock, Later));

        Assert.False(order.Cancel(CancelReasons.PaymentDeclined, Later));
        Assert.Equal(CancelReasons.OutOfStock, order.CancelReason);
    }

    [Fact]
    public void Confirmed_order_can_still_be_cancelled_by_the_customer()
    {
        var order = NewOrder();
        order.RecordStockReserved(Later);
        order.RecordPaymentAuthorized(Later);
        order.Confirm(Later);

        Assert.True(order.Cancel(CancelReasons.Customer, Later));
    }

    [Fact]
    public void Shipped_order_can_no_longer_be_cancelled()
    {
        var order = NewOrder();
        order.RecordStockReserved(Later);
        order.RecordPaymentAuthorized(Later);
        order.Confirm(Later);
        order.Ship(Later);

        Assert.False(order.Cancel(CancelReasons.Customer, Later));
        Assert.Equal(OrderStatus.Shipped, order.Status);
    }
}
