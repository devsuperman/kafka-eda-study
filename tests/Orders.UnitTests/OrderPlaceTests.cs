using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Orders.Domain;

namespace KafkaEdaStudy.Orders.UnitTests;

/// <summary>Rule O1: at least one line, quantity 1..10, known SKUs, no duplicate SKUs.</summary>
public sealed class OrderPlaceTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static PlaceOrderResult Place(params OrderLine[] lines) =>
        Order.Place(Guid.NewGuid(), Guid.NewGuid(), lines, Now);

    [Fact]
    public void Valid_order_starts_pending_with_lines_and_total()
    {
        var customerId = Guid.NewGuid();
        var correlationId = Guid.NewGuid();

        var result = Order.Place(customerId, correlationId,
            [new("KB-001", 2, 120m), new("MS-002", 1, 40m)], Now);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
        var order = result.Order!;
        Assert.NotEqual(Guid.Empty, order.Id);
        Assert.Equal(customerId, order.CustomerId);
        Assert.Equal(correlationId, order.CorrelationId);
        Assert.Equal(OrderStatus.Pending, order.Status);
        Assert.Null(order.CancelReason);
        Assert.Equal(2, order.Lines.Count);
        Assert.Equal(280m, order.Total);
        Assert.Equal(Now, order.CreatedAt);
        Assert.Equal(Now, order.UpdatedAt);
    }

    [Fact]
    public void No_lines_is_rejected()
    {
        var result = Place();

        Assert.False(result.IsValid);
        Assert.Null(result.Order);
        Assert.Contains("lines", result.Errors.Keys);
    }

    [Fact]
    public void Null_lines_is_rejected()
    {
        var result = Order.Place(Guid.NewGuid(), Guid.NewGuid(), null, Now);

        Assert.False(result.IsValid);
        Assert.Contains("lines", result.Errors.Keys);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(11)]
    public void Quantity_outside_1_to_10_is_rejected(int quantity)
    {
        var result = Place(new OrderLine("KB-001", quantity, 120m));

        Assert.False(result.IsValid);
        Assert.Contains("lines[0].quantity", result.Errors.Keys);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    public void Quantity_bounds_are_inclusive(int quantity)
    {
        Assert.True(Place(new OrderLine("KB-001", quantity, 120m)).IsValid);
    }

    [Theory]
    [InlineData("NOPE-999")]
    [InlineData("")]
    [InlineData("kb-001")]
    public void Unknown_sku_is_rejected(string sku)
    {
        var result = Place(new OrderLine(sku, 1, 10m));

        Assert.False(result.IsValid);
        Assert.Contains("lines[0].sku", result.Errors.Keys);
    }

    [Fact]
    public void Duplicate_sku_is_rejected()
    {
        var result = Place(new OrderLine("KB-001", 1, 120m), new OrderLine("KB-001", 2, 120m));

        Assert.False(result.IsValid);
        Assert.Contains("lines[1].sku", result.Errors.Keys);
        Assert.DoesNotContain("lines[0].sku", result.Errors.Keys);
    }

    [Fact]
    public void Non_positive_unit_price_is_rejected()
    {
        var result = Place(new OrderLine("KB-001", 1, 0m));

        Assert.False(result.IsValid);
        Assert.Contains("lines[0].unitPrice", result.Errors.Keys);
    }

    [Fact]
    public void All_errors_are_reported_at_once()
    {
        var result = Place(new OrderLine("NOPE-999", 0, 1m), new OrderLine("MS-002", 11, 40m));

        Assert.Equal(["lines[0].quantity", "lines[0].sku", "lines[1].quantity"], result.Errors.Keys.Order());
    }
}
