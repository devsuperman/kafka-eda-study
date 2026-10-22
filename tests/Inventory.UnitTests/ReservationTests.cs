using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Inventory.Domain;

namespace KafkaEdaStudy.Inventory.UnitTests;

/// <summary>Rules I1 (all lines or none) and I3 (stock never negative).</summary>
public sealed class ReservationTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(15);

    /// <summary>Fresh stock seeded from the product list, like the worker does at startup.</summary>
    private static Dictionary<string, StockItem> SeededStock() =>
        Products.All.ToDictionary(p => p.Sku, p => new StockItem(p.Sku, p.Name, p.InitialStock));

    private static OrderLine Line(string sku, int quantity) => new(sku, quantity, 1m);

    private static ReserveResult Reserve(Dictionary<string, StockItem> stock, params OrderLine[] lines) =>
        Reservation.TryReserve(Guid.NewGuid(), lines, stock, Now, Timeout);

    [Fact]
    public void Single_line_in_stock_is_reserved()
    {
        var stock = SeededStock();
        var orderId = Guid.NewGuid();

        var result = Reservation.TryReserve(orderId, [Line("KB-001", 2)], stock, Now, Timeout);

        Assert.True(result.Succeeded);
        Assert.Empty(result.MissingSkus);
        var reservation = result.Reservation!;
        Assert.NotEqual(Guid.Empty, reservation.Id);
        Assert.Equal(orderId, reservation.OrderId);
        Assert.Equal(ReservationStatus.Active, reservation.Status);
        Assert.Equal(Now, reservation.CreatedAt);
        Assert.Equal(Now + Timeout, reservation.ExpiresAt);
        var line = Assert.Single(reservation.Lines);
        Assert.Equal(("KB-001", 2), (line.Sku, line.Quantity));
        Assert.Equal(48, stock["KB-001"].Available);
        Assert.Equal(2, stock["KB-001"].Reserved);
    }

    [Fact]
    public void Multiple_lines_are_all_reserved()
    {
        var stock = SeededStock();

        var result = Reserve(stock, Line("KB-001", 2), Line("MS-002", 10), Line("MN-003", 1));

        Assert.True(result.Succeeded);
        Assert.Equal(["KB-001", "MS-002", "MN-003"], result.Reservation!.Lines.Select(l => l.Sku));
        Assert.Equal((48, 2), (stock["KB-001"].Available, stock["KB-001"].Reserved));
        Assert.Equal((90, 10), (stock["MS-002"].Available, stock["MS-002"].Reserved));
        Assert.Equal((19, 1), (stock["MN-003"].Available, stock["MN-003"].Reserved));
    }

    [Fact]
    public void Exact_available_stock_can_be_reserved_down_to_zero()
    {
        var stock = SeededStock(); // LT-005 has 5

        var result = Reserve(stock, Line("LT-005", 5));

        Assert.True(result.Succeeded);
        Assert.Equal(0, stock["LT-005"].Available);
        Assert.Equal(5, stock["LT-005"].Reserved);
    }

    [Fact]
    public void One_more_than_available_fails()
    {
        var stock = SeededStock();

        var result = Reserve(stock, Line("LT-005", 6));

        Assert.False(result.Succeeded);
        Assert.Null(result.Reservation);
        Assert.Equal(["LT-005"], result.MissingSkus);
        Assert.Equal((5, 0), (stock["LT-005"].Available, stock["LT-005"].Reserved));
    }

    [Fact]
    public void Zero_stock_sku_fails()
    {
        var stock = SeededStock(); // CB-006 has 0

        var result = Reserve(stock, Line("CB-006", 1));

        Assert.False(result.Succeeded);
        Assert.Equal(["CB-006"], result.MissingSkus);
        Assert.Equal(0, stock["CB-006"].Available);
    }

    [Fact]
    public void Unknown_sku_is_reported_missing()
    {
        var stock = SeededStock();

        var result = Reserve(stock, Line("NOPE-999", 1));

        Assert.False(result.Succeeded);
        Assert.Equal(["NOPE-999"], result.MissingSkus);
    }

    [Fact]
    public void One_missing_line_reserves_nothing()
    {
        var stock = SeededStock();

        var result = Reserve(stock, Line("KB-001", 2), Line("CB-006", 1), Line("MS-002", 3));

        Assert.False(result.Succeeded);
        Assert.Equal(["CB-006"], result.MissingSkus);
        // All or none (I1): the lines that fit were not touched either.
        Assert.Equal((50, 0), (stock["KB-001"].Available, stock["KB-001"].Reserved));
        Assert.Equal((100, 0), (stock["MS-002"].Available, stock["MS-002"].Reserved));
    }

    [Fact]
    public void Every_missing_sku_is_reported()
    {
        var stock = SeededStock();

        var result = Reserve(stock, Line("CB-006", 1), Line("KB-001", 1), Line("LT-005", 6), Line("NOPE-999", 1));

        Assert.Equal(["CB-006", "LT-005", "NOPE-999"], result.MissingSkus);
        Assert.Equal(50, stock["KB-001"].Available);
    }

    [Fact]
    public void Lines_for_the_same_sku_are_summed()
    {
        var stock = SeededStock();

        var failed = Reserve(stock, Line("LT-005", 3), Line("LT-005", 3));
        var reserved = Reserve(stock, Line("LT-005", 2), Line("LT-005", 3));

        Assert.Equal(["LT-005"], failed.MissingSkus);
        var line = Assert.Single(reserved.Reservation!.Lines);
        Assert.Equal(5, line.Quantity);
        Assert.Equal(0, stock["LT-005"].Available);
    }

    [Fact]
    public void Consecutive_orders_drain_stock_and_never_go_negative()
    {
        var stock = SeededStock(); // MN-003 has 20

        var results = Enumerable.Range(0, 3).Select(_ => Reserve(stock, Line("MN-003", 8))).ToList();

        Assert.Equal([true, true, false], results.Select(r => r.Succeeded));
        Assert.Equal(4, stock["MN-003"].Available);
        Assert.Equal(16, stock["MN-003"].Reserved);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Non_positive_quantity_cannot_be_reserved(int quantity)
    {
        var item = new StockItem("KB-001", "Mechanical keyboard", 50);

        Assert.False(item.CanReserve(quantity));
        Assert.Throws<InvalidOperationException>(() => item.Reserve(quantity));
        Assert.Equal(50, item.Available);
    }

    [Fact]
    public void Empty_order_is_rejected()
    {
        Assert.Throws<ArgumentException>(() => Reserve(SeededStock()));
    }
}
