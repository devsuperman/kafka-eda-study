using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// Flow 2 (out of stock): the cart holds a product with zero stock. Inventory rejects the reservation,
/// Orders cancels, and Payments voids the authorization it gave earlier.
/// </summary>
public sealed class PlaceOutOfStockOrderScenario(OrdersApiClient orders) : PlaceOrderScenario(orders)
{
    public override string Name => "place-out-of-stock-order";
    public override string Title => "Place out-of-stock order";
    public override string Description =>
        "Orders a product with no stock: inventory rejects the reservation, the order is cancelled and the payment voided.";

    protected override IReadOnlyList<OrderLine> Lines() =>
        [Line("CB-006", 1)]; // seeded with zero stock on purpose
}

/// <summary>
/// Flow 3 (payment declined): the laptop costs more than the 1,000 limit. Payments declines (P2), Orders cancels,
/// and Inventory releases the reservation it made.
/// </summary>
public sealed class PlaceOverLimitOrderScenario(OrdersApiClient orders) : PlaceOrderScenario(orders)
{
    public override string Name => "place-over-limit-order";
    public override string Title => "Place over-limit order";
    public override string Description =>
        "Orders a laptop above the payment limit: payment is declined, the order is cancelled and the stock reservation released.";

    protected override IReadOnlyList<OrderLine> Lines() =>
        [Line("LT-005", 1)]; // costs 1500, above the 1000 limit
}

/// <summary>Shared shape of the two order-placing scenarios: place a fixed order through Orders.Api.</summary>
public abstract class PlaceOrderScenario(OrdersApiClient orders) : IScenario
{
    public abstract string Name { get; }
    public abstract string Title { get; }
    public abstract string Description { get; }
    public bool Available => true;

    protected abstract IReadOnlyList<OrderLine> Lines();

    protected static OrderLine Line(string sku, int quantity) => new(sku, quantity, Products.Find(sku)!.Price);

    public async Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken)
    {
        var lines = Lines();
        var response = await orders.PlaceOrderAsync(new PlaceOrderRequest(Guid.NewGuid(), lines), cancellationToken);

        var summary = string.Join(", ", lines.Select(l => $"{l.Quantity}x {l.Sku}"));
        return new ScenarioResult(
            Name,
            response.OrderId.ToString(),
            response.CorrelationId.ToString(),
            $"Order {response.OrderId} placed ({summary}, total {lines.Sum(l => l.Quantity * l.UnitPrice)}). " +
            "The outcome arrives as events: watch the swimlane for the cancellation.");
    }
}
