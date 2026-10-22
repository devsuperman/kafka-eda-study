using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>Happy path: an in-stock order under the payment limit, which should end up confirmed.</summary>
public sealed class PlaceValidOrderScenario(OrdersApiClient orders) : IScenario
{
    private const decimal MaxTotal = 1000m;

    public string Name => "place-valid-order";
    public string Title => "Place valid order";
    public string Description =>
        "Places an order of 1-3 in-stock products totalling at most 1000: stock is reserved, payment authorized and the order confirmed.";
    public bool Available => true;

    public async Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken)
    {
        var lines = RandomLines();
        var response = await orders.PlaceOrderAsync(new PlaceOrderRequest(Guid.NewGuid(), lines), cancellationToken);

        var summary = string.Join(", ", lines.Select(l => $"{l.Quantity}x {l.Sku}"));
        return new ScenarioResult(
            Name,
            response.OrderId.ToString(),
            response.CorrelationId.ToString(),
            $"Order {response.OrderId} placed ({summary}, total {lines.Sum(l => l.Quantity * l.UnitPrice)}); status {response.Status}.");
    }

    internal static IReadOnlyList<OrderLine> RandomLines()
    {
        var inStock = Products.All.Where(p => p.InitialStock > 0).ToArray();

        // Rejection sampling: draw until the total is within the limit (a single cheap line always is).
        for (var attempt = 0; attempt < 100; attempt++)
        {
            Random.Shared.Shuffle(inStock);
            var lines = inStock
                .Take(Random.Shared.Next(1, 4))
                .Select(p => new OrderLine(p.Sku, Random.Shared.Next(1, 3), p.Price))
                .ToList();

            if (lines.Sum(l => l.Quantity * l.UnitPrice) <= MaxTotal)
                return lines;
        }

        var cheapest = inStock.MinBy(p => p.Price)!;
        return [new OrderLine(cheapest.Sku, 1, cheapest.Price)];
    }
}
