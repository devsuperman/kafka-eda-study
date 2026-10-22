namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// Flow 5 (customer cancel): the customer cancels an order that has not shipped. Orders publishes OrderCancelled;
/// Inventory releases the reservation and Payments voids the authorization.
/// </summary>
public sealed class CancelOrderScenario(OrdersApiClient orders) : IScenario
{
    public string Name => "cancel-order";
    public string Title => "Cancel order";
    public string Description =>
        "The customer cancels an order before it ships: stock is released and the payment voided (flow 5).";
    public bool Available => true;

    public async Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken)
    {
        var orderId = await ResolveOrderAsync(input.OrderId, orders, cancellationToken);
        await orders.CancelOrderAsync(orderId, cancellationToken);

        return new ScenarioResult(
            Name,
            orderId.ToString(),
            null,
            $"Order {orderId} cancelled by the customer (rule O6). Inventory should release the stock and Payments void the authorization.");
    }

    /// <summary>The order the user picked, or else the newest order that can still be cancelled.</summary>
    private static async Task<Guid> ResolveOrderAsync(string? orderIdText, OrdersApiClient orders, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(orderIdText))
        {
            if (!Guid.TryParse(orderIdText, out var orderId))
                throw ScenarioRejectedException.BadRequest($"'{orderIdText}' is not a valid orderId (GUID expected).");
            return orderId;
        }

        var open = (await orders.ListOrdersAsync(50, cancellationToken))
            .FirstOrDefault(o => o.Status is "Pending" or "Confirmed");
        return open?.Id ?? throw ScenarioRejectedException.NotFound(
            "No open order to cancel. Place an order first.");
    }
}
