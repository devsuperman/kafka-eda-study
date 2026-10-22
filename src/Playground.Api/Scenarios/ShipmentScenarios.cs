namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>Rule S2: staff hand a confirmed order's parcel to the carrier (flow 1, Orders moves to Shipped).</summary>
public sealed class DispatchShipmentScenario(ShippingApiClient shipping) : ShipmentScenario(shipping)
{
    public override string Name => "dispatch-shipment";
    public override string Title => "Dispatch shipment";
    public override string Description => "Dispatches the shipment of a confirmed order to the carrier.";

    protected override string WaitingStatus => "Created";

    protected override Task<ShipmentInfo> ActAsync(Guid orderId, CancellationToken cancellationToken) =>
        Shipping.DispatchAsync(orderId, cancellationToken);

    protected override string Verb => "dispatched";
}

/// <summary>Rule S2: the carrier reports the parcel delivered (flow 1, Orders moves to Delivered).</summary>
public sealed class DeliverShipmentScenario(ShippingApiClient shipping) : ShipmentScenario(shipping)
{
    public override string Name => "deliver-shipment";
    public override string Title => "Deliver shipment";
    public override string Description => "Marks a dispatched shipment as delivered.";

    protected override string WaitingStatus => "Dispatched";

    protected override Task<ShipmentInfo> ActAsync(Guid orderId, CancellationToken cancellationToken) =>
        Shipping.DeliverAsync(orderId, cancellationToken);

    protected override string Verb => "delivered";
}

/// <summary>Shared shape of the two shipment scenarios: act on the order the user picked, or else the oldest shipment waiting in its status.</summary>
public abstract class ShipmentScenario(ShippingApiClient shipping) : IScenario
{
    protected ShippingApiClient Shipping => shipping;

    public abstract string Name { get; }
    public abstract string Title { get; }
    public abstract string Description { get; }
    public bool Available => true;

    protected abstract string WaitingStatus { get; }
    protected abstract string Verb { get; }

    protected abstract Task<ShipmentInfo> ActAsync(Guid orderId, CancellationToken cancellationToken);

    public async Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken)
    {
        var orderId = await ResolveOrderAsync(input.OrderId, cancellationToken);
        var shipment = await ActAsync(orderId, cancellationToken);

        return new ScenarioResult(
            Name,
            orderId.ToString(),
            null,
            $"Shipment {shipment.Id} of order {orderId} {Verb} (tracking {shipment.TrackingCode ?? "none"}).");
    }

    private async Task<Guid> ResolveOrderAsync(string? orderIdText, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(orderIdText))
        {
            if (!Guid.TryParse(orderIdText, out var orderId))
                throw ScenarioRejectedException.BadRequest($"'{orderIdText}' is not a valid orderId (GUID expected).");
            return orderId;
        }

        var waiting = await shipping.ListShipmentsAsync(WaitingStatus, 50, cancellationToken);
        return waiting.LastOrDefault()?.OrderId ?? throw ScenarioRejectedException.NotFound(
            $"No shipment is waiting in status {WaitingStatus}. Place and confirm an order first.");
    }
}
