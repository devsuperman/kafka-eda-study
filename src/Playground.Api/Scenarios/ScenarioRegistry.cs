namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// All scenarios known to the playground. Implemented ones are registered in DI as
/// <see cref="IScenario"/>; any planned name without an implementation is shown as unavailable.
/// Later steps add a scenario by implementing <see cref="IScenario"/> and registering it.
/// </summary>
public sealed class ScenarioRegistry
{
    private static readonly PlannedScenario[] Planned =
    [
        new("place-out-of-stock-order", "Place out-of-stock order",
            "Orders a product with no stock: inventory rejects the reservation and the order is cancelled."),
        new("place-over-limit-order", "Place over-limit order",
            "Orders more than the payment limit: payment is declined and the stock reservation is released."),
        new("place-timeout-order", "Place order that times out",
            "Places an order that never completes, so the timeout cancels it."),
        new("cancel-order", "Cancel order",
            "The customer cancels an order, triggering compensations (release stock, void payment)."),
        new("dispatch-shipment", "Dispatch shipment",
            "Dispatches the shipment of a confirmed order."),
        new("deliver-shipment", "Deliver shipment",
            "Marks a dispatched shipment as delivered."),
        new("resend-duplicate-event", "Resend duplicate event",
            "Publishes an already-seen event again to show idempotent consumers."),
        new("send-poison-message", "Send poison message",
            "Publishes a malformed message to show retries and the dead-letter topic."),
    ];

    private readonly Dictionary<string, IScenario> _scenarios;

    public ScenarioRegistry(IEnumerable<IScenario> implemented)
    {
        _scenarios = implemented.ToDictionary(s => s.Name, StringComparer.OrdinalIgnoreCase);
        foreach (var planned in Planned)
            _scenarios.TryAdd(planned.Name, planned);
    }

    /// <summary>Available scenarios first, then the planned ones, each group in roadmap (catalog) order.</summary>
    public IEnumerable<ScenarioInfo> List() =>
        _scenarios.Values
            .OrderBy(s => !s.Available)
            .ThenBy(s => CatalogIndex(s.Name))
            .Select(s => new ScenarioInfo(s.Name, s.Title, s.Description, s.Available));

    // Scenarios missing from the catalog (e.g. place-valid-order) come first.
    private static int CatalogIndex(string name) =>
        Array.FindIndex(Planned, p => string.Equals(p.Name, name, StringComparison.OrdinalIgnoreCase));

    public IScenario? Find(string name) => _scenarios.GetValueOrDefault(name);
}
