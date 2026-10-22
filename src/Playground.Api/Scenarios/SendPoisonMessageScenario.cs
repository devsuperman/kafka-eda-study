using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// Publishes a message no consumer can ever process to <c>orders.events</c>. A consumer must not get
/// stuck on it (it would block its whole partition): it parks it in <c>orders.events.dlq</c> and moves on.
/// </summary>
/// <remarks>
/// Kinds (request body <c>kind</c>):
/// <list type="bullet">
/// <item><c>unknown-sku</c> (default): a well-formed <c>OrderPlaced</c> envelope for a new order whose
/// only line has SKU <see cref="UnknownSku"/>, which is not in <see cref="Products.All"/>. Valid JSON,
/// invalid business data.</item>
/// <item><c>malformed-json</c>: a value that is not JSON at all, with a new key.</item>
/// </list>
/// </remarks>
public sealed class SendPoisonMessageScenario(FaultInjectionProducer producer) : IScenario
{
    public const string UnknownSkuKind = "unknown-sku";
    public const string MalformedJsonKind = "malformed-json";
    public const string UnknownSku = "XX-999";

    public string Name => "send-poison-message";
    public string Title => "Send poison message";
    public string Description =>
        "Publishes an OrderPlaced with an unknown SKU, or a value that is not JSON: consumers dead-letter it to orders.events.dlq and keep going.";
    public bool Available => true;

    public Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken) =>
        (input.Kind?.Trim().ToLowerInvariant() ?? UnknownSkuKind) switch
        {
            UnknownSkuKind or "" => SendUnknownSkuAsync(cancellationToken),
            MalformedJsonKind => SendMalformedJsonAsync(cancellationToken),
            var other => throw ScenarioRejectedException.BadRequest(
                $"Unknown poison kind '{other}'. Use '{UnknownSkuKind}' or '{MalformedJsonKind}'."),
        };

    private async Task<ScenarioResult> SendUnknownSkuAsync(CancellationToken cancellationToken)
    {
        var line = new OrderLine(UnknownSku, 1, 10m);
        var envelope = EventEnvelope.Create(
            new OrderPlaced(Guid.NewGuid(), [line], line.Quantity * line.UnitPrice),
            orderId: Guid.NewGuid(),
            correlationId: Guid.NewGuid());

        var message = KafkaMessage.From(envelope);
        var written = await producer.ProduceAsync(
            Topics.Orders,
            new Message<string?, string> { Key = message.Key, Value = message.Value, Headers = message.Headers },
            cancellationToken);

        return new ScenarioResult(
            Name,
            envelope.OrderId.ToString(),
            envelope.CorrelationId.ToString(),
            $"OrderPlaced with unknown SKU {UnknownSku} published to {Topics.Orders} " +
            $"(partition {written.Partition.Value}@{written.Offset.Value}). The JSON is fine but the data can never " +
            $"be processed, so consumers should send it straight to {Topics.Dlq(Topics.Orders)} and carry on.");
    }

    private async Task<ScenarioResult> SendMalformedJsonAsync(CancellationToken cancellationToken)
    {
        var key = Guid.NewGuid().ToString();
        const string garbage = """{"eventId": "not-a-guid", "eventType": "OrderPlaced", payload: <<this is not JSON>>""";

        var written = await producer.ProduceAsync(
            Topics.Orders,
            new Message<string?, string> { Key = key, Value = garbage },
            cancellationToken);

        return new ScenarioResult(
            Name,
            key,
            null,
            $"Malformed (non-JSON) value published to {Topics.Orders} with key {key} " +
            $"(partition {written.Partition.Value}@{written.Offset.Value}). No consumer can deserialize it, so each " +
            $"one should park it in {Topics.Dlq(Topics.Orders)} immediately and move on.");
    }
}
