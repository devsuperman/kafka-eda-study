using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Playground.Observer;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>
/// Simulates at-least-once delivery: re-produces an already published <c>OrderPlaced</c> record
/// byte for byte (same key, value and headers, hence the same <c>eventId</c>). Idempotent consumers
/// recognise the eventId and skip it, so no second reservation, authorization or notification happens.
/// </summary>
public sealed class ResendDuplicateEventScenario(EventStore store, FaultInjectionProducer producer) : IScenario
{
    public string Name => "resend-duplicate-event";
    public string Title => "Resend duplicate event";
    public string Description =>
        "Publishes an OrderPlaced again with the same eventId (as a retrying producer would): idempotent consumers ignore it.";
    public bool Available => true;

    public async Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken)
    {
        var original = FindOriginal(input.OrderId);

        var headers = new Headers();
        foreach (var (name, value) in original.Headers)
            headers.Add(name, Encoding.UTF8.GetBytes(value));

        var written = await producer.ProduceAsync(
            original.Topic,
            new Message<string?, string> { Key = original.Key, Value = original.Raw, Headers = headers },
            cancellationToken);

        return new ScenarioResult(
            Name,
            original.OrderId?.ToString(),
            original.CorrelationId?.ToString(),
            $"Same eventId {original.EventId} re-sent: {original.EventType} of order {original.OrderId} " +
            $"(copied from {original.Topic} partition {original.Partition}@{original.Offset} to partition " +
            $"{written.Partition.Value}@{written.Offset.Value}). Consumers should ignore it: no second " +
            "reservation, authorization or notification.");
    }

    private ObservedEvent FindOriginal(string? orderIdText)
    {
        if (string.IsNullOrWhiteSpace(orderIdText))
        {
            // Prefer an order the consumers actually processed, so the duplicate is skipped by the inbox
            // rather than dead-lettered (an unknown-SKU poison message is an OrderPlaced too).
            return store.FindLatest(e => IsOrderPlaced(e) && HasOnlyKnownSkus(e))
                ?? store.FindLatest(IsOrderPlaced)
                ?? throw ScenarioRejectedException.NotFound(
                    "Nothing to resend: no OrderPlaced in the observed history yet. Place an order first.");
        }

        if (!Guid.TryParse(orderIdText, out var orderId))
            throw ScenarioRejectedException.BadRequest($"'{orderIdText}' is not a valid orderId (GUID expected).");

        return store.FindLatest(e => IsOrderPlaced(e) && e.OrderId == orderId)
            ?? throw ScenarioRejectedException.NotFound(
                $"Nothing to resend: no OrderPlaced for order {orderId} in the observed history.");
    }

    private static bool IsOrderPlaced(ObservedEvent e) =>
        !e.IsDlq && e.Topic == Topics.Orders && e.EventType == nameof(OrderPlaced);

    private static bool HasOnlyKnownSkus(ObservedEvent e)
    {
        try
        {
            var placed = e.Payload?.Deserialize<OrderPlaced>(EventJson.Options);
            return placed?.Lines is { Count: > 0 } lines && lines.All(l => Products.Find(l.Sku) is not null);
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
