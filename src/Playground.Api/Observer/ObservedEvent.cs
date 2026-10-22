using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Observer;

/// <summary>
/// One Kafka record as the UI sees it. Envelope fields are null when the value is not a valid
/// <see cref="EventEnvelope"/> (e.g. a poison message), so malformed data stays visible.
/// </summary>
public sealed record ObservedEvent(
    string Id,
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    DateTimeOffset Timestamp,
    IReadOnlyDictionary<string, string> Headers,
    bool IsDlq,
    string? EventType,
    Guid? EventId,
    Guid? OrderId,
    Guid? CorrelationId,
    Guid? CausationId,
    int? Version,
    DateTimeOffset? OccurredAt,
    JsonElement? Payload,
    string Raw)
{
    public static ObservedEvent From(ConsumeResult<string, string> result)
    {
        var message = result.Message;
        var raw = message.Value ?? string.Empty;

        var headers = new Dictionary<string, string>();
        foreach (var header in message.Headers ?? [])
            headers[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes()); // last value wins

        var envelope = TryParseEnvelope(raw);

        return new ObservedEvent(
            $"{result.Topic}-{result.Partition.Value}-{result.Offset.Value}",
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            message.Key,
            message.Timestamp.UtcDateTime,
            headers,
            result.Topic.EndsWith(".dlq", StringComparison.Ordinal),
            envelope?.EventType,
            envelope?.EventId,
            envelope?.OrderId,
            envelope?.CorrelationId,
            envelope?.CausationId,
            envelope?.Version,
            envelope?.OccurredAt,
            envelope?.Payload,
            raw);
    }

    private static EventEnvelope? TryParseEnvelope(string raw)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<EventEnvelope>(raw, EventJson.Options);
            // System.Text.Json happily fills missing members with defaults, so {"bad":1} would
            // "deserialize". Treat it as an envelope only if the identifying fields are present.
            return envelope is { EventId: var id, EventType: { Length: > 0 } } && id != Guid.Empty
                ? envelope with { Payload = envelope.Payload.Clone() }
                : null;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or ArgumentException)
        {
            return null;
        }
    }
}
