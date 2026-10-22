using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.BuildingBlocks.Kafka;

/// <summary>Converts envelopes to Kafka messages and back.</summary>
public static class KafkaMessage
{
    public static Message<string, string> From(EventEnvelope envelope, string? traceParent = null)
    {
        var headers = new Headers
        {
            { KafkaHeaderNames.EventType, Encoding.UTF8.GetBytes(envelope.EventType) },
            { KafkaHeaderNames.EventVersion, Encoding.UTF8.GetBytes(envelope.Version.ToString()) },
            { KafkaHeaderNames.CorrelationId, Encoding.UTF8.GetBytes(envelope.CorrelationId.ToString()) },
        };
        if (envelope.CausationId is { } causationId)
            headers.Add(KafkaHeaderNames.CausationId, Encoding.UTF8.GetBytes(causationId.ToString()));
        if (traceParent is not null)
            headers.Add(KafkaHeaderNames.TraceParent, Encoding.UTF8.GetBytes(traceParent));

        return new Message<string, string>
        {
            Key = envelope.OrderId.ToString(),
            Value = JsonSerializer.Serialize(envelope, EventJson.Options),
            Headers = headers,
        };
    }

    public static EventEnvelope ToEnvelope(Message<string, string> message) =>
        JsonSerializer.Deserialize<EventEnvelope>(message.Value, EventJson.Options)
        ?? throw new JsonException("Message value is an empty envelope.");

    public static string? Header(Message<string, string> message, string name) =>
        message.Headers?.TryGetLastBytes(name, out var bytes) == true ? Encoding.UTF8.GetString(bytes) : null;
}
