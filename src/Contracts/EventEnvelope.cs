using System.Text.Json;

namespace KafkaEdaStudy.Contracts;

/// <summary>
/// Wraps every event published to Kafka. The Kafka message key is always <see cref="OrderId"/>.
/// </summary>
public sealed record EventEnvelope(
    Guid EventId,
    string EventType,
    int Version,
    DateTimeOffset OccurredAt,
    Guid OrderId,
    Guid CorrelationId,
    Guid? CausationId,
    JsonElement Payload)
{
    public static EventEnvelope Create<T>(
        T payload, Guid orderId, Guid correlationId, Guid? causationId = null, int version = 1)
        where T : notnull =>
        new(
            Guid.NewGuid(),
            typeof(T).Name,
            version,
            DateTimeOffset.UtcNow,
            orderId,
            correlationId,
            causationId,
            JsonSerializer.SerializeToElement(payload, EventJson.Options));

    /// <summary>Creates an event caused by <paramref name="cause"/>, keeping its order and correlation.</summary>
    public static EventEnvelope CausedBy<T>(EventEnvelope cause, T payload, int version = 1)
        where T : notnull =>
        Create(payload, cause.OrderId, cause.CorrelationId, cause.EventId, version);

    public T GetPayload<T>() =>
        Payload.Deserialize<T>(EventJson.Options)
        ?? throw new InvalidOperationException($"Event {EventId} has an empty {EventType} payload.");

    public bool Is<T>() => EventType == typeof(T).Name;
}

public static class EventJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
