namespace KafkaEdaStudy.Contracts;

public sealed record ShipmentCreated(Guid ShipmentId);

public sealed record ShipmentDispatched(Guid ShipmentId, string TrackingCode);

public sealed record ShipmentDelivered(Guid ShipmentId, DateTimeOffset DeliveredAt);
