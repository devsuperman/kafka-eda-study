namespace KafkaEdaStudy.Contracts;

public sealed record StockReserved(Guid ReservationId, DateTimeOffset ExpiresAt);

public sealed record StockReservationFailed(IReadOnlyList<string> MissingSkus);

public sealed record StockReleased(Guid ReservationId, string Reason);

public sealed record StockCommitted(Guid ReservationId);
