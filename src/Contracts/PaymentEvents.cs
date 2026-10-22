namespace KafkaEdaStudy.Contracts;

public sealed record PaymentAuthorized(Guid AuthorizationId, decimal Amount);

public sealed record PaymentDeclined(string Reason);

public sealed record PaymentCaptured(decimal Amount);

public sealed record PaymentVoided(Guid AuthorizationId);

public sealed record PaymentRefunded(decimal Amount);
