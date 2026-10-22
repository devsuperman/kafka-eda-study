namespace KafkaEdaStudy.Contracts;

public sealed record PaymentAuthorized(Guid AuthorizationId, decimal Amount);

public sealed record PaymentDeclined(string Reason);

public sealed record PaymentCaptured(decimal Amount);

public sealed record PaymentVoided(Guid AuthorizationId);

public sealed record PaymentRefunded(decimal Amount);

public static class DeclineReasons
{
    /// <summary>Rule P2: the order total is above the 1,000 limit of the simulated gateway.</summary>
    public const string OverLimit = "over_limit";
}
