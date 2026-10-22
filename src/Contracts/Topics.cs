namespace KafkaEdaStudy.Contracts;

public static class Topics
{
    public const string Orders = "orders.events";
    public const string Inventory = "inventory.events";
    public const string Payments = "payments.events";
    public const string Shipping = "shipping.events";

    public static readonly IReadOnlyList<string> All = [Orders, Inventory, Payments, Shipping];

    public static string Dlq(string topic) => $"{topic}.dlq";
}

public static class KafkaHeaderNames
{
    public const string EventType = "event-type";
    public const string EventVersion = "event-version";
    public const string CorrelationId = "correlation-id";
    public const string CausationId = "causation-id";
    public const string TraceParent = "traceparent";
    public const string Error = "error";
    public const string OriginalTopic = "original-topic";

    // Dead-letter headers (step 5): where the message came from and why it was set aside.
    public const string OriginalPartition = "original-partition";
    public const string OriginalOffset = "original-offset";
    public const string ConsumerGroup = "consumer-group";
    public const string FailedAt = "failed-at";
    public const string Attempts = "attempts";
}
