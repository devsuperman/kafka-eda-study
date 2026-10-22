namespace KafkaEdaStudy.Contracts;

public sealed record OrderLine(string Sku, int Quantity, decimal UnitPrice);

public sealed record OrderPlaced(Guid CustomerId, IReadOnlyList<OrderLine> Lines, decimal Total);

public sealed record OrderConfirmed;

public sealed record OrderCancelled(string Reason);

public sealed record OrderShipped(Guid ShipmentId);

public sealed record OrderDelivered(DateTimeOffset DeliveredAt);

public static class CancelReasons
{
    public const string OutOfStock = "out_of_stock";
    public const string PaymentDeclined = "payment_declined";
    public const string Timeout = "timeout";
    public const string Customer = "customer";
}
