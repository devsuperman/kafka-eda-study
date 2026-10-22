using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Orders.Domain;

public enum OrderStatus
{
    Pending,
    Confirmed,
    Shipped,
    Delivered,
    Cancelled,
}

/// <summary>One line of an order (an EF owned type, stored in its own table).</summary>
public sealed class OrderLineItem
{
    public required string Sku { get; init; }
    public required int Quantity { get; init; }
    public required decimal UnitPrice { get; init; }

    public decimal Subtotal => Quantity * UnitPrice;
}

/// <summary>Outcome of <see cref="Order.Place"/>: either an order or validation errors keyed by field.</summary>
public sealed record PlaceOrderResult(Order? Order, IReadOnlyDictionary<string, string[]> Errors)
{
    public bool IsValid => Order is not null;
}

/// <summary>Order aggregate. Domain rules live here so the API and future consumers share them.</summary>
public sealed class Order
{
    public const int MinQuantity = 1;
    public const int MaxQuantity = 10;

    private readonly List<OrderLineItem> _lines = [];

    private Order() { } // EF Core

    public Guid Id { get; private set; }
    public Guid CustomerId { get; private set; }
    public Guid CorrelationId { get; private set; }
    public OrderStatus Status { get; private set; }
    public string? CancelReason { get; private set; }
    public IReadOnlyList<OrderLineItem> Lines => _lines;
    public decimal Total { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset UpdatedAt { get; private set; }

    /// <summary>Rule O4: the two answers a Pending order waits for. Set by <see cref="RecordStockReserved"/> and
    /// <see cref="RecordPaymentAuthorized"/>; the order is confirmed once both are true, in any order.</summary>
    public bool StockReserved { get; private set; }
    public bool PaymentAuthorized { get; private set; }

    /// <summary>
    /// Optimistic concurrency token, mapped to Postgres' <c>xmin</c>. The saga consumer and the customer's cancel
    /// request can change the same order at the same time; the loser gets <c>DbUpdateConcurrencyException</c>.
    /// </summary>
    public uint Version { get; private set; }

    /// <summary>
    /// Creates a Pending order, enforcing rule O1: at least one line, quantity 1..10 per line,
    /// every SKU in <see cref="Products.All"/>, no duplicate SKUs. Invalid input returns errors instead of throwing.
    /// </summary>
    public static PlaceOrderResult Place(
        Guid customerId, Guid correlationId, IReadOnlyList<OrderLine>? lines, DateTimeOffset now)
    {
        var errors = new Dictionary<string, List<string>>();
        void Add(string key, string message)
        {
            if (!errors.TryGetValue(key, out var list))
                errors[key] = list = [];
            list.Add(message);
        }

        if (lines is null || lines.Count == 0)
        {
            Add("lines", "An order must have at least one line.");
        }
        else
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            for (var i = 0; i < lines.Count; i++)
            {
                var line = lines[i];
                var key = $"lines[{i}]";

                if (string.IsNullOrWhiteSpace(line.Sku))
                    Add($"{key}.sku", "SKU is required.");
                else if (Products.Find(line.Sku) is null)
                    Add($"{key}.sku", $"Unknown SKU '{line.Sku}'.");
                else if (!seen.Add(line.Sku))
                    Add($"{key}.sku", $"Duplicate SKU '{line.Sku}'; use one line per SKU.");

                if (line.Quantity is < MinQuantity or > MaxQuantity)
                    Add($"{key}.quantity", $"Quantity must be between {MinQuantity} and {MaxQuantity}.");

                if (line.UnitPrice <= 0)
                    Add($"{key}.unitPrice", "Unit price must be greater than zero.");
            }
        }

        if (errors.Count > 0)
            return new PlaceOrderResult(null, errors.ToDictionary(e => e.Key, e => e.Value.ToArray()));

        var order = new Order
        {
            Id = Guid.NewGuid(),
            CustomerId = customerId,
            CorrelationId = correlationId,
            Status = OrderStatus.Pending,
            CreatedAt = now,
            UpdatedAt = now,
        };
        order._lines.AddRange(lines!.Select(l => new OrderLineItem
        {
            Sku = l.Sku,
            Quantity = l.Quantity,
            UnitPrice = l.UnitPrice,
        }));
        order.Total = order._lines.Sum(l => l.Subtotal);

        return new PlaceOrderResult(order, new Dictionary<string, string[]>());
    }

    /// <summary>
    /// Rule O3: the only allowed status transitions. Events can arrive late or twice, so a transition
    /// that is not in this table is rejected (the caller ignores it) instead of corrupting the status.
    /// </summary>
    public static readonly IReadOnlyDictionary<OrderStatus, OrderStatus[]> AllowedTransitions =
        new Dictionary<OrderStatus, OrderStatus[]>
        {
            [OrderStatus.Pending] = [OrderStatus.Confirmed, OrderStatus.Cancelled],
            [OrderStatus.Confirmed] = [OrderStatus.Shipped, OrderStatus.Cancelled],
            [OrderStatus.Shipped] = [OrderStatus.Delivered],
            [OrderStatus.Delivered] = [],
            [OrderStatus.Cancelled] = [],
        };

    public static bool CanTransition(OrderStatus from, OrderStatus to) => AllowedTransitions[from].Contains(to);

    /// <summary>Returns true the first time the answer is recorded; false for a duplicate or when no answer is awaited.</summary>
    public bool RecordStockReserved(DateTimeOffset now)
    {
        if (Status != OrderStatus.Pending || StockReserved)
            return false;
        StockReserved = true;
        UpdatedAt = now;
        return true;
    }

    /// <summary>Returns true the first time the answer is recorded; false for a duplicate or when no answer is awaited.</summary>
    public bool RecordPaymentAuthorized(DateTimeOffset now)
    {
        if (Status != OrderStatus.Pending || PaymentAuthorized)
            return false;
        PaymentAuthorized = true;
        UpdatedAt = now;
        return true;
    }

    /// <summary>Rule O4: confirmed only when both stock and payment answered positively.</summary>
    public bool IsReadyToConfirm => Status == OrderStatus.Pending && StockReserved && PaymentAuthorized;

    public bool Confirm(DateTimeOffset now) => TransitionTo(OrderStatus.Confirmed, now);

    public bool Ship(DateTimeOffset now) => TransitionTo(OrderStatus.Shipped, now);

    public bool Deliver(DateTimeOffset now) => TransitionTo(OrderStatus.Delivered, now);

    /// <summary>Rule O6: cancelling is possible until the order is Shipped (enforced by the transition table).</summary>
    public bool Cancel(string reason, DateTimeOffset now)
    {
        if (!TransitionTo(OrderStatus.Cancelled, now))
            return false;
        CancelReason = reason;
        return true;
    }

    /// <summary>Returns false and leaves the order unchanged when the transition is not allowed.</summary>
    private bool TransitionTo(OrderStatus next, DateTimeOffset now)
    {
        if (!CanTransition(Status, next))
            return false;
        Status = next;
        UpdatedAt = now;
        return true;
    }
}
