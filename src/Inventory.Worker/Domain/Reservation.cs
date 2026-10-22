using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Inventory.Domain;

public enum ReservationStatus
{
    Active,
    Committed,
    Released,
    Expired,
}

/// <summary>One SKU held by a reservation (an EF owned type, stored in its own table).</summary>
public sealed class ReservationLine
{
    public required string Sku { get; init; }
    public required int Quantity { get; init; }
}

/// <summary>Outcome of <see cref="Reservation.TryReserve"/>: a reservation, or the SKUs that could not be reserved.</summary>
public sealed record ReserveResult(Reservation? Reservation, IReadOnlyList<string> MissingSkus)
{
    public bool Succeeded => Reservation is not null;
}

/// <summary>Stock held for one order until it is committed, released or expires.</summary>
public sealed class Reservation
{
    private readonly List<ReservationLine> _lines = [];

    private Reservation() { } // EF Core

    public Guid Id { get; private set; }
    public Guid OrderId { get; private set; }
    public ReservationStatus Status { get; private set; }
    public IReadOnlyList<ReservationLine> Lines => _lines;
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset ExpiresAt { get; private set; }

    /// <summary>
    /// Rule I1: reserves every line of the order or none. Rule I3: stock never goes negative.
    /// Lines for the same SKU are summed. A SKU is "missing" when it is unknown (not in <paramref name="stock"/>)
    /// or has fewer units available than requested. On failure <paramref name="stock"/> is left untouched.
    /// </summary>
    public static ReserveResult TryReserve(
        Guid orderId,
        IReadOnlyList<OrderLine> lines,
        IReadOnlyDictionary<string, StockItem> stock,
        DateTimeOffset now,
        TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(lines);
        if (lines.Count == 0)
            throw new ArgumentException("An order to reserve must have at least one line.", nameof(lines));

        // Sum per SKU, keeping the order in which SKUs first appear.
        var requested = lines
            .GroupBy(l => l.Sku, StringComparer.Ordinal)
            .Select(g => new ReservationLine { Sku = g.Key, Quantity = g.Sum(l => l.Quantity) })
            .ToList();

        // Check everything first, change nothing until we know every line fits (all or none).
        var missing = requested
            .Where(r => !stock.TryGetValue(r.Sku, out var item) || !item.CanReserve(r.Quantity))
            .Select(r => r.Sku)
            .ToList();
        if (missing.Count > 0)
            return new ReserveResult(null, missing);

        foreach (var line in requested)
            stock[line.Sku].Reserve(line.Quantity);

        var reservation = new Reservation
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            Status = ReservationStatus.Active,
            CreatedAt = now,
            ExpiresAt = now + timeout,
        };
        reservation._lines.AddRange(requested);
        return new ReserveResult(reservation, []);
    }
}
