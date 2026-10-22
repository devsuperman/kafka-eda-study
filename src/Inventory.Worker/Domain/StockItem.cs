namespace KafkaEdaStudy.Inventory.Domain;

/// <summary>Stock of one SKU. <see cref="Available"/> can be reserved; <see cref="Reserved"/> is held for orders.</summary>
public sealed class StockItem
{
    private StockItem() { } // EF Core

    public StockItem(string sku, string name, int available)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sku);
        ArgumentOutOfRangeException.ThrowIfNegative(available);
        Sku = sku;
        Name = name;
        Available = available;
    }

    public string Sku { get; private set; } = null!;
    public string Name { get; private set; } = null!;
    public int Available { get; private set; }
    public int Reserved { get; private set; }

    /// <summary>
    /// Optimistic concurrency token, mapped to Postgres' system column <c>xmin</c> (the id of the
    /// transaction that last wrote the row). An UPDATE whose xmin no longer matches affects 0 rows and
    /// EF Core throws <c>DbUpdateConcurrencyException</c>.
    /// </summary>
    public uint Version { get; private set; }

    /// <summary>Rule I3: a reservation may never take <see cref="Available"/> below zero.</summary>
    public bool CanReserve(int quantity) => quantity > 0 && quantity <= Available;

    public void Reserve(int quantity)
    {
        if (!CanReserve(quantity))
            throw new InvalidOperationException(
                $"Cannot reserve {quantity} of {Sku}: only {Available} available.");

        Available -= quantity;
        Reserved += quantity;
    }
}
