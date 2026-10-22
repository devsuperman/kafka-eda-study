using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Inventory.Domain;

namespace KafkaEdaStudy.Inventory.UnitTests;

/// <summary>Rule I4: a cancelled order releases its reservation, a shipped order commits it.</summary>
public sealed class ReservationSettlementTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static (Reservation Reservation, Dictionary<string, StockItem> Stock) ReservedKeyboards(int quantity)
    {
        var stock = Products.All.ToDictionary(p => p.Sku, p => new StockItem(p.Sku, p.Name, p.InitialStock));
        var result = Reservation.TryReserve(Guid.NewGuid(), [new OrderLine("KB-001", quantity, 120m)], stock, Now, TimeSpan.FromMinutes(15));
        return (result.Reservation!, stock);
    }

    [Fact]
    public void Release_gives_the_units_back_to_available()
    {
        var (reservation, stock) = ReservedKeyboards(2);

        Assert.True(reservation.Release(stock));

        Assert.Equal(ReservationStatus.Released, reservation.Status);
        Assert.Equal(50, stock["KB-001"].Available);
        Assert.Equal(0, stock["KB-001"].Reserved);
    }

    [Fact]
    public void Commit_takes_the_units_out_of_reserved_without_touching_available()
    {
        var (reservation, stock) = ReservedKeyboards(2);

        Assert.True(reservation.Commit(stock));

        Assert.Equal(ReservationStatus.Committed, reservation.Status);
        Assert.Equal(48, stock["KB-001"].Available);
        Assert.Equal(0, stock["KB-001"].Reserved);
    }

    [Fact]
    public void A_settled_reservation_cannot_be_settled_again()
    {
        var (reservation, stock) = ReservedKeyboards(2);
        Assert.True(reservation.Release(stock));

        Assert.False(reservation.Release(stock));
        Assert.False(reservation.Commit(stock));
        Assert.Equal(50, stock["KB-001"].Available);
    }

    [Fact]
    public void Release_never_pushes_available_above_the_initial_stock()
    {
        var (reservation, stock) = ReservedKeyboards(3);
        reservation.Release(stock);

        Assert.Equal(50, stock["KB-001"].Available + stock["KB-001"].Reserved);
    }
}
