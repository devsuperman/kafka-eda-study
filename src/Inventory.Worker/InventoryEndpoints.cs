using KafkaEdaStudy.Inventory.Data;
using KafkaEdaStudy.Inventory.Domain;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Inventory;

public sealed record StockItemDto(string Sku, string Name, int Available, int Reserved);

public sealed record ReservationLineDto(string Sku, int Quantity);

public sealed record ReservationDto(
    Guid Id,
    Guid OrderId,
    ReservationStatus Status,
    IReadOnlyList<ReservationLineDto> Lines,
    DateTimeOffset CreatedAt,
    DateTimeOffset ExpiresAt)
{
    public static ReservationDto From(Reservation r) => new(
        r.Id, r.OrderId, r.Status,
        [.. r.Lines.Select(l => new ReservationLineDto(l.Sku, l.Quantity))],
        r.CreatedAt, r.ExpiresAt);
}

public static class InventoryEndpoints
{
    public static IEndpointRouteBuilder MapInventoryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/stock", GetStockAsync).WithTags("Stock");
        app.MapGet("/reservations", ListReservationsAsync).WithTags("Reservations");
        return app;
    }

    private static async Task<Ok<List<StockItemDto>>> GetStockAsync(InventoryDbContext db, CancellationToken ct)
    {
        var items = await db.StockItems.AsNoTracking()
            .OrderBy(s => s.Sku)
            .Select(s => new StockItemDto(s.Sku, s.Name, s.Available, s.Reserved))
            .ToListAsync(ct);
        return TypedResults.Ok(items);
    }

    private static async Task<Ok<List<ReservationDto>>> ListReservationsAsync(
        InventoryDbContext db, CancellationToken ct, int limit = 50)
    {
        var reservations = await db.Reservations.AsNoTracking()
            .OrderByDescending(r => r.CreatedAt)
            .Take(Math.Clamp(limit, 1, 200))
            .ToListAsync(ct);
        return TypedResults.Ok(reservations.Select(ReservationDto.From).ToList());
    }
}
