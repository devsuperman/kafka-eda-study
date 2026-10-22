using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Shipping.Data;
using KafkaEdaStudy.Shipping.Domain;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Shipping;

public sealed record ShipmentDto(
    Guid Id,
    Guid OrderId,
    ShipmentStatus Status,
    string? TrackingCode,
    DateTimeOffset CreatedAt,
    DateTimeOffset? DispatchedAt,
    DateTimeOffset? DeliveredAt)
{
    public static ShipmentDto From(Shipment s) => new(
        s.Id, s.OrderId, s.Status, s.TrackingCode, s.CreatedAt, s.DispatchedAt, s.DeliveredAt);
}

public static class ShipmentEndpoints
{
    public static IEndpointRouteBuilder MapShipmentEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/shipments", ListShipmentsAsync).WithTags("Shipments");
        app.MapPost("/shipments/{orderId:guid}/dispatch", DispatchAsync).WithTags("Shipments");
        app.MapPost("/shipments/{orderId:guid}/deliver", DeliverAsync).WithTags("Shipments");
        return app;
    }

    private static async Task<Ok<List<ShipmentDto>>> ListShipmentsAsync(
        ShippingDbContext db, CancellationToken ct, Guid? orderId = null, ShipmentStatus? status = null, int limit = 50)
    {
        var query = db.Shipments.AsNoTracking();
        if (orderId is { } id)
            query = query.Where(x => x.OrderId == id);
        if (status is { } s)
            query = query.Where(x => x.Status == s);
        var shipments = await query
            .OrderByDescending(x => x.CreatedAt)
            .Take(Math.Clamp(limit, 1, 200))
            .ToListAsync(ct);
        return TypedResults.Ok(shipments.Select(ShipmentDto.From).ToList());
    }

    /// <summary>Rule S2: a person hands the parcel to the carrier. Orders learns about it from ShipmentDispatched.</summary>
    private static Task<Results<Ok<ShipmentDto>, NotFound, ProblemHttpResult>> DispatchAsync(
        Guid orderId, ShippingDbContext db, IOutbox outbox, TimeProvider clock, CancellationToken ct) =>
        ChangeAsync(orderId, db, clock, ct, (shipment, now) =>
        {
            if (!shipment.Dispatch(now))
                return null;
            outbox.Add(Topics.Shipping, EventEnvelope.Create(
                new ShipmentDispatched(shipment.Id, shipment.TrackingCode!), shipment.OrderId, shipment.CorrelationId));
            return shipment;
        }, "dispatched");

    /// <summary>Rule S2: the carrier reports the parcel delivered. Orders learns about it from ShipmentDelivered.</summary>
    private static Task<Results<Ok<ShipmentDto>, NotFound, ProblemHttpResult>> DeliverAsync(
        Guid orderId, ShippingDbContext db, IOutbox outbox, TimeProvider clock, CancellationToken ct) =>
        ChangeAsync(orderId, db, clock, ct, (shipment, now) =>
        {
            if (!shipment.Deliver(now))
                return null;
            outbox.Add(Topics.Shipping, EventEnvelope.Create(
                new ShipmentDelivered(shipment.Id, now), shipment.OrderId, shipment.CorrelationId));
            return shipment;
        }, "delivered");

    private static async Task<Results<Ok<ShipmentDto>, NotFound, ProblemHttpResult>> ChangeAsync(
        Guid orderId,
        ShippingDbContext db,
        TimeProvider clock,
        CancellationToken ct,
        Func<Shipment, DateTimeOffset, Shipment?> change,
        string verb)
    {
        var shipment = await db.Shipments.FirstOrDefaultAsync(s => s.OrderId == orderId, ct);
        if (shipment is null)
            return TypedResults.NotFound();

        if (change(shipment, clock.GetUtcNow()) is null)
            return Conflict($"A shipment that is {shipment.Status} cannot be {verb}.");

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateConcurrencyException)
        {
            return Conflict("The shipment changed at the same time; reload it and try again.");
        }

        return TypedResults.Ok(ShipmentDto.From(shipment));
    }

    private static ProblemHttpResult Conflict(string detail) =>
        TypedResults.Problem(detail: detail, statusCode: StatusCodes.Status409Conflict);
}
