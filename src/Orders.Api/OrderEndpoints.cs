using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Orders.Data;
using KafkaEdaStudy.Orders.Domain;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Orders;

public sealed record PlaceOrderRequest(Guid? CustomerId, IReadOnlyList<OrderLine>? Lines);

public sealed record PlaceOrderResponse(Guid OrderId, Guid CorrelationId, OrderStatus Status);

public sealed record OrderLineDto(string Sku, int Quantity, decimal UnitPrice);

public sealed record OrderDto(
    Guid Id,
    Guid CustomerId,
    Guid CorrelationId,
    OrderStatus Status,
    string? CancelReason,
    IReadOnlyList<OrderLineDto> Lines,
    decimal Total,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt)
{
    public static OrderDto From(Order o) => new(
        o.Id, o.CustomerId, o.CorrelationId, o.Status, o.CancelReason,
        [.. o.Lines.Select(l => new OrderLineDto(l.Sku, l.Quantity, l.UnitPrice))],
        o.Total, o.CreatedAt, o.UpdatedAt);
}

public static class OrderEndpoints
{
    public const string CorrelationHeader = "X-Correlation-Id";

    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/orders", PlaceOrderAsync).WithTags("Orders");
        app.MapGet("/orders/{id:guid}", GetOrderAsync).WithTags("Orders");
        app.MapGet("/orders", ListOrdersAsync).WithTags("Orders");
        app.MapGet("/products", () => Products.All).WithTags("Products");
        return app;
    }

    private static async Task<Results<Created<PlaceOrderResponse>, ValidationProblem>> PlaceOrderAsync(
        PlaceOrderRequest request,
        HttpRequest http,
        OrdersDbContext db,
        IOutbox outbox,
        TimeProvider clock,
        CancellationToken ct)
    {
        var correlationId = Guid.TryParse(http.Headers[CorrelationHeader], out var fromHeader)
            ? fromHeader
            : Guid.NewGuid();

        var result = Order.Place(
            request.CustomerId ?? Guid.NewGuid(), correlationId, request.Lines, clock.GetUtcNow());
        if (result.Order is not { } order)
            return TypedResults.ValidationProblem(result.Errors);

        // Transactional outbox (step 6): the order and its OrderPlaced event are written by ONE SaveChanges,
        // i.e. one database transaction. Either both exist or neither does; the outbox relay publishes the
        // event to Kafka afterwards, even if this process crashes right after the commit.
        var placed = new OrderPlaced(
            order.CustomerId,
            [.. order.Lines.Select(l => new OrderLine(l.Sku, l.Quantity, l.UnitPrice))],
            order.Total);
        db.Orders.Add(order);
        outbox.Add(Topics.Orders, EventEnvelope.Create(placed, order.Id, correlationId));
        await db.SaveChangesAsync(ct);

        return TypedResults.Created(
            $"/orders/{order.Id}", new PlaceOrderResponse(order.Id, correlationId, order.Status));
    }

    private static async Task<Results<Ok<OrderDto>, NotFound>> GetOrderAsync(
        Guid id, OrdersDbContext db, CancellationToken ct)
    {
        var order = await db.Orders.AsNoTracking().FirstOrDefaultAsync(o => o.Id == id, ct);
        return order is null ? TypedResults.NotFound() : TypedResults.Ok(OrderDto.From(order));
    }

    private static async Task<Ok<List<OrderDto>>> ListOrdersAsync(
        OrdersDbContext db, CancellationToken ct, int limit = 50)
    {
        var orders = await db.Orders.AsNoTracking()
            .OrderByDescending(o => o.CreatedAt)
            .Take(Math.Clamp(limit, 1, 200))
            .ToListAsync(ct);
        return TypedResults.Ok(orders.Select(OrderDto.From).ToList());
    }
}
