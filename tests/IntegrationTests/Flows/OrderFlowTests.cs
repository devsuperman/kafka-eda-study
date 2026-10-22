using System.Net;
using System.Net.Http.Json;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.IntegrationTests.Flows;

/// <summary>
/// The choreography of step 7, end to end: real services, real Kafka, real PostgreSQL. Each test starts its own set of
/// services and checks the end state in every service, as the Business Rules doc's flows describe them.
/// </summary>
public sealed class OrderFlowTests(InfraFixture infra)
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>Flow 1: stock and payment answer, the order is confirmed, shipped and delivered; stock is committed and the money captured.</summary>
    [Fact]
    public async Task Flow_1_happy_path_confirms_ships_and_delivers()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var env = FlowEnvironment.Start(infra);

        var placed = await PlaceOrderAsync(env, [("KB-001", 1), ("MS-002", 2)], ct);

        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Confirmed", "confirmation", ct);
        var confirmed = await GetOrderAsync(env, placed.OrderId, ct);
        Assert.True(confirmed.StockReserved);
        Assert.True(confirmed.PaymentAuthorized);

        // Shipping creates the shipment after OrderConfirmed, so the first dispatch may still find nothing (404).
        await WaitForAsync(async () => await TryPostAsync(env.Shipping, $"/shipments/{placed.OrderId}/dispatch", ct), "dispatch", ct);
        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Shipped", "shipment", ct);

        Assert.Equal(HttpStatusCode.OK, (await env.Shipping.PostAsync($"/shipments/{placed.OrderId}/deliver", null, ct)).StatusCode);
        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Delivered", "delivery", ct);

        // Shipped: payment captured (P1), reservation committed (I4).
        await WaitForAsync(async () => (await GetAuthorizationsAsync(env, placed.OrderId, ct)).Single().Status == "Captured", "capture", ct);
        Assert.Equal(200m, (await GetAuthorizationsAsync(env, placed.OrderId, ct)).Single().Amount);
        await WaitForAsync(async () => (await GetReservationsAsync(env, placed.OrderId, ct)).Single().Status == "Committed", "commit", ct);

        var stock = await GetStockAsync(env, ct);
        Assert.Equal((49, 0), (stock["KB-001"].Available, stock["KB-001"].Reserved));
        Assert.Equal((98, 0), (stock["MS-002"].Available, stock["MS-002"].Reserved));

        var shipment = Assert.Single(await GetShipmentsAsync(env, placed.OrderId, ct));
        Assert.Equal("Delivered", shipment.Status);
        Assert.StartsWith("TRK-", shipment.TrackingCode);
    }

    /// <summary>Flow 2: CB-006 has no stock, so the order is cancelled and the authorization made earlier is voided.</summary>
    [Fact]
    public async Task Flow_2_out_of_stock_cancels_the_order_and_voids_the_payment()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var env = FlowEnvironment.Start(infra);

        var placed = await PlaceOrderAsync(env, [("CB-006", 1)], ct);

        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Cancelled", "cancellation", ct);
        Assert.Equal("out_of_stock", (await GetOrderAsync(env, placed.OrderId, ct)).CancelReason);

        // Payment ran first (it authorized the total), then the cancellation voided it (P3).
        await WaitForAsync(async () => (await GetAuthorizationsAsync(env, placed.OrderId, ct)).Single().Status == "Voided", "void", ct);

        Assert.Empty(await GetReservationsAsync(env, placed.OrderId, ct));
        Assert.Equal(0, (await GetStockAsync(env, ct))["CB-006"].Available);
    }

    /// <summary>Flow 3: the laptop costs more than the 1,000 limit (P2), so payment is declined and the reservation released (I4).</summary>
    [Fact]
    public async Task Flow_3_payment_declined_cancels_the_order_and_releases_the_stock()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var env = FlowEnvironment.Start(infra);

        var placed = await PlaceOrderAsync(env, [("LT-005", 1)], ct);

        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Cancelled", "cancellation", ct);
        Assert.Equal("payment_declined", (await GetOrderAsync(env, placed.OrderId, ct)).CancelReason);

        var authorization = Assert.Single(await GetAuthorizationsAsync(env, placed.OrderId, ct));
        Assert.Equal(("Declined", "over_limit"), (authorization.Status, authorization.DeclineReason));

        await WaitForAsync(async () => (await GetReservationsAsync(env, placed.OrderId, ct)).Single().Status == "Released", "release", ct);
        var laptop = (await GetStockAsync(env, ct))["LT-005"];
        Assert.Equal((5, 0), (laptop.Available, laptop.Reserved));
    }

    /// <summary>Flow 5: the customer cancels a confirmed order; stock is released and the authorization voided.</summary>
    [Fact]
    public async Task Flow_5_customer_cancel_releases_the_stock_and_voids_the_payment()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var env = FlowEnvironment.Start(infra);

        var placed = await PlaceOrderAsync(env, [("HD-004", 2)], ct);
        await WaitForAsync(async () => (await GetOrderAsync(env, placed.OrderId, ct)).Status == "Confirmed", "confirmation", ct);

        var cancel = await env.Orders.PostAsync($"/orders/{placed.OrderId}/cancel", null, ct);
        Assert.Equal(HttpStatusCode.OK, cancel.StatusCode);

        await WaitForAsync(async () => (await GetReservationsAsync(env, placed.OrderId, ct)).Single().Status == "Released", "release", ct);
        await WaitForAsync(async () => (await GetAuthorizationsAsync(env, placed.OrderId, ct)).Single().Status == "Voided", "void", ct);

        var order = await GetOrderAsync(env, placed.OrderId, ct);
        Assert.Equal(("Cancelled", "customer"), (order.Status, order.CancelReason));
        Assert.Equal(30, (await GetStockAsync(env, ct))["HD-004"].Available);

        // Rule O6: a cancelled order cannot be cancelled again.
        Assert.Equal(HttpStatusCode.Conflict, (await env.Orders.PostAsync($"/orders/{placed.OrderId}/cancel", null, ct)).StatusCode);
    }

    private static async Task<PlacedOrder> PlaceOrderAsync(FlowEnvironment env, (string Sku, int Quantity)[] items, CancellationToken ct)
    {
        var lines = items
            .Select(i => new OrderLine(i.Sku, i.Quantity, Products.Find(i.Sku)!.Price))
            .ToList();
        using var response = await env.Orders.PostAsJsonAsync("/orders", new PlaceOrderBody(Guid.NewGuid(), lines), ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<PlacedOrder>(ct))!;
    }

    private static Task<OrderView> GetOrderAsync(FlowEnvironment env, Guid orderId, CancellationToken ct) =>
        GetAsync<OrderView>(env.Orders, $"/orders/{orderId}", ct);

    private static Task<List<AuthorizationView>> GetAuthorizationsAsync(FlowEnvironment env, Guid orderId, CancellationToken ct) =>
        GetAsync<List<AuthorizationView>>(env.Payments, $"/authorizations?orderId={orderId}", ct);

    private static Task<List<ReservationView>> GetReservationsAsync(FlowEnvironment env, Guid orderId, CancellationToken ct) =>
        GetAsync<List<ReservationView>>(env.Inventory, $"/reservations?orderId={orderId}", ct);

    private static Task<List<ShipmentView>> GetShipmentsAsync(FlowEnvironment env, Guid orderId, CancellationToken ct) =>
        GetAsync<List<ShipmentView>>(env.Shipping, $"/shipments?orderId={orderId}", ct);

    private static async Task<Dictionary<string, StockView>> GetStockAsync(FlowEnvironment env, CancellationToken ct) =>
        (await GetAsync<List<StockView>>(env.Inventory, "/stock", ct)).ToDictionary(s => s.Sku);

    private static async Task<T> GetAsync<T>(HttpClient http, string path, CancellationToken ct)
    {
        using var response = await http.GetAsync(path, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<T>(ct))!;
    }

    /// <summary>Returns true when the endpoint accepted the action, false while the shipment does not exist yet.</summary>
    private static async Task<bool> TryPostAsync(HttpClient http, string path, CancellationToken ct)
    {
        using var response = await http.PostAsync(path, null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return false;
        response.EnsureSuccessStatusCode();
        return true;
    }

    private static async Task WaitForAsync(Func<Task<bool>> condition, string what, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting for {what}.");
            await Task.Delay(200, ct);
        }
    }
}
