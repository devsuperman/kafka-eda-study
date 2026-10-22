extern alias OrdersApi;
extern alias InventoryWorker;
extern alias PaymentsWorker;
extern alias ShippingApi;

using System.Net.Http.Json;
using KafkaEdaStudy.Contracts;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Npgsql;

namespace KafkaEdaStudy.IntegrationTests.Flows;

/// <summary>
/// The services of the order flow (Orders, Inventory, Payments, Shipping), each started as a real host with its own
/// Program.cs as in compose. They share the Kafka and PostgreSQL containers of the test run, so events really travel
/// through the broker, and every host gets its own database, so tests do not see each other's orders.
/// </summary>
public sealed class FlowEnvironment : IAsyncDisposable
{
    private readonly IDisposable[] _hosts;

    private FlowEnvironment(IDisposable[] hosts, HttpClient orders, HttpClient inventory, HttpClient payments, HttpClient shipping)
    {
        _hosts = hosts;
        Orders = orders;
        Inventory = inventory;
        Payments = payments;
        Shipping = shipping;
    }

    public HttpClient Orders { get; }
    public HttpClient Inventory { get; }
    public HttpClient Payments { get; }
    public HttpClient Shipping { get; }

    public static FlowEnvironment Start(InfraFixture infra)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var orders = new ServiceHost<OrdersApi::Program>("Orders", DatabaseFor("orders", suffix, infra), infra);
        var inventory = new ServiceHost<InventoryWorker::Program>("Inventory", DatabaseFor("inventory", suffix, infra), infra);
        var payments = new ServiceHost<PaymentsWorker::Program>("Payments", DatabaseFor("payments", suffix, infra), infra);
        var shipping = new ServiceHost<ShippingApi::Program>("Shipping", DatabaseFor("shipping", suffix, infra), infra);

        // CreateClient starts each host, which also starts its outbox relay and its Kafka consumers.
        return new FlowEnvironment(
            [orders, inventory, payments, shipping],
            orders.CreateClient(), inventory.CreateClient(), payments.CreateClient(), shipping.CreateClient());
    }

    public ValueTask DisposeAsync()
    {
        foreach (var host in _hosts)
            host.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>A connection string to a database of its own (EF Core creates it on the first migration).</summary>
    private static string DatabaseFor(string service, string suffix, InfraFixture infra) =>
        new NpgsqlConnectionStringBuilder(infra.Postgres.GetConnectionString())
        {
            Database = $"{service}_{suffix}",
        }.ConnectionString;

    /// <summary>Starts one service in this process, pointed at the test run's Kafka and its own database.</summary>
    private sealed class ServiceHost<TProgram>(string connectionName, string connectionString, InfraFixture infra)
        : WebApplicationFactory<TProgram>
        where TProgram : class
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseContentRoot(AppContext.BaseDirectory);
            builder.UseSetting($"ConnectionStrings:{connectionName}", connectionString);
            builder.UseSetting("Kafka:BootstrapServers", infra.BootstrapServers);
        }
    }
}

/// <summary>Response shapes read by the flow tests (only the fields they check).</summary>
public sealed record OrderView(Guid Id, string Status, string? CancelReason, bool StockReserved, bool PaymentAuthorized);

public sealed record ReservationView(Guid Id, Guid OrderId, string Status);

public sealed record StockView(string Sku, string Name, int Available, int Reserved);

public sealed record AuthorizationView(Guid Id, Guid OrderId, decimal Amount, string Status, string? DeclineReason);

public sealed record ShipmentView(Guid Id, Guid OrderId, string Status, string? TrackingCode);

public sealed record PlaceOrderBody(Guid CustomerId, IReadOnlyList<OrderLine> Lines);

public sealed record PlacedOrder(Guid OrderId, Guid CorrelationId, string Status);
