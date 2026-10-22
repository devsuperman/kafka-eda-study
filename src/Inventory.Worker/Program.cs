using System.Text.Json.Serialization;
using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Inventory;
using KafkaEdaStudy.Inventory.Consumers;
using KafkaEdaStudy.Inventory.Data;
using Microsoft.EntityFrameworkCore;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.Configure<InventoryOptions>(builder.Configuration.GetSection(InventoryOptions.Section));
builder.Services.AddDbContext<InventoryDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Inventory")));
// Step 4: inbox (skip already-processed events). Step 6: outbox + relay (each of the 3 replicas runs a relay;
// FOR UPDATE SKIP LOCKED keeps them from publishing the same row).
builder.Services.AddInbox<InventoryDbContext>();
builder.Services.AddOutbox<InventoryDbContext>();
builder.Services.AddScoped<ReserveStockHandler>();
builder.Services.AddScoped<StockOutcomeHandler>(); // step 7: release on OrderCancelled, commit on OrderShipped
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
    await db.Database.MigrateAsync();
    await StockSeeder.SeedAsync(db, app.Logger);
}

app.Logger.LogInformation("Inventory instance {Instance} starting", Environment.MachineName);

// Study project: API docs are enabled in every environment.
app.MapOpenApi();
app.MapScalarApiReference();

app.MapDefaultEndpoints();
app.MapInventoryEndpoints();

app.Run();

public partial class Program { }
