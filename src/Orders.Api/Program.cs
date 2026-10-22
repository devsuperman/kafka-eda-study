using System.Text.Json.Serialization;
using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Orders;
using KafkaEdaStudy.Orders.Data;
using Microsoft.EntityFrameworkCore;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddDbContext<OrdersDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Orders")));
// Step 6: IOutbox for the endpoints + the relay that publishes outbox rows to Kafka.
builder.Services.AddOutbox<OrdersDbContext>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<OrdersDbContext>().Database.MigrateAsync();

// Study project: API docs are enabled in every environment.
app.MapOpenApi();
app.MapScalarApiReference();

app.MapDefaultEndpoints();
app.MapOrderEndpoints();

app.Run();
