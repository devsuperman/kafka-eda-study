using System.Text.Json.Serialization;
using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Shipping;
using KafkaEdaStudy.Shipping.Consumers;
using KafkaEdaStudy.Shipping.Data;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddProblemDetails();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddDbContext<ShippingDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Shipping")));
builder.Services.AddInbox<ShippingDbContext>();
builder.Services.AddOutbox<ShippingDbContext>();
builder.Services.AddScoped<ShipmentHandler>();
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<ShippingDbContext>().Database.MigrateAsync();

app.MapDefaultEndpoints();
app.MapShipmentEndpoints();

app.Run();

public partial class Program { }
