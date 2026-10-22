using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Payments.Consumers;
using KafkaEdaStudy.Payments.Data;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddDbContext<PaymentsDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Payments")));
// Step 4: inbox (P4 and N2-style idempotency). Step 6: outbox, so PaymentAuthorized is never lost with the database write.
builder.Services.AddInbox<PaymentsDbContext>();
builder.Services.AddOutbox<PaymentsDbContext>();
builder.Services.AddScoped<PaymentHandler>();
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<PaymentsDbContext>().Database.MigrateAsync();

app.MapDefaultEndpoints();

// Authorizations of an order (or the latest ones), for the Playground and for checking the flows.
app.MapGet("/authorizations", async (PaymentsDbContext db, Guid? orderId, int? limit, CancellationToken ct) =>
{
    var query = db.Authorizations.AsNoTracking();
    if (orderId is { } id)
        query = query.Where(a => a.OrderId == id);
    return await query
        .OrderByDescending(a => a.CreatedAt)
        .Take(Math.Clamp(limit ?? 50, 1, 200))
        .Select(a => new { a.Id, a.OrderId, a.Amount, Status = a.Status.ToString(), a.DeclineReason, a.CreatedAt, a.UpdatedAt })
        .ToListAsync(ct);
});

app.Run();

public partial class Program { }
