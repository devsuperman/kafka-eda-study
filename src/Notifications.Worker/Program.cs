using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Notifications;
using KafkaEdaStudy.Notifications.Data;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddDbContext<NotificationsDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Notifications")));
builder.Services.AddInbox<NotificationsDbContext>(); // step 4: never notify twice (N2)
builder.Services.AddHostedService<OrderEventsConsumer>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
    await scope.ServiceProvider.GetRequiredService<NotificationsDbContext>().Database.MigrateAsync();

app.MapDefaultEndpoints();

// Stored notifications, newest first (the Playground shows them).
app.MapGet("/notifications", async (NotificationsDbContext db, Guid? orderId, int? limit, CancellationToken ct) =>
{
    var query = db.Notifications.AsNoTracking();
    if (orderId is { } id)
        query = query.Where(n => n.OrderId == id);
    return await query
        .OrderByDescending(n => n.CreatedAt)
        .Take(Math.Clamp(limit ?? 50, 1, 200))
        .Select(n => new { n.Id, n.OrderId, n.EventId, n.Kind, n.Message, n.CreatedAt })
        .ToListAsync(ct);
});

app.Run();
