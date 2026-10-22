using KafkaEdaStudy.BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Notifications.Data;

/// <summary>A message "sent" to the customer (rule N1). Logged, and stored so the Playground can show it.</summary>
public sealed class Notification
{
    public Guid Id { get; set; }
    public Guid OrderId { get; set; }
    /// <summary>The event that caused this notification. Unique: rule N2, never notify twice for one event.</summary>
    public Guid EventId { get; set; }
    public required string Kind { get; set; }
    public required string Message { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

public sealed class NotificationsDbContext(DbContextOptions<NotificationsDbContext> options) : DbContext(options)
{
    public DbSet<Notification> Notifications => Set<Notification>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Notification>(n =>
        {
            n.ToTable("notifications");
            n.HasKey(x => x.Id);
            n.Property(x => x.Id).HasColumnName("id");
            n.Property(x => x.OrderId).HasColumnName("order_id");
            n.Property(x => x.EventId).HasColumnName("event_id");
            n.Property(x => x.Kind).HasColumnName("kind").HasMaxLength(50);
            n.Property(x => x.Message).HasColumnName("message").HasMaxLength(1000);
            n.Property(x => x.CreatedAt).HasColumnName("created_at");
            n.HasIndex(x => x.OrderId);
            n.HasIndex(x => x.EventId).IsUnique(); // second guard behind the inbox
            n.HasIndex(x => x.CreatedAt);
        });

        // Notifications publishes no business events, so it only needs the inbox table.
        modelBuilder.AddInboxTable();
    }
}
