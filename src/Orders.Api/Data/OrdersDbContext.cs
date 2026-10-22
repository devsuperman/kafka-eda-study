using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Orders.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Orders.Data;

public sealed class OrdersDbContext(DbContextOptions<OrdersDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(order =>
        {
            order.ToTable("orders");
            order.HasKey(o => o.Id);
            order.Property(o => o.Status).HasConversion<string>().HasMaxLength(20);
            order.Property(o => o.CancelReason).HasMaxLength(100);
            order.Property(o => o.Total).HasPrecision(18, 2);
            order.HasIndex(o => o.CreatedAt);

            order.OwnsMany(o => o.Lines, line =>
            {
                line.ToTable("order_lines");
                line.WithOwner().HasForeignKey("OrderId");
                line.Property<int>("Id");
                line.HasKey("Id");
                line.Property(l => l.Sku).HasMaxLength(50);
                line.Property(l => l.UnitPrice).HasPrecision(18, 2);
                line.Ignore(l => l.Subtotal);
            });
            order.Navigation(o => o.Lines).HasField("_lines").UsePropertyAccessMode(PropertyAccessMode.Field);
        });

        // outbox_messages (step 6) and processed_messages (inbox, used once Orders consumes events in step 7).
        modelBuilder.AddMessagingTables();
    }
}
