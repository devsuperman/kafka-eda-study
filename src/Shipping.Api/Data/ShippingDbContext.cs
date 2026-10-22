using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Shipping.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Shipping.Data;

public sealed class ShippingDbContext(DbContextOptions<ShippingDbContext> options) : DbContext(options)
{
    public DbSet<Shipment> Shipments => Set<Shipment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Shipment>(s =>
        {
            s.ToTable("shipments");
            s.HasKey(x => x.Id);
            s.Property(x => x.Id).HasColumnName("id");
            s.Property(x => x.OrderId).HasColumnName("order_id");
            s.HasIndex(x => x.OrderId).IsUnique(); // one shipment per order
            s.Property(x => x.CorrelationId).HasColumnName("correlation_id");
            s.Property(x => x.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(20);
            s.Property(x => x.TrackingCode).HasColumnName("tracking_code").HasMaxLength(50);
            s.Property(x => x.CreatedAt).HasColumnName("created_at");
            s.Property(x => x.DispatchedAt).HasColumnName("dispatched_at");
            s.Property(x => x.DeliveredAt).HasColumnName("delivered_at");
            s.Property(x => x.Version).IsRowVersion();
            s.HasIndex(x => x.Status);
        });

        modelBuilder.AddMessagingTables();
    }
}
