using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Inventory.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Inventory.Data;

public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options)
{
    public DbSet<StockItem> StockItems => Set<StockItem>();
    public DbSet<Reservation> Reservations => Set<Reservation>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StockItem>(item =>
        {
            item.ToTable("stock_items", t =>
            {
                // Rule I3 again, as a last line of defense in the database.
                t.HasCheckConstraint("ck_stock_items_available_non_negative", "available >= 0");
                t.HasCheckConstraint("ck_stock_items_reserved_non_negative", "reserved >= 0");
            });
            item.HasKey(s => s.Sku);
            item.Property(s => s.Sku).HasColumnName("sku").HasMaxLength(50);
            item.Property(s => s.Name).HasColumnName("name").HasMaxLength(200);
            item.Property(s => s.Available).HasColumnName("available");
            item.Property(s => s.Reserved).HasColumnName("reserved");
            // uint + IsRowVersion() is mapped by Npgsql to the xmin system column (no real column is created).
            item.Property(s => s.Version).IsRowVersion();
        });

        modelBuilder.Entity<Reservation>(reservation =>
        {
            reservation.ToTable("reservations");
            reservation.HasKey(r => r.Id);
            reservation.Property(r => r.Id).HasColumnName("id");
            reservation.Property(r => r.OrderId).HasColumnName("order_id");
            reservation.HasIndex(r => r.OrderId).IsUnique();
            reservation.Property(r => r.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(20);
            reservation.Property(r => r.CreatedAt).HasColumnName("created_at");
            reservation.Property(r => r.ExpiresAt).HasColumnName("expires_at");
            reservation.HasIndex(r => r.CreatedAt);

            reservation.OwnsMany(r => r.Lines, line =>
            {
                line.ToTable("reservation_lines");
                line.WithOwner().HasForeignKey("ReservationId");
                line.Property<Guid>("ReservationId").HasColumnName("reservation_id");
                line.Property<int>("Id").HasColumnName("id");
                line.HasKey("Id");
                line.Property(l => l.Sku).HasColumnName("sku").HasMaxLength(50);
                line.Property(l => l.Quantity).HasColumnName("quantity");
            });
            reservation.Navigation(r => r.Lines).HasField("_lines").UsePropertyAccessMode(PropertyAccessMode.Field);
        });

        // outbox_messages (step 6) and processed_messages (inbox, step 4).
        modelBuilder.AddMessagingTables();
    }
}
