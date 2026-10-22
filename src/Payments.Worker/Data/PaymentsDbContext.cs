using KafkaEdaStudy.BuildingBlocks.Persistence;
using KafkaEdaStudy.Payments.Domain;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Payments.Data;

public sealed class PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : DbContext(options)
{
    public DbSet<Authorization> Authorizations => Set<Authorization>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Authorization>(a =>
        {
            a.ToTable("authorizations");
            a.HasKey(x => x.Id);
            a.Property(x => x.Id).HasColumnName("id");
            a.Property(x => x.OrderId).HasColumnName("order_id");
            a.HasIndex(x => x.OrderId).IsUnique(); // rule P4 guard
            a.Property(x => x.Amount).HasColumnName("amount").HasPrecision(18, 2);
            a.Property(x => x.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(20);
            a.Property(x => x.DeclineReason).HasColumnName("decline_reason").HasMaxLength(100);
            a.Property(x => x.CreatedAt).HasColumnName("created_at");
            a.Property(x => x.UpdatedAt).HasColumnName("updated_at");
        });

        modelBuilder.AddMessagingTables();
    }
}
