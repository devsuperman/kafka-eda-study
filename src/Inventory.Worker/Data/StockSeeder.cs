using KafkaEdaStudy.Contracts;
using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.Inventory.Data;

public static class StockSeeder
{
    /// <summary>
    /// Seeds <c>stock_items</c> from <see cref="Products.All"/> when the table is empty.
    /// Several replicas may start at once, so each insert is "ON CONFLICT DO NOTHING" instead of failing.
    /// </summary>
    public static async Task SeedAsync(InventoryDbContext db, ILogger logger, CancellationToken ct = default)
    {
        if (await db.StockItems.AnyAsync(ct))
            return;

        var inserted = 0;
        foreach (var product in Products.All)
        {
            inserted += await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                INSERT INTO stock_items (sku, name, available, reserved)
                VALUES ({product.Sku}, {product.Name}, {product.InitialStock}, 0)
                ON CONFLICT (sku) DO NOTHING
                """, ct);
        }

        logger.LogInformation("Seeded {Count} stock items from the product list", inserted);
    }
}
