namespace KafkaEdaStudy.Contracts;

/// <summary>Fixed seed list of products (rule O2: there is no catalog service).</summary>
public static class Products
{
    public sealed record Product(string Sku, string Name, decimal Price, int InitialStock);

    public static readonly IReadOnlyList<Product> All =
    [
        new("KB-001", "Mechanical keyboard", 120m, 50),
        new("MS-002", "Wireless mouse", 40m, 100),
        new("MN-003", "27\" monitor", 350m, 20),
        new("HD-004", "Headphones", 90m, 30),
        new("LT-005", "Laptop", 1500m, 5),
        new("CB-006", "USB-C cable", 10m, 0),
    ];

    public static Product? Find(string sku) => All.FirstOrDefault(p => p.Sku == sku);
}
