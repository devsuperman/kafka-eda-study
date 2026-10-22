namespace KafkaEdaStudy.Inventory;

public sealed class InventoryOptions
{
    public const string Section = "Inventory";

    /// <summary>How long a reservation holds stock before it expires (rule I2, enforced in step 8).</summary>
    public TimeSpan ReservationTimeout { get; set; } = TimeSpan.FromMinutes(15);
}
