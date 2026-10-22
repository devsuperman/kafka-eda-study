namespace KafkaEdaStudy.BuildingBlocks.Kafka;

public sealed class KafkaOptions
{
    public const string Section = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
}
