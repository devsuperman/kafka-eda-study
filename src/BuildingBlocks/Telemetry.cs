using System.Diagnostics;

namespace KafkaEdaStudy.BuildingBlocks;

public static class Telemetry
{
    public const string SourceName = "KafkaEdaStudy.Messaging";

    public static readonly ActivitySource Source = new(SourceName);
}
