namespace KafkaEdaStudy.Playground;

/// <summary>Settings of the playground itself (section <c>Playground</c>).</summary>
public sealed class PlaygroundOptions
{
    public const string Section = "Playground";

    /// <summary>
    /// Consumer group of the read-only observer. Give a second local instance its own group
    /// (e.g. <c>Playground__ObserverGroupId=playground-dev</c>) so the two never share partitions:
    /// in one group each instance would see only half of the records.
    /// </summary>
    public string ObserverGroupId { get; set; } = "playground";
}
