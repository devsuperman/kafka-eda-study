namespace KafkaEdaStudy.Playground.Internals;

/// <summary>One topic as the "Kafka internals" page sees it. <c>Exists</c> is false for a DLQ nobody created yet.</summary>
public sealed record TopicInfo(string Topic, bool IsDlq, bool Exists, IReadOnlyList<PartitionInfo> Partitions);

/// <summary>
/// Offsets of one partition. <c>LowOffset</c> is the first offset still in the log (retention may have
/// deleted older ones), <c>HighOffset</c> is the offset the next record will get, so
/// <c>Messages = HighOffset - LowOffset</c>.
/// </summary>
public sealed record PartitionInfo(int Partition, long LowOffset, long HighOffset, long Messages);

/// <summary>A consumer group: who is in it, what each member owns, and how far behind it is.</summary>
public sealed record ConsumerGroupInfo(
    string GroupId,
    string State,
    bool IsObserver,
    IReadOnlyList<GroupMemberInfo> Members,
    IReadOnlyList<GroupPartitionInfo> Partitions,
    long TotalLag);

public sealed record GroupMemberInfo(
    string MemberId,
    string ClientId,
    string Host,
    IReadOnlyList<TopicPartitionRef> Assignments);

public sealed record TopicPartitionRef(string Topic, int Partition);

/// <summary>
/// <c>CommittedOffset</c> is the next offset the group will read (null if it never committed);
/// <c>Lag = EndOffset - CommittedOffset</c> (null without a commit).
/// </summary>
public sealed record GroupPartitionInfo(
    string Topic,
    int Partition,
    long? CommittedOffset,
    long EndOffset,
    long? Lag);
