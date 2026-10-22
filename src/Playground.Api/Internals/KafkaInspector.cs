using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaEdaStudy.Playground.Observer;
using Microsoft.Extensions.Options;

namespace KafkaEdaStudy.Playground.Internals;

/// <summary>
/// Read-only view of the broker's metadata for the "Kafka internals" page: partitions and their
/// offset ranges per topic, and per consumer group its members, their assignments and the lag.
/// </summary>
/// <remarks>
/// Everything comes from the Admin API (one singleton <see cref="IAdminClient"/>): no consumer joins
/// any group, so looking at a group never triggers a rebalance in it. Results are cached for
/// <see cref="CacheFor"/> and concurrent callers share the same in-flight request, so a few browsers
/// polling every couple of seconds cost the broker one round of requests per second at most.
/// </remarks>
public sealed class KafkaInspector(IAdminClient admin, TimeProvider time, IOptions<PlaygroundOptions> playground)
{
    public static readonly TimeSpan CacheFor = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(5);

    private readonly Lock _gate = new();
    private Cached<IReadOnlyList<TopicInfo>>? _topics;
    private Cached<IReadOnlyList<ConsumerGroupInfo>>? _groups;

    public Task<IReadOnlyList<TopicInfo>> GetTopicsAsync() => GetCached(ref _topics, LoadTopicsAsync);

    public Task<IReadOnlyList<ConsumerGroupInfo>> GetConsumerGroupsAsync() => GetCached(ref _groups, LoadConsumerGroupsAsync);

    private Task<T> GetCached<T>(ref Cached<T>? slot, Func<Task<T>> load)
    {
        lock (_gate)
        {
            var now = time.GetTimestamp();
            // Failures are cached too (for the same short time): a broker outage then costs one
            // request per second instead of one per browser poll.
            if (slot is null || (slot.Task.IsCompleted && time.GetElapsedTime(slot.StartedAt, now) >= CacheFor))
                slot = new Cached<T>(load(), now);
            return slot.Task;
        }
    }

    private sealed record Cached<T>(Task<T> Task, long StartedAt);

    // ---- Topics -------------------------------------------------------------------------------

    private async Task<IReadOnlyList<TopicInfo>> LoadTopicsAsync()
    {
        var partitionsByTopic = await DescribePartitionsAsync(KafkaObserver.ObservedTopics);
        var partitions = partitionsByTopic
            .SelectMany(kv => kv.Value.Select(p => new TopicPartition(kv.Key, p)))
            .ToList();

        var lowTask = ListOffsetsAsync(partitions, OffsetSpec.Earliest());
        var highTask = ListOffsetsAsync(partitions, OffsetSpec.Latest());
        var (low, high) = (await lowTask, await highTask);

        return
        [
            .. KafkaObserver.ObservedTopics.Select(topic =>
            {
                if (!partitionsByTopic.TryGetValue(topic, out var ids))
                    return new TopicInfo(topic, IsDlq(topic), Exists: false, []);

                return new TopicInfo(topic, IsDlq(topic), Exists: true,
                [
                    .. ids.Select(p =>
                    {
                        var tp = new TopicPartition(topic, p);
                        var lo = low.GetValueOrDefault(tp);
                        var hi = high.GetValueOrDefault(tp);
                        return new PartitionInfo(p, lo, hi, Math.Max(0, hi - lo));
                    }),
                ]);
            }),
        ];
    }

    /// <summary>Partition ids per existing topic; topics that do not exist (yet) are simply absent.</summary>
    private async Task<Dictionary<string, int[]>> DescribePartitionsAsync(IEnumerable<string> topics)
    {
        IEnumerable<TopicDescription> descriptions;
        try
        {
            var result = await admin.DescribeTopicsAsync(
                TopicCollection.OfTopicNames(topics),
                new DescribeTopicsOptions { RequestTimeout = RequestTimeout });
            descriptions = result.TopicDescriptions;
        }
        catch (DescribeTopicsException ex)
        {
            // Partial failure, typically UnknownTopicOrPart for a DLQ that was never created.
            descriptions = ex.Results.TopicDescriptions.Where(d => d.Error is not { IsError: true });
        }

        return descriptions.ToDictionary(
            d => d.Name,
            d => d.Partitions.Select(p => p.Partition).Order().ToArray());
    }

    private async Task<Dictionary<TopicPartition, long>> ListOffsetsAsync(
        IReadOnlyCollection<TopicPartition> partitions, OffsetSpec spec)
    {
        if (partitions.Count == 0)
            return [];

        IEnumerable<ListOffsetsResultInfo> infos;
        try
        {
            var result = await admin.ListOffsetsAsync(
                partitions.Select(tp => new TopicPartitionOffsetSpec { TopicPartition = tp, OffsetSpec = spec }),
                new ListOffsetsOptions { RequestTimeout = RequestTimeout });
            infos = result.ResultInfos;
        }
        catch (ListOffsetsException ex)
        {
            infos = ex.Result.ResultInfos.Where(i => i.TopicPartitionOffsetError.Error is not { IsError: true });
        }

        return infos.ToDictionary(
            i => i.TopicPartitionOffsetError.TopicPartition,
            i => i.TopicPartitionOffsetError.Offset.Value);
    }

    // ---- Consumer groups ----------------------------------------------------------------------

    private async Task<IReadOnlyList<ConsumerGroupInfo>> LoadConsumerGroupsAsync()
    {
        var listing = await admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions { RequestTimeout = RequestTimeout });
        var groupIds = listing.Valid.Select(g => g.GroupId).Order(StringComparer.Ordinal).ToList();
        if (groupIds.Count == 0)
            return [];

        var describeTask = DescribeGroupsAsync(groupIds);
        // The Admin API fetches committed offsets for exactly one group per call.
        var committedTasks = groupIds.ToDictionary(id => id, ListCommittedAsync);
        // Partition counts of the known topics, so a group shows every partition of the topics it
        // reads, including partitions it has not committed on yet.
        var knownTopicsTask = GetTopicsAsync();

        var descriptions = await describeTask;
        var knownTopics = (await knownTopicsTask).ToDictionary(t => t.Topic, t => t.Partitions.Select(p => p.Partition).ToArray());
        var committed = new Dictionary<string, Dictionary<TopicPartition, long>>();
        foreach (var (id, task) in committedTasks)
            committed[id] = await task;

        // Rows per group: every partition it committed on or has assigned, widened to all
        // partitions of those topics.
        var rowsByGroup = groupIds.ToDictionary(id => id, id =>
        {
            var referenced = committed[id].Keys
                .Concat(descriptions.GetValueOrDefault(id)?.Members.SelectMany(m => m.Assignment?.TopicPartitions ?? []) ?? [])
                .ToHashSet();
            foreach (var topic in referenced.Select(tp => tp.Topic).Distinct().ToList())
                foreach (var p in knownTopics.GetValueOrDefault(topic) ?? [])
                    referenced.Add(new TopicPartition(topic, p));
            return referenced;
        });

        // End offsets are read AFTER the committed offsets, so end >= committed and lag is never negative.
        var endOffsets = await ListOffsetsAsync(
            rowsByGroup.Values.SelectMany(r => r).Distinct().ToList(), OffsetSpec.Latest());

        return
        [
            .. groupIds
                .Select(id => BuildGroup(id, descriptions.GetValueOrDefault(id), committed[id], rowsByGroup[id], endOffsets,
                    KafkaObserver.IsObserverGroup(id, playground.Value.ObserverGroupId)))
                .OrderBy(g => g.IsObserver) // the observer last: it is not a business consumer
                .ThenBy(g => g.GroupId, StringComparer.Ordinal),
        ];
    }

    private static ConsumerGroupInfo BuildGroup(
        string groupId,
        ConsumerGroupDescription? description,
        Dictionary<TopicPartition, long> committed,
        HashSet<TopicPartition> rows,
        Dictionary<TopicPartition, long> endOffsets,
        bool isObserver)
    {
        var members = (description?.Members ?? [])
            .Select(m => new GroupMemberInfo(
                m.ConsumerId,
                m.ClientId,
                m.Host.TrimStart('/'),
                [
                    .. (m.Assignment?.TopicPartitions ?? [])
                        .OrderBy(tp => TopicRank(tp.Topic)).ThenBy(tp => tp.Topic).ThenBy(tp => tp.Partition.Value)
                        .Select(tp => new TopicPartitionRef(tp.Topic, tp.Partition.Value)),
                ]))
            .OrderBy(m => m.ClientId, StringComparer.Ordinal)
            .ThenBy(m => m.MemberId, StringComparer.Ordinal)
            .ToList();

        var partitions = rows
            .OrderBy(tp => TopicRank(tp.Topic)).ThenBy(tp => tp.Topic).ThenBy(tp => tp.Partition.Value)
            .Select(tp =>
            {
                var end = endOffsets.GetValueOrDefault(tp);
                long? done = committed.TryGetValue(tp, out var c) ? c : null;
                long? lag = done is { } d ? Math.Max(0, end - d) : null;
                return new GroupPartitionInfo(tp.Topic, tp.Partition.Value, done, end, lag);
            })
            .ToList();

        return new ConsumerGroupInfo(
            groupId,
            description?.State.ToString() ?? "Unknown",
            IsObserver: isObserver,
            members,
            partitions,
            TotalLag: partitions.Sum(p => p.Lag ?? 0));
    }

    private async Task<Dictionary<string, ConsumerGroupDescription>> DescribeGroupsAsync(IReadOnlyList<string> groupIds)
    {
        IEnumerable<ConsumerGroupDescription> descriptions;
        try
        {
            var result = await admin.DescribeConsumerGroupsAsync(
                groupIds, new DescribeConsumerGroupsOptions { RequestTimeout = RequestTimeout });
            descriptions = result.ConsumerGroupDescriptions;
        }
        catch (DescribeConsumerGroupsException ex)
        {
            descriptions = ex.Results.ConsumerGroupDescriptions.Where(d => d.Error is not { IsError: true });
        }

        return descriptions.ToDictionary(d => d.GroupId);
    }

    /// <summary>Committed offsets of one group; partitions without a commit are left out.</summary>
    private async Task<Dictionary<TopicPartition, long>> ListCommittedAsync(string groupId)
    {
        IEnumerable<TopicPartitionOffsetError> partitions;
        try
        {
            var results = await admin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(groupId, null)], // null = every partition it committed on
                new ListConsumerGroupOffsetsOptions { RequestTimeout = RequestTimeout });
            partitions = results.SelectMany(r => r.Partitions);
        }
        catch (ListConsumerGroupOffsetsException ex)
        {
            partitions = ex.Results.SelectMany(r => r.Partitions);
        }

        return partitions
            .Where(p => p.Error is not { IsError: true } && p.Offset.Value >= 0)
            .ToDictionary(p => p.TopicPartition, p => p.Offset.Value);
    }

    private static bool IsDlq(string topic) => topic.EndsWith(".dlq", StringComparison.Ordinal);

    private static int TopicRank(string topic)
    {
        var index = -1;
        for (var i = 0; i < KafkaObserver.ObservedTopics.Count; i++)
            if (KafkaObserver.ObservedTopics[i] == topic)
                index = i;
        return index < 0 ? int.MaxValue : index;
    }
}
