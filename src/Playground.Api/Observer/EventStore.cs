using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace KafkaEdaStudy.Playground.Observer;

/// <summary>
/// Keeps the last <see cref="Capacity"/> observed events (ring buffer) and fans each new one out
/// to every connected SSE subscriber.
/// </summary>
public sealed class EventStore
{
    public const int Capacity = 1_000;
    private const int SubscriberBuffer = 500;

    private readonly Lock _gate = new();
    private readonly Queue<ObservedEvent> _buffer = new(Capacity);
    private readonly List<Channel<ObservedEvent>> _subscribers = [];

    public void Add(ObservedEvent observed)
    {
        lock (_gate)
        {
            if (_buffer.Count == Capacity)
                _buffer.Dequeue();
            _buffer.Enqueue(observed);

            // Each subscriber has its own bounded channel with DropOldest: TryWrite never blocks,
            // so a slow browser loses old events instead of stalling the Kafka consumer.
            foreach (var subscriber in _subscribers)
                subscriber.Writer.TryWrite(observed);
        }
    }

    /// <summary>
    /// Oldest first (by Kafka timestamp), optionally filtered by order and/or to dead-letter topics,
    /// returning the newest <paramref name="limit"/> matches.
    /// </summary>
    public IReadOnlyList<ObservedEvent> Snapshot(Guid? orderId, int limit, bool dlqOnly = false)
    {
        lock (_gate)
        {
            // Arrival order interleaves partitions arbitrarily; the Kafka timestamp gives a readable timeline.
            var matches = _buffer
                .Where(e => orderId is not { } id || e.OrderId == id)
                .Where(e => !dlqOnly || e.IsDlq)
                .OrderBy(e => e.Timestamp)
                .ToList();
            return matches.Count > limit ? matches[^limit..] : matches;
        }
    }

    /// <summary>The newest record (by Kafka timestamp) matching <paramref name="predicate"/>, if any is still buffered.</summary>
    public ObservedEvent? FindLatest(Func<ObservedEvent, bool> predicate)
    {
        lock (_gate)
            return _buffer.Where(predicate).MaxBy(e => e.Timestamp);
    }

    public async IAsyncEnumerable<ObservedEvent> SubscribeAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<ObservedEvent>(new BoundedChannelOptions(SubscriberBuffer)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        lock (_gate)
            _subscribers.Add(channel);

        try
        {
            await foreach (var observed in channel.Reader.ReadAllAsync(cancellationToken))
                yield return observed;
        }
        finally
        {
            lock (_gate)
                _subscribers.Remove(channel);
        }
    }
}
