using Microsoft.EntityFrameworkCore;

namespace KafkaEdaStudy.BuildingBlocks.Persistence;

/// <summary>
/// One row per event waiting to be published (transactional outbox, step 6).
/// The row is inserted in the SAME transaction as the state change, so "order saved" and
/// "OrderPlaced will be published" are one atomic fact. <see cref="OutboxRelay{TDbContext}"/> publishes it later.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>Sequential id (identity column): the relay publishes in this order.</summary>
    public long Id { get; set; }
    public Guid EventId { get; set; }
    public required string EventType { get; set; }
    public required string Topic { get; set; }
    public required string Key { get; set; }
    /// <summary>The whole <c>EventEnvelope</c> serialized as JSON (a <c>jsonb</c> column, readable with psql).</summary>
    public required string Envelope { get; set; }
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    /// <summary>Null until the relay got the broker's ack.</summary>
    public DateTimeOffset? SentAt { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }
}

/// <summary>
/// One row per (event, consumer) already handled (inbox, step 4). The composite primary key is what makes
/// a redelivered event detectable: inserting the same pair twice is impossible.
/// </summary>
public sealed class ProcessedMessage
{
    public Guid EventId { get; set; }
    public required string Consumer { get; set; }
    public DateTimeOffset ProcessedAt { get; set; }
}

public static class MessagingTables
{
    public const string Outbox = "outbox_messages";
    public const string Inbox = "processed_messages";

    /// <summary>Adds both the outbox and the inbox tables to a service's model.</summary>
    public static ModelBuilder AddMessagingTables(this ModelBuilder modelBuilder) =>
        modelBuilder.AddOutboxTable().AddInboxTable();

    public static ModelBuilder AddOutboxTable(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>(m =>
        {
            m.ToTable(Outbox);
            m.HasKey(x => x.Id);
            m.Property(x => x.Id).HasColumnName("id").UseIdentityAlwaysColumn();
            m.Property(x => x.EventId).HasColumnName("event_id");
            m.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(200);
            m.Property(x => x.Topic).HasColumnName("topic").HasMaxLength(200);
            m.Property(x => x.Key).HasColumnName("key").HasMaxLength(200);
            m.Property(x => x.Envelope).HasColumnName("envelope").HasColumnType("jsonb");
            m.Property(x => x.OccurredAt).HasColumnName("occurred_at");
            m.Property(x => x.CreatedAt).HasColumnName("created_at");
            m.Property(x => x.SentAt).HasColumnName("sent_at");
            m.Property(x => x.Attempts).HasColumnName("attempts");
            m.Property(x => x.LastError).HasColumnName("last_error");
            // Partial index: the relay only ever looks for unsent rows, so the index stays tiny.
            m.HasIndex(x => x.Id).HasFilter("sent_at IS NULL").HasDatabaseName("ix_outbox_messages_unsent");
        });
        return modelBuilder;
    }

    public static ModelBuilder AddInboxTable(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedMessage>(m =>
        {
            m.ToTable(Inbox);
            m.HasKey(x => new { x.EventId, x.Consumer });
            m.Property(x => x.EventId).HasColumnName("event_id");
            m.Property(x => x.Consumer).HasColumnName("consumer").HasMaxLength(100);
            m.Property(x => x.ProcessedAt).HasColumnName("processed_at");
        });
        return modelBuilder;
    }
}
