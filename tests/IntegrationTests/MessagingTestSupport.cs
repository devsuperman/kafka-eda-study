using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace KafkaEdaStudy.IntegrationTests;

/// <summary>A tiny service state for the tests: named counters.</summary>
public sealed class Counter
{
    public required string Name { get; set; }
    public int Value { get; set; }
}

/// <summary>A minimal "service database": some state plus the outbox and inbox tables from BuildingBlocks.</summary>
public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public DbSet<Counter> Counters => Set<Counter>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Counter>(c =>
        {
            c.ToTable("counters");
            c.HasKey(x => x.Name);
        });
        modelBuilder.AddMessagingTables();
    }
}

public static class MessagingTestSupport
{
    /// <summary>
    /// Services wired like a real service (DbContext on a fresh database, Kafka publisher, outbox, inbox),
    /// without the hosted relay running: tests start relays explicitly.
    /// </summary>
    public static async Task<ServiceProvider> CreateServicesAsync(InfraFixture infra, CancellationToken ct)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(infra.Postgres.GetConnectionString())
        {
            Database = $"test_{Guid.NewGuid():N}",
        }.ConnectionString;

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(TimeProvider.System);
        services.AddDbContext<TestDbContext>(o => o.UseNpgsql(connectionString));
        services.Configure<KafkaOptions>(o => o.BootstrapServers = infra.BootstrapServers);
        services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
        services.AddScoped<IOutbox, Outbox<TestDbContext>>();
        services.AddInbox<TestDbContext>();
        var provider = services.BuildServiceProvider();

        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<TestDbContext>().Database.EnsureCreatedAsync(ct);
        return provider;
    }

    public static async Task CreateTopicsAsync(InfraFixture infra, params string[] topics)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = infra.BootstrapServers }).Build();
        await admin.CreateTopicsAsync(
            topics.Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 }));
    }

    /// <summary>
    /// Reads <paramref name="topic"/> from the beginning and returns the messages matching <paramref name="match"/>.
    /// Once <paramref name="expected"/> matches are in, it keeps reading a little longer so duplicates would show up.
    /// </summary>
    public static List<ConsumeResult<string, string>> ReadTopic(
        InfraFixture infra, string topic, Func<ConsumeResult<string, string>, bool> match, int expected,
        TimeSpan? timeout = null)
    {
        using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = infra.BootstrapServers,
            GroupId = $"reader-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();
        consumer.Subscribe(topic);

        var found = new List<ConsumeResult<string, string>>();
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        DateTime? graceEnd = null;
        while (DateTime.UtcNow < (graceEnd ?? deadline))
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is not null && match(result))
                found.Add(result);
            if (found.Count >= expected && graceEnd is null)
                graceEnd = DateTime.UtcNow.AddSeconds(2);
        }
        consumer.Close();
        return found;
    }

    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition not met in time.");
            await Task.Delay(100, ct);
        }
    }
}
