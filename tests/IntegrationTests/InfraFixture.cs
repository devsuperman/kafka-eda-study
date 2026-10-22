using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaEdaStudy.Contracts;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(KafkaEdaStudy.IntegrationTests.InfraFixture))]

namespace KafkaEdaStudy.IntegrationTests;

/// <summary>One Kafka and one PostgreSQL container shared by all tests, with the study topics created.</summary>
public sealed class InfraFixture : IAsyncLifetime
{
    public KafkaContainer Kafka { get; } = new KafkaBuilder("apache/kafka:4.1.0").Build();
    public PostgreSqlContainer Postgres { get; } = new PostgreSqlBuilder("postgres:17-alpine").Build();

    public string BootstrapServers => Kafka.GetBootstrapAddress();

    public async ValueTask InitializeAsync()
    {
        await Task.WhenAll(Kafka.StartAsync(), Postgres.StartAsync());

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        var specs = Topics.All
            .SelectMany(t => new[] { t, Topics.Dlq(t) })
            .Select(t => new TopicSpecification { Name = t, NumPartitions = 3, ReplicationFactor = 1 });
        await admin.CreateTopicsAsync(specs);
    }

    public async ValueTask DisposeAsync()
    {
        await Kafka.DisposeAsync();
        await Postgres.DisposeAsync();
    }
}
