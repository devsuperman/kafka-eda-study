using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace KafkaEdaStudy.BuildingBlocks.Persistence;

public static class MessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IOutbox"/> (scoped, writes into <typeparamref name="TDbContext"/>) and the
    /// <see cref="OutboxRelay{TDbContext}"/> background service that publishes the rows.
    /// The DbContext must call <see cref="MessagingTables.AddOutboxTable"/> in OnModelCreating.
    /// </summary>
    public static IServiceCollection AddOutbox<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddScoped<IOutbox, Outbox<TDbContext>>();
        services.AddHostedService<OutboxRelay<TDbContext>>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="IInbox"/> (scoped, writes into <typeparamref name="TDbContext"/>).
    /// The DbContext must call <see cref="MessagingTables.AddInboxTable"/> in OnModelCreating.
    /// </summary>
    public static IServiceCollection AddInbox<TDbContext>(this IServiceCollection services)
        where TDbContext : DbContext
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddScoped<IInbox, Inbox<TDbContext>>();
        return services;
    }
}
