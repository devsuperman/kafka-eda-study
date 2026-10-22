using KafkaEdaStudy.BuildingBlocks.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace KafkaEdaStudy.BuildingBlocks.Hosting;

public static class ServiceDefaults
{
    /// <summary>OpenTelemetry (exported via OTLP when OTEL_EXPORTER_OTLP_ENDPOINT is set), health checks and Kafka.</summary>
    public static IHostApplicationBuilder AddServiceDefaults(this IHostApplicationBuilder builder)
    {
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeFormattedMessage = true;
            logging.IncludeScopes = true;
        });

        var otel = builder.Services.AddOpenTelemetry()
            .WithMetrics(metrics => metrics
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation())
            .WithTracing(tracing => tracing
                .AddSource(Telemetry.SourceName)
                .AddAspNetCoreInstrumentation(o => o.Filter = ctx => ctx.Request.Path != "/health")
                .AddHttpClientInstrumentation());

        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
            otel.UseOtlpExporter();

        builder.Services.AddHealthChecks();
        builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.Section));
        builder.Services.AddSingleton<IEventPublisher, KafkaEventPublisher>();

        return builder;
    }

    public static WebApplication MapDefaultEndpoints(this WebApplication app)
    {
        app.MapHealthChecks("/health");
        return app;
    }
}
