using System.Net.ServerSentEvents;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using KafkaEdaStudy.BuildingBlocks.Hosting;
using KafkaEdaStudy.BuildingBlocks.Kafka;
using KafkaEdaStudy.Playground;
using KafkaEdaStudy.Playground.Internals;
using KafkaEdaStudy.Playground.Observer;
using KafkaEdaStudy.Playground.Scenarios;
using Microsoft.Extensions.Options;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.Services.AddOpenApi();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

builder.Services.Configure<PlaygroundOptions>(builder.Configuration.GetSection(PlaygroundOptions.Section));

// Observer: one consumer feeding an in-memory ring buffer + SSE fan-out.
builder.Services.AddSingleton<EventStore>();
builder.Services.AddHostedService<KafkaObserver>();

// Kafka internals: one Admin API client for the whole app (it holds broker connections and a
// background thread), wrapped by an inspector that caches results for a second.
builder.Services.AddSingleton<IAdminClient>(sp => new AdminClientBuilder(new AdminClientConfig
{
    BootstrapServers = sp.GetRequiredService<IOptions<KafkaOptions>>().Value.BootstrapServers,
    ClientId = "playground-admin",
}).Build());
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<KafkaInspector>();

// Scenarios: each IScenario registration becomes a button in the UI.
builder.Services.AddHttpClient<OrdersApiClient>(http =>
{
    http.BaseAddress = new Uri(builder.Configuration["Services:OrdersApi"] ?? "http://localhost:5001");
    http.Timeout = TimeSpan.FromSeconds(10);
});
builder.Services.AddTransient<IScenario, PlaceValidOrderScenario>();
// Steps 7: the order flows that cancel, and the shipment steps. Shipping.Api is called over HTTP like Orders.Api.
builder.Services.AddTransient<IScenario, PlaceOutOfStockOrderScenario>();
builder.Services.AddTransient<IScenario, PlaceOverLimitOrderScenario>();
builder.Services.AddTransient<IScenario, CancelOrderScenario>();
builder.Services.AddHttpClient<ShippingApiClient>(http =>
{
    http.BaseAddress = new Uri(builder.Configuration["Services:ShippingApi"] ?? "http://localhost:5002");
    http.Timeout = TimeSpan.FromSeconds(10);
});
builder.Services.AddTransient<IScenario, DispatchShipmentScenario>();
builder.Services.AddTransient<IScenario, DeliverShipmentScenario>();
// Fault injection (steps 4-5): a dev-only raw producer that writes duplicates and poison messages.
builder.Services.AddSingleton<FaultInjectionProducer>();
builder.Services.AddTransient<IScenario, ResendDuplicateEventScenario>();
builder.Services.AddTransient<IScenario, SendPoisonMessageScenario>();
builder.Services.AddTransient<ScenarioRegistry>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.UseCors(); // the UI normally goes through a dev proxy; this is just a convenience

app.MapOpenApi();
app.MapScalarApiReference();
app.MapDefaultEndpoints();

var events = app.MapGroup("/api/events").WithTags("Events");

// ?dlq=true returns only records of the dead-letter topics (the DLQ viewer).
events.MapGet("/", (EventStore store, Guid? orderId, int? limit, bool? dlq) =>
    TypedResults.Ok(store.Snapshot(orderId, Math.Clamp(limit ?? 200, 1, EventStore.Capacity), dlq ?? false)));

// Server-Sent Events: a long-lived HTTP response the browser reads with EventSource.
events.MapGet("/stream", (EventStore store, CancellationToken cancellationToken) =>
    TypedResults.ServerSentEvents(Stream(store, cancellationToken)));

var scenarios = app.MapGroup("/api/scenarios").WithTags("Scenarios");

scenarios.MapGet("/", (ScenarioRegistry registry) => TypedResults.Ok(registry.List()));

// The JSON body ({ orderId?, kind? }) is optional: a bodiless POST runs the scenario with defaults.
scenarios.MapPost("/{name}", async Task<IResult> (
    string name, ScenarioRegistry registry, CancellationToken cancellationToken, ScenarioInput? input = null) =>
{
    if (registry.Find(name) is not { } scenario)
        return TypedResults.NotFound(new ScenarioResult(name, null, null, $"Unknown scenario '{name}'."));

    if (!scenario.Available)
        return TypedResults.Json(
            new ScenarioResult(scenario.Name, null, null, $"Scenario '{scenario.Name}' is not implemented yet."),
            statusCode: StatusCodes.Status501NotImplemented);

    try
    {
        return TypedResults.Ok(await scenario.RunAsync(input ?? ScenarioInput.Empty, cancellationToken));
    }
    catch (ScenarioRejectedException ex)
    {
        return TypedResults.Json(new ScenarioResult(scenario.Name, null, null, ex.Message), statusCode: ex.StatusCode);
    }
    catch (DownstreamException ex)
    {
        return TypedResults.Json(
            new ScenarioResult(scenario.Name, null, null, ex.Message),
            statusCode: StatusCodes.Status502BadGateway);
    }
});

var kafka = app.MapGroup("/api/kafka").WithTags("Kafka internals");

// Partitions and offset ranges of the 4 event topics and their DLQs.
kafka.MapGet("/topics", (KafkaInspector inspector, CancellationToken cancellationToken) =>
    FromBroker(inspector.GetTopicsAsync(), cancellationToken));

// Every consumer group: state, members and their assignments, committed offsets and lag.
kafka.MapGet("/consumer-groups", (KafkaInspector inspector, CancellationToken cancellationToken) =>
    FromBroker(inspector.GetConsumerGroupsAsync(), cancellationToken));

app.Run();

// The inspector's task is shared between callers, so a disconnecting browser only stops waiting
// for it (WaitAsync) instead of cancelling it for everyone.
static async Task<IResult> FromBroker<T>(Task<T> query, CancellationToken cancellationToken)
{
    try
    {
        return TypedResults.Ok(await query.WaitAsync(cancellationToken));
    }
    catch (KafkaException ex)
    {
        return TypedResults.Problem(
            title: "Kafka is not reachable",
            detail: ex.Error.Reason,
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static async IAsyncEnumerable<SseItem<ObservedEvent>> Stream(
    EventStore store, [EnumeratorCancellation] CancellationToken cancellationToken)
{
    await foreach (var observed in store.SubscribeAsync(cancellationToken))
        yield return new SseItem<ObservedEvent>(observed, "kafka-event") { EventId = observed.Id };
}
