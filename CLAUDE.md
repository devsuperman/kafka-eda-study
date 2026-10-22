# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Start here

Read `HANDOFF.md` first: it records what is done, which tags exist, and the next steps. Then `docs/03-tasks.md` (the roadmap) and `docs/01-business-rules.md` (rule IDs such as O4, I2, P3 are referenced throughout the code and tests). `docs/02-tech-stack.md` has the architecture and Kafka design.

## Commands

```bash
# Full stack (services + infra + Playground at :3000)
docker compose up -d --build

# Infra only, then run one service from the IDE or CLI
docker compose up -d kafka kafka-init kafka-ui postgres aspire-dashboard
dotnet run --project src/Orders.Api

# Build and test (.NET)
dotnet build
dotnet test                                              # all test projects
dotnet test --project tests/Orders.UnitTests             # one project
dotnet test --project tests/Orders.UnitTests -- --filter-class "*OrderStatusTests*"
dotnet test --project tests/IntegrationTests -- --filter-class "*PartitioningTests*"

# Playground frontend (run inside src/Playground.Web)
npm install
npm run dev        # Vite dev server
npm run build      # tsc -b && vite build
npm run lint       # oxlint
npm run mock       # node scripts/mock-api.mjs
```

- The test runner is Microsoft.Testing.Platform (set in `global.json`), so use `--project` and `-- --filter-*` arguments, not `--filter`.
- Integration tests start their own Kafka and PostgreSQL containers through Testcontainers, so Docker must be running. They do not need `docker compose up`.
- The .NET SDK is pinned to 10.0.x with `rollForward: latestFeature`.
- Adding a new EF migration: `dotnet tool restore`, then `dotnet tool run dotnet-ef migrations add <Name> --project src/<Service>`. Migrations run automatically at startup (`MigrateAsync` in each `Program.cs`), so there is no manual `database update` step.
- Step 7-8 plan: compose timeouts should drop to 30 seconds so timeout flows are visible in the Playground. Not set yet.

## Build rules that will break a build if ignored

- `Directory.Build.props` sets `TreatWarningsAsErrors=true` and `Nullable=enable`. A new warning fails the build.
- Package versions live only in `Directory.Packages.props` (central package management). Add a `PackageVersion` there and reference the package without a version in the `.csproj`.
- Auto-create of topics is disabled on the broker. A new topic must be added to both `infra/create-topics.sh` and `Contracts/Topics.cs` (`Topics.All`), or the publisher and consumers fail.
- The shared `Dockerfile` takes a `PROJECT` build arg. Every .NET service in `docker-compose.yml` passes its project name, and the image copies only `Directory.Build.props`, `Directory.Packages.props` and `src/`, so `tests/` is not available inside the image.

## Architecture: the big picture

**Kafka is the only path between business services.** No service calls another over HTTP for the order flow. Each context publishes to its own topic (`orders.events`, `inventory.events`, `payments.events`, `shipping.events`), with 3 partitions and a `.dlq` twin. The message key is always `orderId`, so every event of one order stays in one partition, in order.

**Every event goes through the same envelope.** `Contracts/EventEnvelope.cs` wraps each payload with `EventId`, `EventType`, `Version`, `OrderId`, `CorrelationId`, `CausationId` and the JSON `Payload`. Use `EventEnvelope.Create` to start a new flow and `EventEnvelope.CausedBy(incoming, payload)` for a reaction. `CausedBy` keeps the order and correlation IDs and sets `CausationId` to the triggering event, which is what the Playground swimlanes draw. The event records live in `Contracts/*Events.cs`, one file per context.

**Publishing is transactional (outbox).** A handler saves its state change and calls `IOutbox.Add(...)` in one EF transaction, then `SaveChanges`. The `OutboxRelay<TDbContext>` `BackgroundService` (in `BuildingBlocks/Persistence`) polls `outbox_messages` with `FOR UPDATE SKIP LOCKED`, publishes the rows and marks them sent. Delivery is at-least-once. Several replicas can run the relay safely, but strict per-key ordering holds only within a batch. Stopping the publisher does not lose events: rows wait in Postgres.

**Consuming is idempotent (inbox) and retried.** Consumers inherit `KafkaConsumerService` (`BuildingBlocks/Kafka`), which:
1. deserializes the envelope,
2. calls `HandleAsync` with 3 attempts (200 ms, then 1 s),
3. on a `PoisonMessageException` or a `JsonException`, or after the last attempt, copies the original message to `<topic>.dlq` with `error`/`original-*`/`consumer-group` headers and commits,
4. commits the offset only after success (auto-commit is off).

Inside the handler, call `IInbox.TryMarkProcessedAsync(eventId, consumerName)` in the same transaction as the domain change. It returns `false` for a duplicate, and the handler must then return without side effects. The `processed_messages` table has a primary key on `(event_id, consumer)`, so a rollback also un-marks the event. `Inventory.Worker/Consumers/OrderEventsConsumer.cs` and `ReserveStockHandler.cs` are the reference implementation.

**The Playground observes; it does not participate.** `Playground.Api` runs `KafkaObserver`, which is deliberately not a `KafkaConsumerService`, because it must show poison messages that the base class would dead-letter. It uses a `playground*` consumer group that never commits, and it replays each topic from the beginning on startup. The events live in an in-memory ring buffer (`EventStore`) and reach the browser over SSE. Scenario buttons (`Playground.Api/Scenarios`) call the real Orders API over HTTP, except the fault-injection scenarios (duplicate event, poison message), which write raw messages to Kafka through `FaultInjectionProducer`. Business services never know the UI exists.

**Each service owns its database.** PostgreSQL 17 runs one database per service (`orders`, `inventory`, `notifications`), set through connection strings in `docker-compose.yml`. The initial schemas come from migrations in each service's `Data/Migrations`. Schema changes are EF migrations in the owning service, never shared.

**Shared infrastructure pieces:**
- `BuildingBlocks/Hosting/ServiceDefaults.cs`: every service calls `builder.AddServiceDefaults()` and `app.MapDefaultEndpoints()`. This is where OpenTelemetry (traces, logs and metrics via OTLP to the Aspire Dashboard) and the health endpoint are configured.
- `BuildingBlocks/Telemetry.cs`: the `ActivitySource` used for consumer spans. The `traceparent` header is copied from producer to consumer, so one trace follows a message across services.
- `Contracts/Topics.cs`: topic names and `Topics.Dlq(topic)`. `Contracts/Products.cs` is the seed product catalogue that the O1 validation checks against.

## Domain code conventions

- Domain rules live on the aggregate, not in endpoints. `Orders.Api/Domain/Order.cs` has `Order.Place` (rule O1, returns errors instead of throwing) and the status machine (O3/O6). Unit tests in `tests/Orders.UnitTests` cover these without Kafka or a database.
- The stock seed data is intentional: `CB-006` has zero stock (the out-of-stock scenario), and `LT-005` costs 1500, above the 1000 payment limit.
- Commit one step at a time, and tag the commit `step-NN-<name>` after its "Done when" check in `docs/03-tasks.md` passes. Check the existing tags (`git tag`) before you start a step.

## Integration tests

`tests/IntegrationTests` shares one Kafka and one PostgreSQL container for the whole run through `InfraFixture` (registered with `[assembly: AssemblyFixture]`). Each test gets its own fresh database through `MessagingTestSupport.CreateServicesAsync`, and relays are started explicitly rather than as hosted services. The topics are created by the fixture, the same way `infra/create-topics.sh` does for compose.
