# Kafka EDA Study — Tasks

## How to use this list

Work top to bottom: Phase 0 once, then one roadmap step at a time. A step is done when its "Done when" check passes; then tag it in git (`step-NN-<name>`). Rule IDs (O1, I2, …) and flow numbers refer to the Business Rules doc.

## Phase 0: Setup

- [x] Create repo `kafka-eda-study`, `.gitignore`, `README.md`, `KafkaEdaStudy.slnx`
- [x] Add `Directory.Build.props` (net10.0, nullable, warnings as errors) and `Directory.Packages.props`
- [x] Copy the three docs into `docs/` (business rules, tech stack, tasks)
- [x] `docker-compose.yml` infra services: kafka (KRaft), kafka-init, kafka-ui, postgres, aspire-dashboard
- [x] `kafka-init` creates the 4 topics + 4 `.dlq` topics with 3 partitions; disable auto-create
- [x] `Contracts` project: `EventEnvelope` and event records for all contexts
- [x] `BuildingBlocks` project: JSON serializer, `KafkaProducer`, `KafkaConsumerService` base (manual commit), OpenTelemetry setup
- [x] Shared Dockerfile pattern (`sdk:10.0` → `aspnet:10.0`) and healthchecks
- [x] `IntegrationTests` project with a Testcontainers Kafka + PostgreSQL fixture

**Done when:** `docker compose up -d kafka kafka-init kafka-ui postgres aspire-dashboard` is healthy, Kafka UI at :8080 lists 8 topics, `dotnet build` passes.

## Roadmap steps

### Step 1: Pub/sub

- [x] Orders.Api: `POST /orders` validates O1, saves the order, publishes `OrderPlaced` (direct produce for now)
- [x] Notifications.Worker: consumes `OrderPlaced`, logs the notification
- [x] Playground.Api: consumes all topics, keeps the last 1,000 events, streams them via SSE
- [x] Playground.Web: Vite + React + shadcn/ui scaffold; live event feed + "Place valid order" button
- [x] Add app services to compose (orders-api, notifications-worker, playground-api, playground-web)

**Done when:** clicking "Place valid order" at :3000 shows `OrderPlaced` in the feed and a log line in Notifications.

### Step 2: Keys, partitions, ordering

- [ ] Key every message by `orderId`; add the O3 status machine to the Order entity with unit tests
- [ ] Playground: Kafka internals page showing the partition of each order

**Done when:** all events of one order land in the same partition, and an invalid status transition is rejected in tests.

### Step 3: Consumer groups and scaling

- [ ] Inventory.Worker: seed stock, consume `OrderPlaced`, reserve all-or-nothing (I1, I3), publish `StockReserved` / `StockReservationFailed`
- [ ] Run Inventory with `deploy.replicas: 3`; Playground shows consumer lag per partition

**Done when:** stopping one Inventory replica triggers a rebalance and no order is lost.

### Step 4: Delivery semantics and idempotency

- [x] Inbox table in BuildingBlocks; all consumers skip already-processed event IDs (N2)
- [x] Payments.Worker: authorize once per order (P2 decline rule, P4)
- [x] Playground: "Re-send duplicate event" button

**Done when:** a duplicate `OrderPlaced` produces no second authorization or notification (flow 6).

### Step 5: Errors, retries, dead letter

- [x] Consumer base: 3 retries with backoff, then publish to `<topic>.dlq` with error headers
- [x] Playground: "Send poison message" button + DLQ viewer

**Done when:** an `OrderPlaced` with an unknown SKU ends in `orders.events.dlq` and the consumer keeps running.

### Step 6: Transactional outbox

- [x] Outbox table + relay `BackgroundService` in BuildingBlocks; Orders, Inventory, Payments publish only through it

**Done when:** killing Orders.Api right after saving an order still delivers `OrderPlaced` after restart.

### Step 7: Choreography saga and compensation

- [x] Orders: confirm on both answers (O4), cancel on failure; publish `OrderConfirmed` / `OrderCancelled`
- [x] Inventory releases and Payments voids on `OrderCancelled` (I4, P3)
- [x] Shipping.Api: create shipment on `OrderConfirmed`; dispatch and deliver endpoints (S1, S2); Orders publishes `OrderShipped` / `OrderDelivered`; Payments captures, Inventory commits
- [x] Playground: remaining scenario buttons + order detail swimlane
- [x] Integration tests for flows 1–3 and 5

**Done when:** flows 1, 2, 3 and 5 match the Business Rules doc in the swimlane and in tests.

### Step 8: Timeouts

- [ ] Orders timeout scanner (O5) and Inventory reservation expiry (I2); 30 s in compose
- [ ] Late `StockReserved` after cancel is released immediately (flow 6)

**Done when:** flow 4 passes in an integration test and in the Playground.

### Step 9: Orchestration saga

- [ ] Orders.Orchestrator sends commands (`ReserveStock`, `AuthorizePayment`, …) and tracks saga state
- [ ] Config switch between choreography and orchestration; Playground shows the active mode

**Done when:** flows 1–5 pass in both modes; write a short ADR comparing them.

### Step 10: CQRS read models

- [ ] `order_summaries` projection ("My orders") and sales per product, built only from events
- [ ] Playground: service state page

**Done when:** rebuilding the projection from offset 0 gives the same result.

### Step 11: Schema evolution

- [ ] `OrderPlaced` v2 adds `couponCode`; consumers read v1 and v2 (tolerant reader)

**Done when:** old and new producers run side by side with no consumer errors.

### Step 12: Observability

- [ ] Propagate `traceparent`; spans per handler; consumer lag metrics in the Aspire Dashboard
- [ ] Playground: "Open trace" link on order detail

**Done when:** one order shows a single trace across all five services.

## Optional extra: Event sourcing

- [ ] Store Order as an event stream; rebuild state by replay; add snapshots

**Done when:** an order rebuilt from its events matches the stored read model.
