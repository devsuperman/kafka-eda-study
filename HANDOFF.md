# Handoff: where the build stopped

Last updated 2026-10-22. Read this first, then `docs/03-tasks.md` (the task list) and `docs/01-business-rules.md` (rule IDs like O4, I2, P3).

## Done and verified (tagged)
| Tag | What |
|---|---|
| `step-01-pubsub` | Orders.Api publishes `OrderPlaced`; Notifications logs it; Playground API (SSE) + React UI with live feed |
| `step-02-partitions` | Order status machine (O3/O6) + unit tests; integration test proving same key → same partition |
| `step-03-consumer-groups` | Inventory.Worker (all-or-nothing reservations, xmin concurrency retry), 3 replicas in compose; Playground "Kafka internals" page (partitions per order, consumer lag) |

## Steps 4–6: done and verified (tags step-04-idempotency, step-05-dead-letter, step-06-outbox)
- Step 4: resending an `OrderPlaced` with the same eventId leaves one inventory inbox row, one reservation and one notification.
- Step 5: poison messages (unknown SKU, invalid envelope) land in `<topic>.dlq` with `error`/`original-*`/`consumer-group` headers; unknown-SKU goes straight there (attempts:1); consumers keep processing.
- Step 6: an outbox row inserted while orders-api was stopped is published after restart.
- Known nits: the "malformed-json" scenario sends valid JSON with an invalid eventId (wording says non-JSON); Playground DLQ page was built but not clicked through in a browser.
- Consumer ClientId = "{group}@{machine}" landed.

## Step 7: choreography saga, done and verified (tag `step-07-choreography`)
- Orders: `OrderAnswersConsumer` + `OrderSagaHandler` (O4 confirm after both answers; cancel on `StockReservationFailed` / `PaymentDeclined`; `POST /orders/{id}/cancel`, O6). Optimistic concurrency via `xmin`.
- Inventory: `StockOutcomeHandler` releases on `OrderCancelled` (I4) and commits on `OrderShipped` (I4).
- Payments.Worker (new): authorize on `OrderPlaced` (P1, P2 limit 1,000, P4 once per order), void on cancel (P3), capture on ship. `GET /authorizations`.
- Shipping.Api (new, port 5002): shipment on `OrderConfirmed` (S1), `POST /shipments/{orderId}/dispatch` and `/deliver` (S2), `GET /shipments`.
- Playground: scenario buttons for flows 2, 3, 5 and dispatch/deliver; order detail swimlane at `/orders` and `/orders/:orderId` (causation arrows).
- Verified: 70 .NET tests green (unit tests for the new rules, plus `OrderFlowTests` running the real services for flows 1, 2, 3, 5). Also ran flows 1, 2, 3, 5 end to end on the live services (Kafka and Postgres in compose, services as local processes) and the swimlane renders in a headless browser.
- Not verified: `docker compose up --build` for the new service images. The sandbox's TLS proxy is not trusted inside image builds, so NuGet restore fails there; the Dockerfile is unchanged.
- Step 7 also needed `dotnet-tools.json` (EF tool, `dotnet tool restore`).

## Remaining plan
- **Next = step 8: timeouts**: Orders timeout scanner (O5), Inventory reservation expiry (I2), late `StockReserved` after cancel released immediately (flow 6), 30 s timeouts in compose, the `place-timeout-order` scenario, flow 4 integration test.
- **Step 9** orchestration saga (Orders.Orchestrator + config switch + ADR).
- **Steps 10, 11, 12** can run in parallel: CQRS read model + Service state page; `OrderPlaced` v2 with `couponCode`; tracing + "open trace" link.
- Known gaps from earlier steps: refund is not implemented (P3 says cancel after capture refunds; Orders cannot cancel after Shipped, so no path reaches it yet).

## Conventions
- Every service: `builder.AddServiceDefaults()` + `app.MapDefaultEndpoints()`, EF Core migrations applied at startup (`dotnet tool run dotnet-ef`), publish through the outbox, consume through `KafkaConsumerService` + inbox.
- Events: `EventEnvelope.Create` for a new flow, `EventEnvelope.CausedBy(incoming, payload)` for reactions. Key = orderId.
- Tests: xunit v3 on Microsoft.Testing.Platform: `dotnet test --project <path>`; the whole solution: `dotnet test --solution KafkaEdaStudy.slnx`.
- Integration flow tests reference the service projects with extern aliases (each has its own top-level `Program`); each service class ends with `public partial class Program { }`.
- Commit per step, with tags `step-NN-name`.
- Seed products: `CB-006` has 0 stock (out-of-stock scenario), `LT-005` costs 1500 (over the 1000 payment limit).
