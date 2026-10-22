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

## Remaining plan
- **Next = steps 7–8** (parallel by service; all events already exist in `src/Contracts`):
  Orders saga (O4 confirm when both `StockReserved` + `PaymentAuthorized`; cancel on failure; `POST /orders/{id}/cancel`; timeout scanner O5),
  Inventory (release on `OrderCancelled`, commit on `OrderShipped`, expiry I2, release late reservations),
  **Payments.Worker (new, also covers step 4's P2/P4)**: authorize on `OrderPlaced` (decline if total > 1000), void/capture/refund,
  **Shipping.Api (new, port 5002)**: shipment on `OrderConfirmed`, `POST /shipments/{orderId}/dispatch` and `/deliver`,
  Playground: remaining scenario buttons + order detail swimlane page; integration tests for flows 1–5. Compose: set timeouts to 30 s.
- **Step 9** orchestration saga (Orders.Orchestrator + config switch + ADR).
- **Steps 10, 11, 12** can run in parallel: CQRS read model + Service state page; `OrderPlaced` v2 with `couponCode`; tracing + "open trace" link.

## Conventions
- Every service: `builder.AddServiceDefaults()` + `app.MapDefaultEndpoints()`, EF Core migrations applied at startup (`dotnet tool run dotnet-ef`), publish through the outbox, consume through `KafkaConsumerService` + inbox.
- Events: `EventEnvelope.Create` for a new flow, `EventEnvelope.CausedBy(incoming, payload)` for reactions. Key = orderId.
- Tests: xunit v3 on Microsoft.Testing.Platform: `dotnet test --project <path>`.
- Commit per step, with tags `step-NN-name`.
- Seed products: `CB-006` has 0 stock (out-of-stock scenario), `LT-005` costs 1500 (over the 1000 payment limit).
