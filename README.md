# Kafka EDA Study

A study repo for event-driven design with Kafka on .NET 10. A small e-commerce domain (Orders, Inventory, Payments, Shipping, Notifications) is built one concept at a time, from pub/sub through sagas, outbox, CQRS and observability. The EDA Playground UI lets you trigger scenarios and watch events flow between services live.

## Docs

- [Business rules](docs/01-business-rules.md): domain, rules, events catalog, key flows
- [Tech stack](docs/02-tech-stack.md): stack, architecture, Kafka design, Docker Compose
- [Tasks](docs/03-tasks.md): Phase 0 setup and the 12-step roadmap

## Run it

```bash
docker compose up -d --build
```

- Playground: http://localhost:3000
- Kafka UI: http://localhost:8080
- Aspire Dashboard: http://localhost:18888
- Orders API (Scalar): http://localhost:5001/scalar

## Run infra only

```bash
docker compose up -d kafka kafka-init kafka-ui postgres aspire-dashboard
dotnet run --project src/<Service>
```

## Tests

```bash
dotnet test
```

## Roadmap progress

- [x] Phase 0: Setup
- [x] Step 1: Pub/sub
- [x] Step 2: Keys, partitions, ordering
- [x] Step 3: Consumer groups and scaling
- [x] Step 4: Delivery semantics and idempotency
- [x] Step 5: Errors, retries, dead letter
- [x] Step 6: Transactional outbox
- [ ] Step 7: Choreography saga and compensation
- [ ] Step 8: Timeouts
- [ ] Step 9: Orchestration saga
- [ ] Step 10: CQRS read models
- [ ] Step 11: Schema evolution
- [ ] Step 12: Observability
