#!/usr/bin/env node
// Throwaway stand-in for Playground.Api so the UI can be developed without
// Kafka/.NET running. Implements the same contract on http://localhost:5003:
//   GET  /api/events?orderId=&limit=&dlq=   GET /api/events/stream (SSE)
//   GET  /api/scenarios                     POST /api/scenarios/{name}  (optional JSON body { orderId?, kind? })
//   GET  /api/kafka/topics                  GET /api/kafka/consumer-groups
// Usage: npm run mock   (then `npm run dev` in another terminal)
import { createServer } from "node:http"
import { randomUUID } from "node:crypto"

const PORT = Number(process.env.PORT ?? 5003)
const PARTITIONS = 3
const TOPICS = ["orders.events", "inventory.events", "payments.events", "shipping.events"]
const events = []
const offsets = new Map()
const clients = new Set()

/** Business consumer groups and the topics they read (mirrors docker-compose). */
const GROUPS = {
  inventory: ["orders.events"],
  payments: ["orders.events"],
  notifications: ["orders.events"],
  orders: ["inventory.events", "payments.events", "shipping.events"],
}
/** Committed offsets per group: `${group}|${topic}-${partition}` -> next offset to read. */
const committed = new Map()

const scenarios = [
  { name: "place-valid-order", title: "Place valid order", description: "Places an order of 1-3 in-stock products totalling at most 1000: stock is reserved, payment authorized and the order confirmed.", available: true },
  { name: "resend-duplicate-event", title: "Resend duplicate event", description: "Publishes an OrderPlaced again with the same eventId (as a retrying producer would): idempotent consumers ignore it.", available: true },
  { name: "send-poison-message", title: "Send poison message", description: "Publishes an OrderPlaced with an unknown SKU, or a value that is not JSON: consumers dead-letter it to orders.events.dlq and keep going.", available: true },
  { name: "place-out-of-stock-order", title: "Place out-of-stock order", description: "Orders a product with no stock: inventory rejects the reservation and the order is cancelled.", available: false },
  { name: "place-over-limit-order", title: "Place over-limit order", description: "Orders more than the payment limit: payment is declined and the stock reservation is released.", available: false },
  { name: "cancel-order", title: "Cancel order", description: "The customer cancels an order, triggering compensations (release stock, void payment).", available: false },
]

function hash(s) {
  let h = 0
  for (const c of s) h = (h * 31 + c.charCodeAt(0)) | 0
  return Math.abs(h)
}

/** Appends one record to the fake log and pushes it to SSE clients. */
function append(topic, key, raw, headers) {
  const partition = key ? hash(key) % PARTITIONS : 0
  const tp = `${topic}-${partition}`
  const offset = offsets.get(tp) ?? 0
  offsets.set(tp, offset + 1)
  let envelope = null
  try {
    const parsed = JSON.parse(raw)
    if (parsed && parsed.eventId && parsed.eventType) envelope = parsed
  } catch {
    // poison: stays unparseable
  }
  const event = {
    id: `${topic}-${partition}-${offset}`,
    topic, partition, offset, key,
    timestamp: new Date().toISOString(),
    headers,
    isDlq: topic.endsWith(".dlq"),
    eventType: envelope?.eventType ?? null,
    eventId: envelope?.eventId ?? null,
    orderId: envelope?.orderId ?? null,
    correlationId: envelope?.correlationId ?? null,
    causationId: envelope?.causationId ?? null,
    version: envelope?.version ?? null,
    occurredAt: envelope?.occurredAt ?? null,
    payload: envelope?.payload ?? null,
    raw,
  }
  events.push(event)
  if (events.length > 2000) events.shift()
  const frame = `event: kafka-event\ndata: ${JSON.stringify(event)}\n\n`
  for (const res of clients) res.write(frame)
  // Business consumers keep up instantly in the mock (lag 0), except poison they skip past.
  for (const [group, topics] of Object.entries(GROUPS))
    if (topics.includes(topic)) committed.set(`${group}|${tp}`, offset + 1)
  return event
}

function publish(topic, orderId, eventType, payload, { correlationId, causationId } = {}) {
  const envelope = {
    eventId: randomUUID(), eventType, version: 1, occurredAt: new Date().toISOString(),
    orderId, correlationId: correlationId ?? randomUUID(), causationId: causationId ?? null, payload,
  }
  const headers = { "event-type": eventType, "event-version": "1", "correlation-id": envelope.correlationId }
  if (causationId) headers["causation-id"] = causationId
  return append(topic, orderId, JSON.stringify(envelope), headers)
}

/** What the real consumer base does with a message it gives up on: same key/value/headers + error headers. */
function deadLetter(original, group, error) {
  return append(`${original.topic}.dlq`, original.key, original.raw, {
    ...original.headers,
    error,
    "original-topic": original.topic,
    "original-partition": String(original.partition),
    "original-offset": String(original.offset),
    "consumer-group": group,
    "failed-at": new Date().toISOString(),
  })
}

const PRODUCTS = [["KB-001", 120], ["MS-002", 40], ["MN-003", 350], ["HD-004", 90]]

function runValidOrder() {
  const orderId = randomUUID()
  const correlationId = randomUUID()
  const [sku, unitPrice] = PRODUCTS[hash(orderId) % PRODUCTS.length]
  const steps = [
    ["orders.events", "OrderPlaced", { customerId: randomUUID(), lines: [{ sku, quantity: 1, unitPrice }], total: unitPrice }],
    ["inventory.events", "StockReserved", { reservationId: randomUUID(), expiresAt: new Date(Date.now() + 30_000).toISOString() }],
    ["payments.events", "PaymentAuthorized", { authorizationId: randomUUID(), amount: unitPrice }],
  ]
  let previous = null
  steps.forEach(([topic, type, payload], i) => {
    setTimeout(() => {
      previous = publish(topic, orderId, type, payload, { correlationId, causationId: previous?.eventId ?? null })
    }, i * 700)
  })
  return { scenario: "place-valid-order", orderId, correlationId, message: `Order ${orderId} placed (1x ${sku}, total ${unitPrice}); status Pending.` }
}

function runResend(input) {
  const isPlaced = (e) => !e.isDlq && e.topic === "orders.events" && e.eventType === "OrderPlaced"
  if (input.orderId && !/^[0-9a-f-]{36}$/i.test(input.orderId))
    return [400, { scenario: "resend-duplicate-event", message: `'${input.orderId}' is not a valid orderId (GUID expected).` }]
  const original = [...events].reverse().find((e) =>
    isPlaced(e) && (input.orderId ? e.orderId === input.orderId : !e.payload?.lines?.some((l) => l.sku === "XX-999")))
  if (!original)
    return [404, { scenario: "resend-duplicate-event", message: input.orderId ? `Nothing to resend: no OrderPlaced for order ${input.orderId} in the observed history.` : "Nothing to resend: no OrderPlaced in the observed history yet. Place an order first." }]
  const copy = append(original.topic, original.key, original.raw, original.headers)
  return [200, {
    scenario: "resend-duplicate-event", orderId: original.orderId, correlationId: original.correlationId,
    message: `Same eventId ${original.eventId} re-sent: OrderPlaced of order ${original.orderId} (copied from ${original.topic} partition ${original.partition}@${original.offset} to partition ${copy.partition}@${copy.offset}). Consumers should ignore it: no second reservation, authorization or notification.`,
  }]
}

function runPoison(input) {
  const kind = input.kind ?? "unknown-sku"
  let poison, error
  if (kind === "unknown-sku") {
    poison = publish("orders.events", randomUUID(), "OrderPlaced", { customerId: randomUUID(), lines: [{ sku: "XX-999", quantity: 1, unitPrice: 10 }], total: 10 })
    error = "PoisonMessageException: Unknown SKU 'XX-999' (not in the product list)"
  } else if (kind === "malformed-json") {
    poison = append("orders.events", randomUUID(), '{"eventId": "not-a-guid", "eventType": "OrderPlaced", payload: <<this is not JSON>>', {})
    error = "JsonException: 'p' is an invalid start of a property name. Path: $ | LineNumber: 0 | BytePositionInLine: 53."
  } else {
    return [400, { scenario: "send-poison-message", message: `Unknown poison kind '${kind}'. Use 'unknown-sku' or 'malformed-json'.` }]
  }
  // Every group reading orders.events gives up on it (poison: no retries) and parks its own copy.
  // Only inventory checks SKUs, so an unknown SKU is dead-lettered by inventory alone.
  const groups = kind === "unknown-sku" ? ["inventory"] : groupsReading("orders.events")
  groups.forEach((group, i) => setTimeout(() => deadLetter(poison, group, error), 600 + i * 250))
  return [200, {
    scenario: "send-poison-message", orderId: poison.key, correlationId: poison.correlationId ?? undefined,
    message: kind === "unknown-sku"
      ? `OrderPlaced with unknown SKU XX-999 published to orders.events (partition ${poison.partition}@${poison.offset}). The JSON is fine but the data can never be processed, so consumers should send it straight to orders.events.dlq and carry on.`
      : `Malformed (non-JSON) value published to orders.events with key ${poison.key} (partition ${poison.partition}@${poison.offset}). No consumer can deserialize it, so each one should park it in orders.events.dlq immediately and move on.`,
  }]
}

function groupsReading(topic) {
  return Object.entries(GROUPS).filter(([, topics]) => topics.includes(topic)).map(([g]) => g)
}

function topicsResponse() {
  return [...TOPICS, ...TOPICS.map((t) => `${t}.dlq`)].map((topic) => {
    const isDlq = topic.endsWith(".dlq")
    const exists = !isDlq || [...offsets.keys()].some((k) => k.startsWith(`${topic}-`))
    return {
      topic, isDlq, exists,
      partitions: exists
        ? Array.from({ length: PARTITIONS }, (_, partition) => {
            const high = offsets.get(`${topic}-${partition}`) ?? 0
            return { partition, lowOffset: 0, highOffset: high, messages: high }
          })
        : [],
    }
  })
}

function groupsResponse() {
  const business = Object.entries(GROUPS).map(([groupId, topics]) => {
    const partitions = topics.flatMap((topic) =>
      Array.from({ length: PARTITIONS }, (_, partition) => {
        const endOffset = offsets.get(`${topic}-${partition}`) ?? 0
        const c = committed.get(`${groupId}|${topic}-${partition}`) ?? null
        return { topic, partition, committedOffset: c, endOffset, lag: c === null ? null : Math.max(0, endOffset - c) }
      }))
    return {
      groupId, state: "Stable", isObserver: false,
      members: [{
        memberId: `${groupId}-worker-${randomUUID().slice(0, 8)}`, clientId: `${groupId}-worker`, host: "/172.18.0.5",
        assignments: partitions.map(({ topic, partition }) => ({ topic, partition })),
      }],
      partitions, totalLag: partitions.reduce((s, p) => s + (p.lag ?? 0), 0),
    }
  })
  const observerTopics = [...TOPICS, ...TOPICS.map((t) => `${t}.dlq`)]
  const observer = {
    groupId: "playground", state: "Stable", isObserver: true,
    members: [{
      memberId: "playground-api-1", clientId: "rdkafka", host: "/172.18.0.9",
      assignments: observerTopics.flatMap((topic) => Array.from({ length: PARTITIONS }, (_, partition) => ({ topic, partition }))),
    }],
    partitions: [], totalLag: 0,
  }
  return [...business, observer]
}

// Background noise: a valid order every 4 seconds so the stream is never idle.
setInterval(runValidOrder, 4000)
for (let i = 0; i < 5; i++) runValidOrder()
// Seed one of each fault so every page has something to show right away.
setTimeout(() => {
  runResend({})
  runPoison({ kind: "unknown-sku" })
  runPoison({ kind: "malformed-json" })
}, 2500)

function json(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json" })
  res.end(JSON.stringify(body))
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = ""
    req.on("data", (chunk) => (data += chunk))
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {})
      } catch {
        resolve(null)
      }
    })
  })
}

createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`)
  if (req.method === "GET" && url.pathname === "/api/events") {
    const orderId = url.searchParams.get("orderId")
    const dlq = url.searchParams.get("dlq") === "true"
    const limit = Number(url.searchParams.get("limit") ?? 200)
    const filtered = events.filter((e) => (!orderId || e.orderId === orderId) && (!dlq || e.isDlq))
    return json(res, 200, filtered.slice(-limit))
  }
  if (req.method === "GET" && url.pathname === "/api/events/stream") {
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" })
    res.write(": connected\n\n")
    clients.add(res)
    const ping = setInterval(() => res.write(": ping\n\n"), 15000)
    req.on("close", () => {
      clearInterval(ping)
      clients.delete(res)
    })
    return
  }
  if (req.method === "GET" && url.pathname === "/api/kafka/topics") return json(res, 200, topicsResponse())
  if (req.method === "GET" && url.pathname === "/api/kafka/consumer-groups") return json(res, 200, groupsResponse())
  if (req.method === "GET" && url.pathname === "/api/scenarios") return json(res, 200, scenarios)
  const m = url.pathname.match(/^\/api\/scenarios\/([^/]+)$/)
  if (req.method === "POST" && m) {
    const s = scenarios.find((x) => x.name === m[1])
    if (!s) return json(res, 404, { scenario: m[1], message: `Unknown scenario '${m[1]}'.` })
    if (!s.available) return json(res, 501, { scenario: s.name, message: `Scenario '${s.name}' is not implemented yet.` })
    const input = await readBody(req)
    if (input === null) return json(res, 400, { title: "Bad Request", status: 400, detail: "The request body is not valid JSON." })
    if (s.name === "place-valid-order") return json(res, 200, runValidOrder())
    const [status, body] = s.name === "resend-duplicate-event" ? runResend(input) : runPoison(input)
    return json(res, status, body)
  }
  json(res, 404, { message: "Not found" })
}).listen(PORT, () => console.log(`mock Playground.Api on http://localhost:${PORT}`))
