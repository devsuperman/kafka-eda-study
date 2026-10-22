// Typed client for Playground.Api. Everything goes through the relative `/api`
// prefix: the Vite dev server proxies it to http://localhost:5003 and nginx
// proxies it to the playground-api container in Docker.

/** One Kafka record as observed by Playground.Api's catch-all consumer. */
export interface ObservedEvent {
  /** `${topic}-${partition}-${offset}`, unique per record. */
  id: string
  topic: string
  partition: number
  offset: number
  key: string | null
  /** Kafka record timestamp, ISO 8601. */
  timestamp: string
  headers: Record<string, string>
  isDlq: boolean
  // Envelope fields, null when the value could not be parsed.
  eventType: string | null
  eventId: string | null
  orderId: string | null
  correlationId: string | null
  causationId: string | null
  version: number | null
  occurredAt: string | null
  payload: unknown
  /** The raw record value as a string. */
  raw: string
}

export interface Scenario {
  name: string
  title: string
  description: string
  available: boolean
}

/** Optional body of POST /api/scenarios/{name}; each scenario reads what it understands. */
export interface ScenarioInput {
  /** resend-duplicate-event: which order's OrderPlaced to re-send (default: the latest). */
  orderId?: string
  /** send-poison-message: "unknown-sku" (default) or "malformed-json". */
  kind?: PoisonKind
}

export type PoisonKind = "unknown-sku" | "malformed-json"

export interface ScenarioResult {
  scenario: string
  orderId?: string
  correlationId?: string
  message: string
}

/** GET /api/kafka/topics: offsets per partition of the 4 event topics and their DLQs. */
export interface KafkaTopic {
  topic: string
  isDlq: boolean
  /** False for a DLQ topic that has not been created (yet). */
  exists: boolean
  partitions: KafkaPartition[]
}

export interface KafkaPartition {
  partition: number
  /** First offset still in the log. */
  lowOffset: number
  /** Offset the next record will get (the "end offset"). */
  highOffset: number
  /** highOffset - lowOffset */
  messages: number
}

/** GET /api/kafka/consumer-groups */
export interface ConsumerGroup {
  groupId: string
  /** Stable, Empty, PreparingRebalance, CompletingRebalance, Dead, Unknown */
  state: string
  /** True for "playground": it reads everything but never commits offsets. */
  isObserver: boolean
  members: ConsumerGroupMember[]
  partitions: ConsumerGroupPartition[]
  totalLag: number
}

export interface ConsumerGroupMember {
  memberId: string
  clientId: string
  host: string
  assignments: { topic: string; partition: number }[]
}

export interface ConsumerGroupPartition {
  topic: string
  partition: number
  /** Next offset the group will read; null if it never committed on this partition. */
  committedOffset: number | null
  endOffset: number
  /** endOffset - committedOffset; null without a commit. */
  lag: number | null
}

export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = "ApiError"
    this.status = status
  }
}

/** Pulls a human message out of `{ message }` or RFC 7807 problem JSON. */
async function readError(res: Response): Promise<ApiError> {
  let message = `${res.status} ${res.statusText}`.trim()
  try {
    const text = await res.text()
    if (text) {
      try {
        const body = JSON.parse(text) as Record<string, unknown>
        const candidate = body.message ?? body.detail ?? body.title
        if (typeof candidate === "string" && candidate) message = candidate
      } catch {
        message = text.slice(0, 300)
      }
    }
  } catch {
    // keep the status line
  }
  return new ApiError(res.status, message)
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: { Accept: "application/json", ...init?.headers },
  })
  if (!res.ok) throw await readError(res)
  return (await res.json()) as T
}

export const api = {
  /** Recent events, oldest first. */
  getEvents(params: { orderId?: string; limit?: number; dlq?: boolean } = {}, signal?: AbortSignal) {
    const query = new URLSearchParams()
    if (params.orderId) query.set("orderId", params.orderId)
    if (params.dlq) query.set("dlq", "true")
    query.set("limit", String(params.limit ?? 200))
    return request<ObservedEvent[]>(`/api/events?${query}`, { signal })
  },

  getScenarios() {
    return request<Scenario[]>("/api/scenarios")
  },

  getKafkaTopics(signal?: AbortSignal) {
    return request<KafkaTopic[]>("/api/kafka/topics", { signal })
  },

  getConsumerGroups(signal?: AbortSignal) {
    return request<ConsumerGroup[]>("/api/kafka/consumer-groups", { signal })
  },

  /** The body is optional; without input the scenario runs with its defaults. */
  runScenario(name: string, input?: ScenarioInput) {
    return request<ScenarioResult>(`/api/scenarios/${encodeURIComponent(name)}`, {
      method: "POST",
      ...(input && {
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(input),
      }),
    })
  },
}

/** Server-Sent Events endpoint; each message has event name `kafka-event`. */
export const EVENT_STREAM_URL = "/api/events/stream"
export const EVENT_STREAM_EVENT_NAME = "kafka-event"
