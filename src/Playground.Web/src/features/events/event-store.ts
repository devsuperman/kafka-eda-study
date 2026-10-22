import {
  api,
  EVENT_STREAM_EVENT_NAME,
  EVENT_STREAM_URL,
  type ObservedEvent,
} from "@/lib/api"

/** Browser-side cap: the feed never holds more than this many records. */
export const MAX_EVENTS = 500
/** How many records to backfill from GET /api/events on (re)connect. */
const BACKFILL_LIMIT = 200

export type ConnectionStatus = "connecting" | "open" | "reconnecting"

export interface FeedItem {
  event: ObservedEvent
  /** True when the record arrived over SSE (gets a highlight animation). */
  live: boolean
}

export interface EventStoreSnapshot {
  /** Newest first, at most MAX_EVENTS. */
  items: FeedItem[]
  status: ConnectionStatus
  /** Count of unique records ever added; lets "paused" views compute "N new". */
  received: number
  /** True once the first backfill has completed (or failed). */
  loaded: boolean
}

function byNewest(a: FeedItem, b: FeedItem): number {
  const ta = Date.parse(a.event.timestamp)
  const tb = Date.parse(b.event.timestamp)
  if (ta !== tb) return tb - ta
  return b.event.offset - a.event.offset
}

/**
 * Single shared source of observed Kafka records for the whole app:
 * backfills from GET /api/events and then appends from the SSE stream,
 * deduplicating by `id`. Framework-agnostic; React reads it through
 * useSyncExternalStore (see use-event-stream.ts).
 */
export class EventStore {
  private snapshot: EventStoreSnapshot = {
    items: [],
    status: "connecting",
    received: 0,
    loaded: false,
  }
  private readonly ids = new Set<string>()
  private readonly listeners = new Set<() => void>()
  private source: EventSource | null = null
  private retryTimer: ReturnType<typeof setTimeout> | null = null
  private retryDelay = 1000
  private backfillAbort: AbortController | null = null
  private running = false

  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  getSnapshot = () => this.snapshot

  start() {
    if (this.running) return
    this.running = true
    this.connect()
  }

  stop() {
    this.running = false
    this.source?.close()
    this.source = null
    this.backfillAbort?.abort()
    if (this.retryTimer) clearTimeout(this.retryTimer)
    this.retryTimer = null
  }

  private set(patch: Partial<EventStoreSnapshot>) {
    this.snapshot = { ...this.snapshot, ...patch }
    for (const l of this.listeners) l()
  }

  private connect() {
    if (!this.running) return
    const source = new EventSource(EVENT_STREAM_URL)
    this.source = source

    source.onopen = () => {
      this.retryDelay = 1000
      this.set({ status: "open" })
      // Backfill on every (re)connect: covers the initial load and anything
      // published while we were disconnected. Dedupe makes this idempotent.
      void this.backfill()
    }

    source.addEventListener(EVENT_STREAM_EVENT_NAME, (e) => {
      try {
        const event = JSON.parse((e as MessageEvent<string>).data) as ObservedEvent
        this.addLive(event)
      } catch (err) {
        console.warn("Ignoring malformed SSE payload", err)
      }
    })

    source.onerror = () => {
      if (!this.running) return
      if (source.readyState === EventSource.CLOSED) {
        // The browser gave up (e.g. proxy returned 502): retry ourselves.
        source.close()
        this.source = null
        this.set({ status: "reconnecting" })
        this.retryTimer = setTimeout(() => this.connect(), this.retryDelay)
        this.retryDelay = Math.min(this.retryDelay * 2, 10_000)
      } else {
        // readyState CONNECTING: the browser retries on its own.
        this.set({ status: "reconnecting" })
      }
    }

    // If the stream never opens (API down), still try a one-off backfill so
    // the feed shows history when only the SSE endpoint is unhappy.
    if (!this.snapshot.loaded) void this.backfill()
  }

  private async backfill() {
    this.backfillAbort?.abort()
    const abort = new AbortController()
    this.backfillAbort = abort
    try {
      const events = await api.getEvents({ limit: BACKFILL_LIMIT }, abort.signal)
      this.merge(events)
    } catch (err) {
      if (!abort.signal.aborted) console.warn("Backfill failed", err)
    } finally {
      if (!abort.signal.aborted && !this.snapshot.loaded) this.set({ loaded: true })
    }
  }

  /** Fast path for SSE: prepend one record. */
  private addLive(event: ObservedEvent) {
    if (this.ids.has(event.id)) return
    this.ids.add(event.id)
    const items = [{ event, live: true }, ...this.snapshot.items]
    this.set({ items: this.trim(items), received: this.snapshot.received + 1 })
  }

  /** Bulk path for backfills: union by id, then sort newest first. */
  private merge(events: ObservedEvent[]) {
    const fresh = events.filter((e) => !this.ids.has(e.id))
    if (fresh.length === 0) return
    for (const e of fresh) this.ids.add(e.id)
    const items = [
      ...this.snapshot.items,
      ...fresh.map((event) => ({ event, live: false })),
    ].sort(byNewest)
    this.set({ items: this.trim(items), received: this.snapshot.received + fresh.length })
  }

  private trim(items: FeedItem[]): FeedItem[] {
    // Ids of dropped records stay in `ids` on purpose, so a later backfill
    // does not resurrect them (and does not inflate `received`).
    return items.length <= MAX_EVENTS ? items : items.slice(0, MAX_EVENTS)
  }
}
