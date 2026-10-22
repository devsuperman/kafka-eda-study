import { useMemo } from "react"
import { ChevronRightIcon, MousePointerClickIcon } from "lucide-react"
import { Link } from "react-router"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ContextBadge } from "@/features/events/context-badge"
import { MAX_EVENTS } from "@/features/events/event-store"
import { useEventStream } from "@/features/events/use-event-stream"
import type { KafkaPartition, KafkaTopic } from "@/lib/api"
import { CONTEXTS, contextOfTopic, type BoundedContext } from "@/lib/contexts"
import { shortId } from "@/lib/format"
import { cn } from "@/lib/utils"

import { KEY_CHIP_CLASS, keyHueStyle } from "./key-color"
import { OrderChip } from "./order-chip"
import { useKafkaTopics } from "./use-kafka-queries"

/** Max chips per partition before collapsing into "+N more". */
const MAX_CHIPS = 24

interface KeyEntry {
  key: string
  count: number
  firstOffset: number
}

/** topic -> partition -> keys in offset order (from the records the browser holds). */
type PartitionIndex = Map<string, Map<number, KeyEntry[]>>

interface HighlightProps {
  highlighted: string | null
  onHighlight: (key: string | null) => void
  onOpen: (key: string) => void
}

export function TopicsSection({ highlighted, onHighlight, onOpen }: HighlightProps) {
  const { data: topics, error, isPending, isFetching } = useKafkaTopics()
  const { items } = useEventStream()

  const index = useMemo<PartitionIndex>(() => {
    const byPartition = new Map<string, Map<number, Map<string, KeyEntry>>>()
    for (const { event } of items) {
      const key = event.key ?? event.orderId
      if (!key) continue
      let partitions = byPartition.get(event.topic)
      if (!partitions) byPartition.set(event.topic, (partitions = new Map()))
      let keys = partitions.get(event.partition)
      if (!keys) partitions.set(event.partition, (keys = new Map()))
      const entry = keys.get(key)
      if (entry) {
        entry.count++
        entry.firstOffset = Math.min(entry.firstOffset, event.offset)
      } else keys.set(key, { key, count: 1, firstOffset: event.offset })
    }
    const result: PartitionIndex = new Map()
    for (const [topic, partitions] of byPartition) {
      const sorted = new Map<number, KeyEntry[]>()
      for (const [p, keys] of partitions)
        sorted.set(p, [...keys.values()].sort((a, b) => a.firstOffset - b.firstOffset))
      result.set(topic, sorted)
    }
    return result
  }, [items])

  // Where did the highlighted order land? One partition per topic, always.
  const placements = useMemo(() => {
    if (!highlighted) return []
    const found: { topic: string; partition: number; count: number }[] = []
    for (const [topic, partitions] of index)
      for (const [partition, keys] of partitions) {
        const entry = keys.find((k) => k.key === highlighted)
        if (entry) found.push({ topic, partition, count: entry.count })
      }
    // Topic order as on the page: orders, inventory, payments, shipping, then the DLQs.
    const rank = (t: string) => (t.endsWith(".dlq") ? 10 : 0) + CONTEXTS.indexOf(contextOfTopic(t) ?? "orders")
    return found.sort((a, b) => rank(a.topic) - rank(b.topic))
  }, [highlighted, index])

  const main = topics?.filter((t) => !t.isDlq) ?? []
  const dlqs = topics?.filter((t) => t.isDlq) ?? []
  const dlqMessages = dlqs.reduce((sum, t) => sum + t.partitions.reduce((s, p) => s + p.messages, 0), 0)

  return (
    <Card className="min-w-0 gap-0 py-0">
      <CardHeader className="gap-1 border-b py-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <CardTitle>Topics &amp; partitions</CardTitle>
          <RefreshDot fetching={isFetching} />
        </div>
        <CardDescription>
          Every event is published with key = <code className="font-mono text-xs">orderId</code>. Kafka hashes the
          key to pick a partition, so all events of one order land in the same partition of a topic and are read
          back in the order they were written. Across partitions (and across topics) there is no ordering.
        </CardDescription>
        <HighlightBar highlighted={highlighted} placements={placements} />
      </CardHeader>

      <CardContent className="flex flex-col gap-4 py-4">
        {isPending ? (
          <p className="py-8 text-center text-sm text-muted-foreground">Reading topic metadata…</p>
        ) : !topics ? (
          <p className="py-8 text-center text-sm text-destructive">
            Could not load topics: {error?.message ?? "unknown error"}
          </p>
        ) : (
          <>
            {error && (
              <p className="text-xs text-destructive">Refresh failed ({error.message}); showing the last result.</p>
            )}
            <div className="grid gap-4 xl:grid-cols-2">
              {main.map((topic) => (
                <TopicCard
                  key={topic.topic}
                  topic={topic}
                  partitions={index.get(topic.topic)}
                  highlighted={highlighted}
                  onHighlight={onHighlight}
                  onOpen={onOpen}
                />
              ))}
            </div>

            <details className="group rounded-xl border border-dashed">
              <summary className="flex cursor-pointer list-none items-center gap-2 px-3 py-2 text-sm font-medium select-none [&::-webkit-details-marker]:hidden">
                <ChevronRightIcon className="size-4 text-muted-foreground transition-transform group-open:rotate-90" />
                Dead-letter topics
                <Badge variant={dlqMessages > 0 ? "destructive" : "outline"} className="ml-1 tabular-nums">
                  {dlqMessages} {dlqMessages === 1 ? "message" : "messages"}
                </Badge>
                <span className="hidden text-xs font-normal text-muted-foreground sm:inline">
                  where messages that can never be processed are set aside (step 5)
                </span>
              </summary>
              <p className="border-t px-3 pt-2 text-xs text-muted-foreground">
                Why each record failed (error, original offset, consumer group) is on the{" "}
                <Link to="/dead-letters" className="font-medium text-primary underline-offset-2 hover:underline">
                  Dead letters
                </Link>{" "}
                page.
              </p>
              <div className="grid gap-3 p-3 xl:grid-cols-2">
                {dlqs.map((topic) => (
                  <TopicCard
                    key={topic.topic}
                    topic={topic}
                    partitions={index.get(topic.topic)}
                    highlighted={highlighted}
                    onHighlight={onHighlight}
                    onOpen={onOpen}
                    compact
                  />
                ))}
              </div>
            </details>

            <p className="text-xs text-muted-foreground">
              Offsets and message counts come from the broker. The order chips come from the newest {MAX_EVENTS}{" "}
              records this browser holds, so older orders may be missing from them.
            </p>
          </>
        )}
      </CardContent>
    </Card>
  )
}

function HighlightBar({
  highlighted,
  placements,
}: {
  highlighted: string | null
  placements: { topic: string; partition: number; count: number }[]
}) {
  if (!highlighted)
    return (
      <p className="mt-1 flex items-center gap-1.5 text-xs text-muted-foreground">
        <MousePointerClickIcon className="size-3.5" aria-hidden />
        Hover an order to highlight it in every topic; click it to open its events in the live stream.
      </p>
    )
  return (
    <p className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs" aria-live="polite">
      <span
        style={keyHueStyle(highlighted)}
        className={cn("rounded border px-1 font-mono", KEY_CHIP_CLASS)}
      >
        {shortId(highlighted)}
      </span>
      {placements.length === 0 ? (
        <span className="text-muted-foreground">no records in the browser buffer</span>
      ) : (
        placements.map((p) => (
          <span key={p.topic} className="font-mono text-muted-foreground">
            {p.topic} → <span className="font-semibold text-foreground">p{p.partition}</span>
            <span className="opacity-70"> ({p.count})</span>
          </span>
        ))
      )}
    </p>
  )
}

function RefreshDot({ fetching }: { fetching: boolean }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
      <span
        className={cn("size-1.5 rounded-full bg-emerald-500 transition-opacity", fetching ? "opacity-100" : "opacity-40")}
        aria-hidden
      />
      refresh 2 s
    </span>
  )
}

interface TopicCardProps extends HighlightProps {
  topic: KafkaTopic
  partitions: Map<number, KeyEntry[]> | undefined
  compact?: boolean
}

function TopicCard({ topic, partitions, compact, highlighted, onHighlight, onOpen }: TopicCardProps) {
  const total = topic.partitions.reduce((sum, p) => sum + p.messages, 0)
  const max = Math.max(1, ...topic.partitions.map((p) => p.messages))
  const context = contextOfTopic(topic.topic)

  return (
    <section
      aria-label={topic.topic}
      className={cn("min-w-0 rounded-xl border", topic.isDlq ? "bg-destructive/[0.03]" : "bg-muted/20")}
    >
      <header className="flex flex-wrap items-center gap-2 border-b px-3 py-2">
        <ContextBadge topic={topic.topic} />
        <span className="truncate font-mono text-sm font-medium">{topic.topic}</span>
        <span className="ml-auto text-xs text-muted-foreground tabular-nums">
          {topic.exists ? `${total} msg · ${topic.partitions.length} partitions` : "not created yet"}
        </span>
      </header>

      {topic.exists && (
        <div
          className="grid gap-2 p-2"
          style={{ gridTemplateColumns: `repeat(${Math.max(1, topic.partitions.length)}, minmax(0, 1fr))` }}
        >
          {topic.partitions.map((p) => (
            <PartitionColumn
              key={p.partition}
              partition={p}
              max={max}
              keys={partitions?.get(p.partition) ?? []}
              barClass={topic.isDlq ? "bg-destructive/70" : context ? CONTEXT_BAR[context] : "bg-muted-foreground"}
              compact={compact}
              highlighted={highlighted}
              onHighlight={onHighlight}
              onOpen={onOpen}
            />
          ))}
        </div>
      )}
    </section>
  )
}

/** Solid fills matching CONTEXT_STYLES in lib/contexts.ts. */
const CONTEXT_BAR: Record<BoundedContext, string> = {
  orders: "bg-sky-500/70",
  inventory: "bg-amber-500/70",
  payments: "bg-emerald-500/70",
  shipping: "bg-violet-500/70",
}

interface PartitionColumnProps extends HighlightProps {
  partition: KafkaPartition
  max: number
  keys: KeyEntry[]
  barClass: string
  compact?: boolean
}

function PartitionColumn({
  partition,
  max,
  keys,
  barClass,
  compact,
  highlighted,
  onHighlight,
  onOpen,
}: PartitionColumnProps) {
  const { messages, lowOffset, highOffset } = partition
  const holdsHighlighted = highlighted !== null && keys.some((k) => k.key === highlighted)
  const shown = keys.slice(0, MAX_CHIPS)

  return (
    <div
      className={cn(
        "flex min-w-0 flex-col gap-1.5 rounded-lg border bg-card p-2 transition-shadow",
        holdsHighlighted && "ring-2 ring-foreground/50",
      )}
    >
      <div className="flex items-baseline justify-between gap-1">
        <span className="font-mono text-xs font-semibold">P{partition.partition}</span>
        <span className="text-[11px] text-muted-foreground tabular-nums">
          {messages} msg
        </span>
      </div>
      <div
        className="h-1 overflow-hidden rounded-full bg-muted"
        role="img"
        aria-label={`${messages} of ${max} messages`}
      >
        <div className={cn("h-full rounded-full", barClass)} style={{ width: `${(messages / max) * 100}%` }} />
      </div>
      <div className="truncate font-mono text-[11px] text-muted-foreground tabular-nums" title="Offsets in the log">
        {messages === 0 ? `next #${highOffset}` : `#${lowOffset} … #${highOffset - 1}`}
      </div>

      {!compact || keys.length > 0 ? (
        <div className="flex flex-wrap gap-1">
          {shown.map((k) => (
            <OrderChip
              key={k.key}
              orderKey={k.key}
              count={k.count}
              highlighted={highlighted}
              onHighlight={onHighlight}
              onOpen={onOpen}
            />
          ))}
          {keys.length > MAX_CHIPS && (
            <span className="text-[11px] text-muted-foreground">+{keys.length - MAX_CHIPS} more</span>
          )}
          {keys.length === 0 && messages > 0 && (
            <span className="text-[11px] text-muted-foreground italic">older records not loaded</span>
          )}
        </div>
      ) : null}
    </div>
  )
}
