import { useCallback, useMemo, useState } from "react"
import { PauseIcon, PlayIcon, SearchIcon, XIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Switch } from "@/components/ui/switch"
import type { ObservedEvent } from "@/lib/api"
import { CONTEXTS, CONTEXT_STYLES, contextOfTopic, type BoundedContext } from "@/lib/contexts"
import { cn } from "@/lib/utils"

import { EventDetailSheet } from "./event-detail-sheet"
import { MAX_EVENTS, type FeedItem } from "./event-store"
import { useEventStream } from "./use-event-stream"
import { EventRow } from "./event-row"

interface EventFeedProps {
  orderFilter: string
  onOrderFilterChange: (value: string) => void
}

interface PausedState {
  items: FeedItem[]
  receivedAtPause: number
}

export function EventFeed({ orderFilter, onOrderFilterChange }: EventFeedProps) {
  const { items: liveItems, received, loaded, status } = useEventStream()
  const [enabled, setEnabled] = useState<Record<BoundedContext, boolean>>({
    orders: true,
    inventory: true,
    payments: true,
    shipping: true,
  })
  const [paused, setPaused] = useState<PausedState | null>(null)
  const [selected, setSelected] = useState<ObservedEvent | null>(null)

  // When paused we render a frozen snapshot; the store keeps buffering.
  const items = paused?.items ?? liveItems
  const newWhilePaused = paused ? received - paused.receivedAtPause : 0

  const visible = useMemo(() => {
    const needle = orderFilter.trim().toLowerCase()
    return items.filter(({ event }) => {
      const context = contextOfTopic(event.topic)
      if (context && !enabled[context]) return false
      // The key is the orderId too, and it is the only id a malformed (unparseable) record still has.
      if (needle && !(event.orderId ?? event.key ?? "").toLowerCase().includes(needle)) return false
      return true
    })
  }, [items, enabled, orderFilter])

  // eventId -> how many records carry it. The same eventId on two records means the same event was
  // delivered again (a retrying producer, the "resend duplicate" scenario). DLQ copies are skipped:
  // a dead letter repeats its original's eventId on purpose.
  const duplicates = useMemo(() => {
    const counts = new Map<string, number>()
    for (const { event } of items)
      if (event.eventId && !event.isDlq) counts.set(event.eventId, (counts.get(event.eventId) ?? 0) + 1)
    return counts
  }, [items])

  const togglePause = () =>
    setPaused((p) => (p ? null : { items: liveItems, receivedAtPause: received }))

  const handleSelect = useCallback(
    (id: string) => setSelected(items.find((i) => i.event.id === id)?.event ?? null),
    [items],
  )

  return (
    <Card className="min-w-0 gap-0 py-0">
      <CardHeader className="gap-3 border-b py-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <CardTitle>Live event feed</CardTitle>
            <CardDescription>
              Each row is one Kafka record. Same order ⇒ same key ⇒ same partition, so events of one order
              stay in order.
            </CardDescription>
          </div>
          <Button
            variant={paused ? "default" : "outline"}
            size="sm"
            onClick={togglePause}
            aria-pressed={paused !== null}
          >
            {paused ? <PlayIcon /> : <PauseIcon />}
            {paused ? "Resume" : "Pause"}
            {paused && newWhilePaused > 0 && (
              <Badge variant="secondary" className="ml-1">
                {newWhilePaused} new
              </Badge>
            )}
          </Button>
        </div>

        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="relative w-full sm:w-72">
            <SearchIcon className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={orderFilter}
              onChange={(e) => onOrderFilterChange(e.target.value)}
              placeholder="Filter by orderId…"
              aria-label="Filter by orderId"
              className="pr-8 pl-8 font-mono text-xs placeholder:font-sans placeholder:text-sm"
            />
            {orderFilter && (
              <Button
                variant="ghost"
                size="icon-xs"
                className="absolute top-1/2 right-1.5 -translate-y-1/2"
                onClick={() => onOrderFilterChange("")}
                aria-label="Clear orderId filter"
              >
                <XIcon />
              </Button>
            )}
          </div>

          <div className="flex flex-wrap items-center gap-x-4 gap-y-2" role="group" aria-label="Contexts">
            {CONTEXTS.map((context) => (
              <label key={context} className="flex cursor-pointer items-center gap-1.5 text-xs">
                <Switch
                  size="sm"
                  checked={enabled[context]}
                  onCheckedChange={(checked) => setEnabled((s) => ({ ...s, [context]: checked }))}
                />
                <span
                  className={cn(
                    "rounded border px-1.5 py-0.5 font-mono uppercase",
                    CONTEXT_STYLES[context],
                    !enabled[context] && "opacity-50",
                  )}
                >
                  {context}
                </span>
              </label>
            ))}
          </div>
        </div>
      </CardHeader>

      <CardContent className="px-0">
        <ScrollArea className="h-[calc(100svh-19rem)] min-h-80">
          {visible.length === 0 ? (
            <EmptyState
              loaded={loaded}
              hasEvents={items.length > 0}
              connected={status === "open"}
            />
          ) : (
            <ul aria-label="Kafka records, newest first">
              {visible.map((item) => (
                <EventRow
                  key={item.event.id}
                  item={item}
                  duplicateCount={item.event.eventId && !item.event.isDlq ? (duplicates.get(item.event.eventId) ?? 1) : 1}
                  onSelect={handleSelect}
                  onOrderClick={onOrderFilterChange}
                />
              ))}
            </ul>
          )}
        </ScrollArea>
        <div className="flex justify-between border-t px-3 py-2 text-xs text-muted-foreground">
          <span>
            Showing {visible.length} of {items.length}
            {paused && " (paused)"}
          </span>
          <span>Browser keeps the newest {MAX_EVENTS} records</span>
        </div>
      </CardContent>

      <EventDetailSheet event={selected} onOpenChange={(open) => !open && setSelected(null)} />
    </Card>
  )
}

function EmptyState({ loaded, hasEvents, connected }: { loaded: boolean; hasEvents: boolean; connected: boolean }) {
  let text: string
  if (hasEvents) text = "No records match the current filters."
  else if (!loaded) text = "Loading recent records…"
  else if (!connected) text = "Not connected to Playground.Api yet. Is it running on :5003?"
  else text = "No records yet. Run a scenario to publish your first event."
  return <p className="px-4 py-16 text-center text-sm text-muted-foreground">{text}</p>
}
