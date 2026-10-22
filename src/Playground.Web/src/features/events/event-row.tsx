import { memo } from "react"

import { CopyIcon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { formatTime, shortId } from "@/lib/format"
import { cn } from "@/lib/utils"

import { ContextBadge } from "./context-badge"
import type { FeedItem } from "./event-store"

interface EventRowProps {
  item: FeedItem
  /** How many records in the feed share this record's eventId (1 = unique). */
  duplicateCount?: number
  onSelect: (id: string) => void
  onOrderClick: (orderId: string) => void
}

export const EventRow = memo(function EventRow({ item, duplicateCount = 1, onSelect, onOrderClick }: EventRowProps) {
  const { event, live } = item
  const unparseable = event.eventType === null
  // A malformed record has no envelope, but its key is still the orderId it was published under.
  const orderKey = event.orderId ?? event.key

  return (
    <li
      className={cn(
        "group grid cursor-pointer grid-cols-[auto_auto_minmax(0,1fr)_auto] items-center gap-x-3 gap-y-1 border-b px-3 py-2 text-sm last:border-b-0 hover:bg-muted/60 sm:grid-cols-[6.5rem_7.5rem_minmax(0,1fr)_6rem_8rem]",
        live && "motion-safe:animate-row-flash",
        event.isDlq && "bg-destructive/5",
      )}
      onClick={() => onSelect(event.id)}
    >
      <time dateTime={event.timestamp} className="font-mono text-xs text-muted-foreground tabular-nums">
        {formatTime(event.timestamp)}
      </time>

      <ContextBadge topic={event.topic} className="justify-self-start" />

      <span className="flex min-w-0 items-center gap-2">
        <button
          type="button"
          className={cn(
            "truncate text-left font-medium focus-visible:outline-none",
            unparseable && "italic text-muted-foreground",
          )}
          title={event.eventType ?? "The value could not be parsed as an event envelope"}
          onClick={(e) => {
            e.stopPropagation()
            onSelect(event.id)
          }}
        >
          {event.eventType ?? "unparseable message"}
        </button>
        {duplicateCount > 1 && (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge
                tabIndex={0}
                variant="outline"
                className="border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300"
                aria-label={`duplicate: ${duplicateCount} records share eventId ${event.eventId}`}
              >
                <CopyIcon aria-hidden />
                duplicate ×{duplicateCount}
              </Badge>
            </TooltipTrigger>
            <TooltipContent>Same event delivered again — idempotent consumers ignore it</TooltipContent>
          </Tooltip>
        )}
      </span>

      {orderKey ? (
        <button
          type="button"
          className="justify-self-start rounded px-1 font-mono text-xs text-primary underline-offset-2 hover:underline"
          title={`Filter by ${event.orderId ? "order" : "key"} ${orderKey}`}
          onClick={(e) => {
            e.stopPropagation()
            onOrderClick(orderKey)
          }}
        >
          {shortId(orderKey)}
        </button>
      ) : (
        <span className="font-mono text-xs text-muted-foreground">—</span>
      )}

      <span
        className="col-span-4 font-mono text-xs text-muted-foreground tabular-nums sm:col-span-1 sm:text-right"
        title={`${event.topic} · partition ${event.partition} · offset ${event.offset}`}
      >
        p{event.partition} · #{event.offset}
      </span>
    </li>
  )
})
