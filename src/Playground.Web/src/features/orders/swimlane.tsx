import { useMemo } from "react"

import type { ObservedEvent } from "@/lib/api"
import { CONTEXT_STYLES } from "@/lib/contexts"
import { formatTime } from "@/lib/format"
import { cn } from "@/lib/utils"

import { byOccurrence, LANES, laneIndex } from "./order-flow"

// Fixed geometry, so the arrows can be drawn from plain coordinates instead of measuring the DOM.
const PAD = 16
const LABEL_W = 112
const COL_W = 200
const LANE_H = 104
const CARD_W = 172
const CARD_H = 64

interface Placed {
  event: ObservedEvent
  lane: number
  left: number
  top: number
}

interface Arrow {
  from: Placed
  to: Placed
}

/** Places each event in its own column (in time order) and links every reaction to the event that caused it. */
function layoutBoard(events: ObservedEvent[]) {
  const placed: Placed[] = events
    .filter((e) => !e.isDlq && laneIndex(e) >= 0)
    .sort(byOccurrence)
    .map((event, column) => {
      const lane = laneIndex(event)
      return {
        event,
        lane,
        left: LABEL_W + PAD + column * COL_W + (COL_W - CARD_W) / 2,
        top: lane * LANE_H + (LANE_H - CARD_H) / 2,
      }
    })

  const byEventId = new Map(placed.map((p) => [p.event.eventId, p]))
  const arrows: Arrow[] = []
  for (const effect of placed) {
    const cause = effect.event.causationId ? byEventId.get(effect.event.causationId) : undefined
    if (cause) arrows.push({ from: cause, to: effect })
  }

  return {
    placed,
    arrows,
    width: LABEL_W + PAD * 2 + Math.max(placed.length, 1) * COL_W,
    height: LANES.length * LANE_H,
  }
}

interface SwimlaneProps {
  /** All records of one order, in any order. */
  events: ObservedEvent[]
  onSelect: (event: ObservedEvent) => void
}

/** One lane per bounded context, one column per event, arrows from cause to reaction. */
export function Swimlane({ events, onSelect }: SwimlaneProps) {
  const board = useMemo(() => layoutBoard(events), [events])

  return (
    <div className="overflow-x-auto rounded-xl border bg-card">
      <div className="relative" style={{ width: board.width, height: board.height }}>
        {LANES.map((lane, index) => (
          <div
            key={lane.context}
            className={cn("absolute inset-x-0 border-t first:border-t-0", index % 2 === 1 && "bg-muted/30")}
            style={{ top: index * LANE_H, height: LANE_H }}
          >
            <span
              className="absolute top-1/2 left-3 -translate-y-1/2 text-sm font-medium"
              style={{ width: LABEL_W - 12 }}
            >
              {lane.title}
            </span>
          </div>
        ))}

        <svg
          className="pointer-events-none absolute inset-0"
          width={board.width}
          height={board.height}
          aria-hidden="true"
        >
          <defs>
            <marker id="swimlane-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L10,5 L0,10 z" className="fill-muted-foreground" />
            </marker>
          </defs>
          {board.arrows.map(({ from, to }) => {
            const x1 = from.left + CARD_W
            const y1 = from.top + CARD_H / 2
            const x2 = to.left
            const y2 = to.top + CARD_H / 2
            const dx = (x2 - x1) / 2
            return (
              <path
                key={`${from.event.id}->${to.event.id}`}
                d={`M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`}
                fill="none"
                strokeWidth={1.5}
                className="stroke-muted-foreground"
                markerEnd="url(#swimlane-arrow)"
              />
            )
          })}
        </svg>

        {board.placed.map((p) => {
          const context = LANES[p.lane].context
          const version = p.event.version === null ? "" : `v${p.event.version} · `
          return (
            <button
              key={p.event.id}
              type="button"
              onClick={() => onSelect(p.event)}
              aria-label={`${p.event.eventType} in ${LANES[p.lane].title}`}
              className={cn(
                "absolute flex flex-col justify-center gap-0.5 rounded-lg border px-3 text-left shadow-sm transition",
                "hover:ring-2 hover:ring-ring/50 focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none",
                CONTEXT_STYLES[context],
              )}
              style={{ left: p.left, top: p.top, width: CARD_W, height: CARD_H }}
            >
              <span className="truncate font-mono text-xs font-semibold">{p.event.eventType}</span>
              <span className="truncate text-[11px] text-muted-foreground">
                {version}
                {formatTime(p.event.occurredAt ?? p.event.timestamp)}
              </span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
