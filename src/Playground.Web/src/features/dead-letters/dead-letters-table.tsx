import { useMemo, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { ArchiveXIcon, Loader2Icon } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { EventDetailSheet } from "@/features/events/event-detail-sheet"
import { useEventStream } from "@/features/events/use-event-stream"
import { api, type ObservedEvent } from "@/lib/api"
import { formatTime, shortId } from "@/lib/format"

import { deadLetterInfo } from "./dead-letter"

/** Same cap as the API's ring buffer: the viewer asks for every DLQ record the API still holds. */
const DLQ_LIMIT = 1000

function newestFirst(a: ObservedEvent, b: ObservedEvent): number {
  const diff = Date.parse(b.timestamp) - Date.parse(a.timestamp)
  return diff !== 0 ? diff : b.offset - a.offset
}

/**
 * Every record of the `*.dlq` topics, newest first. History comes from GET /api/events?dlq=true
 * (the shared feed only backfills the newest records of all topics, so old dead letters could be
 * missing there); new ones arrive through the live SSE store.
 */
export function DeadLettersTable() {
  const history = useQuery({
    queryKey: ["events", "dlq"],
    queryFn: ({ signal }) => api.getEvents({ dlq: true, limit: DLQ_LIMIT }, signal),
  })
  const { items } = useEventStream()
  const [selected, setSelected] = useState<ObservedEvent | null>(null)

  const records = useMemo(() => {
    const byId = new Map<string, ObservedEvent>()
    for (const e of history.data ?? []) byId.set(e.id, e)
    for (const { event } of items) if (event.isDlq) byId.set(event.id, event)
    return [...byId.values()].sort(newestFirst)
  }, [history.data, items])

  return (
    <Card className="min-w-0 gap-0 py-0">
      <CardHeader className="border-b py-4">
        <CardTitle className="flex items-center gap-2">
          <ArchiveXIcon className="size-4 text-destructive" aria-hidden />
          Dead letters
          <Badge variant={records.length > 0 ? "destructive" : "secondary"}>{records.length}</Badge>
        </CardTitle>
        <CardDescription>
          A message that keeps failing is parked here so the consumer can move on. Handler errors are retried 3
          times first; poison messages (malformed JSON, an unknown SKU) come straight here. Each consumer group
          parks its own copy.
        </CardDescription>
      </CardHeader>
      <CardContent className="px-0">
        {records.length === 0 ? (
          <p className="flex items-center justify-center gap-2 px-4 py-16 text-center text-sm text-muted-foreground">
            {history.isPending ? (
              <>
                <Loader2Icon className="size-4 animate-spin" /> Loading dead letters…
              </>
            ) : history.isError ? (
              <>Could not load dead letters: {history.error.message}</>
            ) : (
              <>No dead letters yet. Run “Send poison message” on the Live stream page.</>
            )}
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full min-w-[56rem] text-left text-sm">
              <thead className="border-b bg-muted/40 text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Time</th>
                  <th className="px-3 py-2 font-medium">Original record</th>
                  <th className="px-3 py-2 font-medium">Consumer group</th>
                  <th className="px-3 py-2 font-medium">Error</th>
                  <th className="px-3 py-2 font-medium">Failed at</th>
                  <th className="px-3 py-2 font-medium">Key</th>
                </tr>
              </thead>
              <tbody>
                {records.map((record) => (
                  <DeadLetterRow key={record.id} record={record} onSelect={setSelected} />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>

      <EventDetailSheet event={selected} onOpenChange={(open) => !open && setSelected(null)} />
    </Card>
  )
}

function DeadLetterRow({ record, onSelect }: { record: ObservedEvent; onSelect: (e: ObservedEvent) => void }) {
  const info = deadLetterInfo(record)
  return (
    <tr
      className="cursor-pointer border-b align-top last:border-b-0 hover:bg-muted/60 focus-visible:bg-muted/60 focus-visible:outline-none"
      tabIndex={0}
      onClick={() => onSelect(record)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault()
          onSelect(record)
        }
      }}
      aria-label={`Dead letter from ${info.originalTopic}, open details`}
    >
      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap text-muted-foreground tabular-nums">
        <time dateTime={record.timestamp}>{formatTime(record.timestamp)}</time>
        <div className="text-[11px]" title={`${record.topic} · partition ${record.partition} · offset ${record.offset}`}>
          dlq p{record.partition} · #{record.offset}
        </div>
      </td>
      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap">
        <div>{info.originalTopic ?? "—"}</div>
        <div className="text-muted-foreground tabular-nums">
          p{info.originalPartition ?? "?"} · #{info.originalOffset ?? "?"}
        </div>
      </td>
      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap">{info.consumerGroup ?? "—"}</td>
      <td className="max-w-md px-3 py-2">
        <p className="line-clamp-2 font-mono text-xs break-words text-destructive" title={info.error ?? undefined}>
          {info.error ?? "—"}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground italic">
          {record.eventType ?? "unparseable message"}
        </p>
      </td>
      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap text-muted-foreground tabular-nums">
        {info.failedAt ? <time dateTime={info.failedAt}>{formatTime(info.failedAt)}</time> : "—"}
      </td>
      <td className="px-3 py-2 font-mono text-xs whitespace-nowrap" title={record.key ?? undefined}>
        {shortId(record.key)}
      </td>
    </tr>
  )
}
