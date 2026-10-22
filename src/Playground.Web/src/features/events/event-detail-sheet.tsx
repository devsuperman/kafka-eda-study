import { ArchiveXIcon } from "lucide-react"

import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { deadLetterInfo, DLQ_HEADER_NAMES } from "@/features/dead-letters/dead-letter"
import type { ObservedEvent } from "@/lib/api"
import { formatDateTime, prettyJson } from "@/lib/format"
import { cn } from "@/lib/utils"

import { ContextBadge } from "./context-badge"

interface EventDetailSheetProps {
  event: ObservedEvent | null
  onOpenChange: (open: boolean) => void
}

export function EventDetailSheet({ event, onOpenChange }: EventDetailSheetProps) {
  return (
    <Sheet open={event !== null} onOpenChange={onOpenChange}>
      <SheetContent className="w-full gap-0 sm:max-w-2xl data-[side=right]:sm:max-w-2xl">
        {event && <EventDetail event={event} />}
      </SheetContent>
    </Sheet>
  )
}

function EventDetail({ event }: { event: ObservedEvent }) {
  const headers = Object.entries(event.headers ?? {})
  const unparseable = event.eventType === null

  return (
    <>
      <SheetHeader className="border-b pr-12">
        <div className="flex items-center gap-2">
          <ContextBadge topic={event.topic} />
          {event.version !== null && (
            <span className="font-mono text-xs text-muted-foreground">v{event.version}</span>
          )}
        </div>
        <SheetTitle className={unparseable ? "italic" : undefined}>
          {event.eventType ?? "Unparseable message"}
        </SheetTitle>
        <SheetDescription>
          One Kafka record: where it lives (topic / partition / offset), how it was routed (key), and
          what it says (payload).
        </SheetDescription>
      </SheetHeader>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="flex flex-col gap-6 p-4">
          {event.isDlq && (
            <>
              <DeadLetterSection event={event} />
              <Separator />
            </>
          )}

          <Section
            title="Kafka record"
            hint="The key picks the partition; offsets only grow within a partition, so ordering is per key."
          >
            <Fields
              rows={[
                ["Topic", event.topic],
                ["Partition", String(event.partition)],
                ["Offset", String(event.offset)],
                ["Key", event.key],
                ["Timestamp", formatDateTime(event.timestamp)],
                ["DLQ", event.isDlq ? "yes (dead-letter topic)" : "no"],
              ]}
            />
          </Section>

          <Separator />

          <Section
            title="Envelope"
            hint="Correlation id ties every event of one business flow together; causation id points to the event that caused this one."
          >
            <Fields
              rows={[
                ["Event type", event.eventType],
                ["Event id", event.eventId],
                ["Order id", event.orderId],
                ["Correlation id", event.correlationId],
                ["Causation id", event.causationId],
                ["Version", event.version === null ? null : String(event.version)],
                ["Occurred at", event.occurredAt ? formatDateTime(event.occurredAt) : null],
              ]}
            />
          </Section>

          <Separator />

          <Section title={`Headers (${headers.length})`} hint="Kafka headers: metadata carried next to the value.">
            {headers.length === 0 ? (
              <p className="text-sm text-muted-foreground">No headers.</p>
            ) : (
              <div className="overflow-x-auto rounded-md border">
                <table className="w-full text-left text-xs">
                  <thead className="bg-muted/50 text-muted-foreground">
                    <tr>
                      <th className="px-3 py-1.5 font-medium">Name</th>
                      <th className="px-3 py-1.5 font-medium">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {headers.map(([name, value]) => (
                      <tr
                        key={name}
                        className={cn("border-t", event.isDlq && DLQ_HEADER_NAMES.has(name) && "bg-destructive/5")}
                      >
                        <td className="px-3 py-1.5 align-top font-mono whitespace-nowrap">{name}</td>
                        <td className="px-3 py-1.5 font-mono break-all">{value}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Section>

          <Separator />

          {event.payload !== null && event.payload !== undefined && (
            <Section title="Payload" hint="The event body, pretty-printed.">
              <CodeBlock>{prettyJson(event.payload)}</CodeBlock>
            </Section>
          )}

          {(unparseable || event.payload === null || event.payload === undefined) && (
            <Section
              title="Raw value"
              hint={
                unparseable
                  ? "This value could not be parsed as an event envelope. In a real consumer this is a poison message: it would be sent to a DLQ."
                  : "The record value exactly as stored in Kafka."
              }
            >
              <CodeBlock>{event.raw || "(empty)"}</CodeBlock>
            </Section>
          )}
        </div>
      </div>
    </>
  )
}

function DeadLetterSection({ event }: { event: ObservedEvent }) {
  const info = deadLetterInfo(event)
  const original =
    info.originalPartition !== null || info.originalOffset !== null
      ? `${info.originalTopic} · partition ${info.originalPartition ?? "?"} · offset ${info.originalOffset ?? "?"}`
      : info.originalTopic
  return (
    <section className="flex flex-col gap-3 rounded-lg border border-destructive/40 bg-destructive/5 p-3">
      <div className="flex items-start gap-2">
        <ArchiveXIcon className="mt-0.5 size-4 shrink-0 text-destructive" aria-hidden />
        <div>
          <h3 className="text-sm font-semibold text-destructive">Dead letter</h3>
          <p className="text-xs text-muted-foreground">
            A message that kept failing (or could never be processed) was parked here so the consumer could move
            on. Key, value and original headers are the failed message unchanged; the headers below say why and
            where it came from.
          </p>
        </div>
      </div>
      <div className="rounded-md border border-destructive/30 bg-background p-2.5">
        <p className="text-xs font-medium text-muted-foreground">Error</p>
        <p className="font-mono text-xs leading-5 break-words whitespace-pre-wrap text-destructive">
          {info.error ?? "(no error header)"}
        </p>
      </div>
      <Fields
        rows={[
          ["Original record", original],
          ["Consumer group", info.consumerGroup],
          ["Failed at", info.failedAt ? formatDateTime(info.failedAt) : null],
        ]}
      />
    </section>
  )
}

function Section({ title, hint, children }: { title: string; hint?: string; children: React.ReactNode }) {
  return (
    <section className="flex flex-col gap-2">
      <div>
        <h3 className="text-sm font-semibold">{title}</h3>
        {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
      </div>
      {children}
    </section>
  )
}

function Fields({ rows }: { rows: [label: string, value: string | null][] }) {
  return (
    <dl className="grid grid-cols-[8rem_minmax(0,1fr)] gap-x-4 gap-y-1.5 text-sm">
      {rows.map(([label, value]) => (
        <div key={label} className="contents">
          <dt className="text-muted-foreground">{label}</dt>
          <dd className={value === null ? "text-muted-foreground" : "font-mono text-xs leading-5 break-all"}>
            {value ?? "—"}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function CodeBlock({ children }: { children: string }) {
  return (
    <pre className="max-h-[28rem] overflow-auto rounded-md border bg-muted/40 p-3 font-mono text-xs leading-relaxed">
      {children}
    </pre>
  )
}
