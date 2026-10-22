import { useEffect, useMemo, useState } from "react"
import { Link, useParams } from "react-router"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { orderStatus } from "@/features/orders/order-flow"
import { Swimlane } from "@/features/orders/swimlane"
import { EventDetailSheet } from "@/features/events/event-detail-sheet"
import { useEventStream } from "@/features/events/use-event-stream"
import { api, type ObservedEvent } from "@/lib/api"

/**
 * Order detail: the flow of one order as a swimlane. The events come from the server (the whole history of this order)
 * and from the live stream, so a flow that is still running keeps moving on the page.
 */
export function OrderDetailPage() {
  const { orderId = "" } = useParams()
  const { items } = useEventStream()
  const [history, setHistory] = useState<ObservedEvent[]>([])
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<ObservedEvent | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    api
      .getEvents({ orderId, limit: 200 }, controller.signal)
      .then((events) => {
        setHistory(events)
        setError(null)
      })
      .catch((err: unknown) => {
        if (!controller.signal.aborted) setError(err instanceof Error ? err.message : String(err))
      })
    return () => controller.abort()
  }, [orderId])

  const events = useMemo(() => {
    const byId = new Map(history.map((e) => [e.id, e]))
    for (const { event } of items) {
      if (event.orderId === orderId) byId.set(event.id, event)
    }
    return [...byId.values()]
  }, [history, items, orderId])

  const status = orderStatus(events)
  const correlationId = events.find((e) => e.correlationId)?.correlationId

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-2">
        <div>
          <h1 className="font-heading text-xl font-semibold tracking-tight">Order flow</h1>
          <p className="font-mono text-xs break-all text-muted-foreground">
            order {orderId}
            {correlationId && <> · correlation {correlationId}</>}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="outline" className="font-mono">
            {status}
          </Badge>
          <Link to="/orders" className="text-sm text-muted-foreground underline-offset-4 hover:underline">
            All orders
          </Link>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Swimlane</CardTitle>
          <CardDescription>
            One lane per bounded context. Each event is a box in time order; an arrow runs from the event that caused a
            reaction to the reaction. Click a box for the Kafka record.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {error && <p className="mb-3 text-sm text-destructive">Could not load this order&apos;s history: {error}</p>}
          {events.length === 0 ? (
            <p className="py-10 text-center text-sm text-muted-foreground">
              No events for this order yet. Place one from the live stream page.
            </p>
          ) : (
            <Swimlane events={events} onSelect={setSelected} />
          )}
        </CardContent>
      </Card>

      <EventDetailSheet event={selected} onOpenChange={(open) => !open && setSelected(null)} />
    </div>
  )
}
