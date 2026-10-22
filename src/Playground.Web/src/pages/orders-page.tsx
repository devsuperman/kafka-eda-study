import { useMemo } from "react"
import { Link } from "react-router"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { orderStatus } from "@/features/orders/order-flow"
import { useEventStream } from "@/features/events/use-event-stream"
import type { ObservedEvent } from "@/lib/api"
import { formatTime } from "@/lib/format"

interface OrderSummary {
  orderId: string
  status: string
  placedAt: string | null
}

/** Orders seen in the live stream (those with an OrderPlaced), newest first. Each one opens its swimlane. */
export function OrdersPage() {
  const { items } = useEventStream()

  const orders = useMemo(() => {
    const byOrder = new Map<string, ObservedEvent[]>()
    for (const { event } of items) {
      if (!event.orderId || event.isDlq) continue
      byOrder.set(event.orderId, [...(byOrder.get(event.orderId) ?? []), event])
    }

    const summaries: OrderSummary[] = []
    for (const [orderId, events] of byOrder) {
      const placed = events.find((e) => e.eventType === "OrderPlaced")
      if (!placed) continue
      summaries.push({ orderId, status: orderStatus(events), placedAt: placed.occurredAt ?? placed.timestamp })
    }
    return summaries.sort((a, b) => Date.parse(b.placedAt ?? "") - Date.parse(a.placedAt ?? ""))
  }, [items])

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h1 className="font-heading text-xl font-semibold tracking-tight">Order detail</h1>
        <p className="text-sm text-muted-foreground">
          Pick an order to see its flow across the services as a swimlane. Orders that are still running update live.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Orders in the stream</CardTitle>
          <CardDescription>Placed during this session or still in the live feed.</CardDescription>
        </CardHeader>
        <CardContent>
          {orders.length === 0 ? (
            <p className="py-10 text-center text-sm text-muted-foreground">
              No orders yet. Run a scenario on the <Link className="underline" to="/">live stream</Link> page first.
            </p>
          ) : (
            <ul className="divide-y">
              {orders.map((order) => (
                <li key={order.orderId}>
                  <Link
                    to={`/orders/${order.orderId}`}
                    className="flex flex-wrap items-center justify-between gap-2 py-3 hover:bg-muted/40"
                  >
                    <span className="font-mono text-sm break-all">{order.orderId}</span>
                    <span className="flex items-center gap-3">
                      <Badge variant="outline" className="font-mono">
                        {order.status}
                      </Badge>
                      <span className="font-mono text-xs text-muted-foreground">{formatTime(order.placedAt)}</span>
                    </span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
