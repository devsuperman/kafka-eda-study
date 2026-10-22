import type { ObservedEvent } from "@/lib/api"
import { contextOfTopic, type BoundedContext } from "@/lib/contexts"

/** One swimlane per publishing context. Each context owns its topic, and every record of an order is keyed by orderId. */
export const LANES: { context: BoundedContext; title: string }[] = [
  { context: "orders", title: "Orders" },
  { context: "inventory", title: "Inventory" },
  { context: "payments", title: "Payments" },
  { context: "shipping", title: "Shipping" },
]

/** Index of the swimlane an event belongs to, or -1 when its topic has no lane (e.g. a DLQ). */
export function laneIndex(event: ObservedEvent): number {
  const context = contextOfTopic(event.topic)
  return LANES.findIndex((lane) => lane.context === context)
}

/** Business time order: when the event happened, with the Kafka record time as fallback. */
export function byOccurrence(a: ObservedEvent, b: ObservedEvent): number {
  const ta = Date.parse(a.occurredAt ?? a.timestamp)
  const tb = Date.parse(b.occurredAt ?? b.timestamp)
  if (ta !== tb) return ta - tb
  return a.offset - b.offset
}

/** Where the order stands, read from its events: Pending, Confirmed, Shipped, Delivered or Cancelled (rules O3, O4). */
export function orderStatus(events: ObservedEvent[]): string {
  const live = events.filter((e) => !e.isDlq)
  const has = (type: string) => live.some((e) => e.eventType === type)

  if (has("OrderDelivered")) return "Delivered"
  if (has("OrderShipped")) return "Shipped"
  const cancelled = live.find((e) => e.eventType === "OrderCancelled")
  if (cancelled) return `Cancelled · ${(cancelled.payload as { reason?: string } | null)?.reason ?? "unknown"}`
  if (has("OrderConfirmed")) return "Confirmed"
  if (has("OrderPlaced")) return "Pending"
  return "Unknown"
}
