// Bounded contexts of the study domain, derived from the Kafka topic name.
// `orders.events` -> orders, `payments.events.dlq` -> payments (DLQ).

export const CONTEXTS = ["orders", "inventory", "payments", "shipping"] as const
export type BoundedContext = (typeof CONTEXTS)[number]

export function contextOfTopic(topic: string): BoundedContext | null {
  const prefix = topic.split(".")[0]
  return (CONTEXTS as readonly string[]).includes(prefix) ? (prefix as BoundedContext) : null
}

export function isDlqTopic(topic: string): boolean {
  return topic.endsWith(".dlq")
}

/** Badge classes per context; DLQ rows use the destructive palette instead. */
export const CONTEXT_STYLES: Record<BoundedContext, string> = {
  orders: "bg-sky-500/15 text-sky-700 dark:text-sky-300 border-sky-500/30",
  inventory: "bg-amber-500/15 text-amber-700 dark:text-amber-300 border-amber-500/30",
  payments: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/30",
  shipping: "bg-violet-500/15 text-violet-700 dark:text-violet-300 border-violet-500/30",
}

export const UNKNOWN_CONTEXT_STYLE = "bg-muted text-muted-foreground border-border"
export const DLQ_STYLE = "bg-destructive/15 text-destructive border-destructive/40"
