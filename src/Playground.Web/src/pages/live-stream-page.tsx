import { useCallback } from "react"
import { useSearchParams } from "react-router"

import { EventFeed } from "@/features/events/event-feed"
import { ScenarioPanel } from "@/features/scenarios/scenario-panel"

/**
 * Step 1: run a scenario on the left, watch the resulting Kafka records on the right.
 * The orderId filter lives in the URL (?orderId=...) so a filtered view can be shared/reloaded.
 */
export function LiveStreamPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const orderFilter = searchParams.get("orderId") ?? ""

  const setOrderFilter = useCallback(
    (value: string) =>
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (value) next.set("orderId", value)
          else next.delete("orderId")
          return next
        },
        { replace: true },
      ),
    [setSearchParams],
  )

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h1 className="font-heading text-xl font-semibold tracking-tight">Live stream</h1>
        <p className="text-sm text-muted-foreground">
          Services never call each other: they publish facts to Kafka topics and react to each other&apos;s
          facts. This page shows every record on <code className="font-mono text-xs">orders</code>,{" "}
          <code className="font-mono text-xs">inventory</code>, <code className="font-mono text-xs">payments</code>{" "}
          and <code className="font-mono text-xs">shipping</code> topics (and their DLQs) as it happens.
        </p>
      </div>

      <div className="grid items-start gap-4 lg:grid-cols-[20rem_minmax(0,1fr)]">
        <ScenarioPanel orderFilter={orderFilter} onFocusOrder={setOrderFilter} />
        <EventFeed orderFilter={orderFilter} onOrderFilterChange={setOrderFilter} />
      </div>
    </div>
  )
}
