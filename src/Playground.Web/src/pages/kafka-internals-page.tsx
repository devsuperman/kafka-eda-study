import { useCallback, useState } from "react"
import { useNavigate } from "react-router"

import { ConsumerGroupsSection } from "@/features/kafka/consumer-groups-section"
import { TopicsSection } from "@/features/kafka/topics-section"

/**
 * Steps 2–3: keys, partitions and ordering; consumer groups, rebalancing and lag.
 * Offsets/groups come from the broker's Admin API (polled); the order chips come from the
 * live event store, so a newly placed order shows up here as soon as its events are published.
 */
export function KafkaInternalsPage() {
  const navigate = useNavigate()
  const [highlighted, setHighlighted] = useState<string | null>(null)

  const openOrder = useCallback(
    (key: string) => navigate(`/?${new URLSearchParams({ orderId: key })}`),
    [navigate],
  )

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <h1 className="font-heading text-xl font-semibold tracking-tight">Kafka internals</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">
          A topic is split into partitions; each partition is an append-only log with its own offsets. That gives
          Kafka its two big properties, and both are visible below:
        </p>
        <ul className="grid max-w-5xl gap-2 text-sm text-muted-foreground md:grid-cols-2">
          <li className="rounded-lg border bg-card p-3">
            <span className="font-medium text-foreground">Ordering is per partition.</span> The key (
            <code className="font-mono text-xs">orderId</code>) decides the partition, so one order&apos;s events
            are always read in the order they were written. There is no ordering across partitions or topics, which
            is why Orders must accept <code className="font-mono text-xs">StockReserved</code> and{" "}
            <code className="font-mono text-xs">PaymentAuthorized</code> in any order.
          </li>
          <li className="rounded-lg border bg-card p-3">
            <span className="font-medium text-foreground">A consumer group splits partitions among members.</span>{" "}
            Scale a worker and watch the rebalance:{" "}
            <code className="font-mono text-xs break-all">docker compose up -d --scale inventory-worker=3</code>, then{" "}
            <code className="font-mono text-xs break-all">docker compose stop inventory-worker</code>. While a group
            is down its lag (end offset − committed offset) grows; when it comes back it resumes from its committed
            offsets.
          </li>
        </ul>
      </div>

      <TopicsSection highlighted={highlighted} onHighlight={setHighlighted} onOpen={openOrder} />
      <ConsumerGroupsSection />
    </div>
  )
}
