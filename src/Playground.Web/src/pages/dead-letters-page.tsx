import { DeadLettersTable } from "@/features/dead-letters/dead-letters-table"

/**
 * Step 5: errors, retries and the dead-letter topic. Each `<topic>.dlq` record is the failed message
 * unchanged plus headers saying why it failed and where it came from.
 */
export function DeadLettersPage() {
  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-col gap-2">
        <h1 className="font-heading text-xl font-semibold tracking-tight">Dead letters</h1>
        <p className="max-w-4xl text-sm text-muted-foreground">
          Kafka delivers a partition in order, so a consumer that keeps failing on one message blocks every message
          behind it. Instead it retries a few times and then parks the message in{" "}
          <code className="font-mono text-xs">&lt;topic&gt;.dlq</code> with headers{" "}
          <code className="font-mono text-xs">error</code>, <code className="font-mono text-xs">original-topic</code>
          /<code className="font-mono text-xs">-partition</code>/<code className="font-mono text-xs">-offset</code>,{" "}
          <code className="font-mono text-xs">consumer-group</code> and{" "}
          <code className="font-mono text-xs">failed-at</code>. Nothing is lost: someone can inspect, fix and replay it
          later.
        </p>
      </div>

      <DeadLettersTable />
    </div>
  )
}
