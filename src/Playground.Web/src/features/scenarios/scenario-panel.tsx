import { useState } from "react"
import { useMutation, useQuery } from "@tanstack/react-query"
import { Loader2Icon, LockIcon, PlayIcon, RefreshCwIcon } from "lucide-react"
import { useNavigate } from "react-router"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { api, type PoisonKind, type Scenario, type ScenarioInput } from "@/lib/api"
import { shortId } from "@/lib/format"
import { cn } from "@/lib/utils"

const RESEND_DUPLICATE = "resend-duplicate-event"
const SEND_POISON = "send-poison-message"
/** Scenarios that act on one order: the feed's orderId filter picks it (otherwise the scenario picks the newest that fits). */
const ORDER_TARGETED = new Set([RESEND_DUPLICATE, "cancel-order", "dispatch-shipment", "deliver-shipment"])

const GUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

const POISON_KINDS: { kind: PoisonKind; label: string; hint: string }[] = [
  { kind: "unknown-sku", label: "Unknown SKU", hint: "Valid JSON, impossible data: an OrderPlaced for SKU XX-999." },
  { kind: "malformed-json", label: "Malformed JSON", hint: "A value that is not JSON at all: nobody can even read it." },
]

interface ScenarioPanelProps {
  /** The feed's orderId filter; a full GUID there targets the "resend duplicate" scenario. */
  orderFilter: string
  /** Called when the user wants to focus the feed on the order a scenario created. */
  onFocusOrder: (orderId: string) => void
}

interface RunRequest {
  scenario: Scenario
  input?: ScenarioInput
}

export function ScenarioPanel({ orderFilter, onFocusOrder }: ScenarioPanelProps) {
  const navigate = useNavigate()
  const scenarios = useQuery({ queryKey: ["scenarios"], queryFn: api.getScenarios })
  const [poisonKind, setPoisonKind] = useState<PoisonKind>("unknown-sku")

  const targetOrder = GUID.test(orderFilter.trim()) ? orderFilter.trim() : null

  const run = useMutation({
    mutationFn: ({ scenario, input }: RunRequest) => api.runScenario(scenario.name, input),
    onSuccess: (result, { scenario, input }) => {
      const focus = result.orderId
        ? { label: "Filter feed", onClick: () => onFocusOrder(result.orderId!) }
        : undefined

      if (scenario.name === RESEND_DUPLICATE) {
        toast.success("Duplicate OrderPlaced re-sent (same eventId)", {
          description: `${result.message} Watch for: a "duplicate ×N" badge in the feed and NO new StockReserved, PaymentAuthorized or notification for this order.`,
          duration: 12_000,
          action: focus,
        })
      } else if (scenario.name === SEND_POISON) {
        const malformed = input?.kind === "malformed-json"
        toast.success(malformed ? "Malformed message published" : "Poison OrderPlaced published (unknown SKU)", {
          description: `${result.message} Watch for: one record per consumer group in orders.events.dlq (no retries for poison), while the consumers keep processing new orders.`,
          duration: 12_000,
          action: { label: "Dead letters", onClick: () => void navigate("/dead-letters") },
        })
      } else {
        toast.success(result.message || `${scenario.title} started`, {
          description: result.orderId ? `orderId ${result.orderId}` : undefined,
          action: focus,
        })
      }
    },
    onError: (error, { scenario }) => {
      toast.error(`${scenario.title} failed`, { description: error.message })
    },
  })

  const runScenario = (scenario: Scenario) => {
    let input: ScenarioInput | undefined
    if (ORDER_TARGETED.has(scenario.name) && targetOrder) input = { orderId: targetOrder }
    else if (scenario.name === SEND_POISON) input = { kind: poisonKind }
    run.mutate({ scenario, input })
  }

  const optionsFor = (scenario: Scenario): React.ReactNode => {
    if (!scenario.available) return null
    if (scenario.name === RESEND_DUPLICATE)
      return (
        <p className="text-xs text-muted-foreground">
          Re-sends{" "}
          {targetOrder ? (
            <>
              the OrderPlaced of order <span className="font-mono text-foreground">{shortId(targetOrder)}</span> (from
              the feed filter).
            </>
          ) : (
            <>the latest OrderPlaced. Filter the feed by a full orderId to pick another one.</>
          )}
        </p>
      )
    if (ORDER_TARGETED.has(scenario.name) && scenario.name !== RESEND_DUPLICATE)
      return (
        <p className="text-xs text-muted-foreground">
          Acts on{" "}
          {targetOrder ? (
            <>
              order <span className="font-mono text-foreground">{shortId(targetOrder)}</span> (from the feed filter).
            </>
          ) : (
            <>the newest order that fits. Filter the feed by a full orderId to pick one.</>
          )}
        </p>
      )
    if (scenario.name === SEND_POISON)
      return (
        <div className="flex flex-col gap-1.5">
          <div role="group" aria-label="Poison message kind" className="inline-flex self-start rounded-md border p-0.5">
            {POISON_KINDS.map(({ kind, label }) => (
              <button
                key={kind}
                type="button"
                aria-pressed={poisonKind === kind}
                onClick={() => setPoisonKind(kind)}
                className={cn(
                  "rounded px-2 py-0.5 text-xs font-medium transition-colors outline-none focus-visible:ring-2 focus-visible:ring-ring/50",
                  poisonKind === kind
                    ? "bg-secondary text-secondary-foreground"
                    : "text-muted-foreground hover:text-foreground",
                )}
              >
                {label}
              </button>
            ))}
          </div>
          <p className="text-xs text-muted-foreground">{POISON_KINDS.find((k) => k.kind === poisonKind)?.hint}</p>
        </div>
      )
    return null
  }

  return (
    <Card className="gap-0 py-0">
      <CardHeader className="border-b py-4">
        <CardTitle>Scenarios</CardTitle>
        <CardDescription>
          Each button asks Playground.Api to start a business flow or inject a fault. Watch the events it triggers
          appear in the feed.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-col gap-2 p-3">
        {scenarios.isPending && (
          <p className="flex items-center gap-2 p-2 text-sm text-muted-foreground">
            <Loader2Icon className="size-4 animate-spin" /> Loading scenarios…
          </p>
        )}

        {scenarios.isError && (
          <div className="flex flex-col gap-2 rounded-md border border-dashed p-3 text-sm">
            <p className="text-muted-foreground">
              Could not load scenarios: {scenarios.error.message}
            </p>
            <Button variant="outline" size="sm" className="self-start" onClick={() => void scenarios.refetch()}>
              <RefreshCwIcon /> Retry
            </Button>
          </div>
        )}

        {scenarios.data?.length === 0 && (
          <p className="p-2 text-sm text-muted-foreground">The API returned no scenarios.</p>
        )}

        {scenarios.data?.map((scenario) => (
          <ScenarioButton
            key={scenario.name}
            scenario={scenario}
            running={run.isPending && run.variables?.scenario.name === scenario.name}
            onRun={() => runScenario(scenario)}
            options={optionsFor(scenario)}
          />
        ))}
      </CardContent>
    </Card>
  )
}

function ScenarioButton({
  scenario,
  running,
  onRun,
  options,
}: {
  scenario: Scenario
  running: boolean
  onRun: () => void
  /** Small controls rendered under the button (never inside it: no nested interactive elements). */
  options?: React.ReactNode
}) {
  const body = (
    <Button
      variant="outline"
      className="h-auto w-full flex-col items-start gap-1 px-3 py-2.5 text-left whitespace-normal"
      disabled={!scenario.available || running}
      onClick={onRun}
    >
      <span className="flex w-full items-center gap-2 font-medium">
        {running ? (
          <Loader2Icon className="animate-spin" />
        ) : scenario.available ? (
          <PlayIcon />
        ) : (
          <LockIcon />
        )}
        {scenario.title}
      </span>
      <span className="text-xs font-normal text-muted-foreground">{scenario.description}</span>
    </Button>
  )

  if (scenario.available)
    return options ? (
      <div className="flex flex-col gap-2">
        {body}
        <div className="ml-3 border-l-2 pl-3">{options}</div>
      </div>
    ) : (
      body
    )

  // Disabled buttons swallow pointer events, so the tooltip hangs off a wrapper.
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div tabIndex={0} className="rounded-lg" aria-label={`${scenario.title}: available in a later step`}>
          {body}
        </div>
      </TooltipTrigger>
      <TooltipContent>Available in a later step</TooltipContent>
    </Tooltip>
  )
}
