import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { useEventStream } from "@/features/events/use-event-stream"
import type { ConnectionStatus } from "@/features/events/event-store"
import { cn } from "@/lib/utils"

const META: Record<ConnectionStatus, { label: string; dot: string; hint: string }> = {
  open: {
    label: "Live",
    dot: "bg-emerald-500",
    hint: "Connected to /api/events/stream (Server-Sent Events).",
  },
  connecting: {
    label: "Connecting…",
    dot: "bg-muted-foreground animate-pulse",
    hint: "Opening the Server-Sent Events stream.",
  },
  reconnecting: {
    label: "Reconnecting…",
    dot: "bg-amber-500 animate-pulse",
    hint: "Stream lost. Retrying; missed events are backfilled on reconnect. Is Playground.Api running?",
  },
}

export function ConnectionIndicator() {
  const { status } = useEventStream()
  const meta = META[status]
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span
          role="status"
          className="inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs font-medium"
        >
          <span className={cn("size-2 rounded-full", meta.dot)} aria-hidden />
          {meta.label}
        </span>
      </TooltipTrigger>
      <TooltipContent className="max-w-64">{meta.hint}</TooltipContent>
    </Tooltip>
  )
}
