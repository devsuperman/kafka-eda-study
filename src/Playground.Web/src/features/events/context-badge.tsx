import { Badge } from "@/components/ui/badge"
import {
  CONTEXT_STYLES,
  contextOfTopic,
  DLQ_STYLE,
  isDlqTopic,
  UNKNOWN_CONTEXT_STYLE,
} from "@/lib/contexts"
import { cn } from "@/lib/utils"

/** Colored badge naming the bounded context that owns a topic; DLQ topics in red. */
export function ContextBadge({ topic, className }: { topic: string; className?: string }) {
  const context = contextOfTopic(topic)
  const dlq = isDlqTopic(topic)
  const style = dlq ? DLQ_STYLE : context ? CONTEXT_STYLES[context] : UNKNOWN_CONTEXT_STYLE
  return (
    <Badge variant="outline" className={cn("font-mono uppercase tracking-wide", style, className)}>
      {context ?? topic}
      {dlq && " · dlq"}
    </Badge>
  )
}
