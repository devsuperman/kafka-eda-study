import { memo } from "react"

import { shortId } from "@/lib/format"
import { cn } from "@/lib/utils"

import { KEY_CHIP_CLASS, keyHueStyle } from "./key-color"

interface OrderChipProps {
  /** The Kafka record key (the orderId for every well-formed event). */
  orderKey: string
  /** How many records with this key sit in the partition (within the browser's buffer). */
  count?: number
  /** Key currently highlighted on the page, if any. */
  highlighted: string | null
  onHighlight: (key: string | null) => void
  onOpen: (key: string) => void
}

/** A short, color-hashed order id. Hover/focus highlights it everywhere; click opens the live stream. */
export const OrderChip = memo(function OrderChip({
  orderKey,
  count = 1,
  highlighted,
  onHighlight,
  onOpen,
}: OrderChipProps) {
  const active = highlighted === orderKey
  const dimmed = highlighted !== null && !active
  return (
    <button
      type="button"
      style={keyHueStyle(orderKey)}
      className={cn(
        "inline-flex h-5 items-center gap-0.5 rounded border px-1 font-mono text-[11px] leading-none transition-[opacity,box-shadow] outline-none",
        KEY_CHIP_CLASS,
        active && "ring-2 ring-foreground/70 ring-offset-1 ring-offset-background",
        dimmed && "opacity-25",
      )}
      title={`${orderKey}${count > 1 ? ` · ${count} records` : ""}\nClick to open in the live stream`}
      onMouseEnter={() => onHighlight(orderKey)}
      onMouseLeave={() => onHighlight(null)}
      onFocus={() => onHighlight(orderKey)}
      onBlur={() => onHighlight(null)}
      onClick={() => onOpen(orderKey)}
    >
      {shortId(orderKey)}
      {count > 1 && <span className="opacity-70">×{count}</span>}
    </button>
  )
})
