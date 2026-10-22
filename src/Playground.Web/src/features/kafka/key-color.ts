import type { CSSProperties } from "react"

/**
 * Stable color per Kafka key (orderId): the same order gets the same hue in every topic,
 * which makes "same key ⇒ same partition" visible at a glance. The hue goes into a CSS
 * variable so the light/dark variants live in Tailwind classes (KEY_CHIP_CLASS).
 */
export function keyHueStyle(key: string): CSSProperties {
  let h = 2166136261 // FNV-1a
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i)
    h = Math.imul(h, 16777619)
  }
  // Golden-angle spread keeps neighbouring hashes visually apart.
  const hue = Math.round(((h >>> 0) * 137.508) % 360)
  return { "--key-hue": hue } as CSSProperties
}

export const KEY_CHIP_CLASS =
  "border-[oklch(0.85_0.07_var(--key-hue))] bg-[oklch(0.95_0.04_var(--key-hue))] text-[oklch(0.42_0.12_var(--key-hue))] " +
  "dark:border-[oklch(0.45_0.09_var(--key-hue))] dark:bg-[oklch(0.3_0.06_var(--key-hue))] dark:text-[oklch(0.88_0.08_var(--key-hue))]"
