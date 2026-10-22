/** `14:03:27.512` in local time; falls back to the input if unparseable. */
export function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  const pad = (n: number, len = 2) => String(n).padStart(len, "0")
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`
}

/** Full local date-time plus the original ISO string, for detail views. */
export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return `${d.toLocaleString()} (${iso})`
}

/** First 8 chars of a GUID-like id. */
export function shortId(id: string | null | undefined): string {
  if (!id) return "—"
  return id.length > 8 ? id.slice(0, 8) : id
}

export function prettyJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}
