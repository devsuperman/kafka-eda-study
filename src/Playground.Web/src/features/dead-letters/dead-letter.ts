import type { ObservedEvent } from "@/lib/api"

/**
 * Headers the consumer base adds when it parks a message in `<topic>.dlq`. The rest of the record
 * (key, value, original headers) is the failed message, unchanged.
 */
export const DLQ_HEADERS = {
  error: "error",
  originalTopic: "original-topic",
  originalPartition: "original-partition",
  originalOffset: "original-offset",
  consumerGroup: "consumer-group",
  failedAt: "failed-at",
} as const

export interface DeadLetterInfo {
  error: string | null
  originalTopic: string | null
  originalPartition: string | null
  originalOffset: string | null
  consumerGroup: string | null
  failedAt: string | null
}

export function deadLetterInfo(event: ObservedEvent): DeadLetterInfo {
  const h = event.headers ?? {}
  const get = (name: string) => h[name] ?? null
  return {
    error: get(DLQ_HEADERS.error),
    originalTopic: get(DLQ_HEADERS.originalTopic) ?? event.topic.replace(/\.dlq$/, ""),
    originalPartition: get(DLQ_HEADERS.originalPartition),
    originalOffset: get(DLQ_HEADERS.originalOffset),
    consumerGroup: get(DLQ_HEADERS.consumerGroup),
    failedAt: get(DLQ_HEADERS.failedAt),
  }
}

/** Header names shown in the dedicated "Dead letter" block, so the generic table can skip them. */
export const DLQ_HEADER_NAMES: ReadonlySet<string> = new Set(Object.values(DLQ_HEADERS))
