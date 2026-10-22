import { createContext, useContext, useSyncExternalStore } from "react"

import type { EventStore, EventStoreSnapshot } from "./event-store"

export const EventStoreContext = createContext<EventStore | null>(null)

/** Live snapshot of the shared event store: records, connection status, counters. */
export function useEventStream(): EventStoreSnapshot {
  const store = useContext(EventStoreContext)
  if (!store) throw new Error("useEventStream must be used inside <EventStreamProvider>")
  return useSyncExternalStore(store.subscribe, store.getSnapshot)
}
