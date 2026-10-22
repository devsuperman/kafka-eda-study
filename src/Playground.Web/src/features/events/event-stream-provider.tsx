import { useEffect, useState } from "react"

import { EventStore } from "./event-store"
import { EventStoreContext } from "./use-event-stream"

/** Owns the single SSE connection for the app (shared by the header and all pages). */
export function EventStreamProvider({ children }: { children: React.ReactNode }) {
  const [store] = useState(() => new EventStore())

  useEffect(() => {
    store.start()
    return () => store.stop()
  }, [store])

  return <EventStoreContext value={store}>{children}</EventStoreContext>
}
