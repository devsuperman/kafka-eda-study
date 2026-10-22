import { lazy, Suspense } from "react"
import { Navigate, Route, Routes } from "react-router"

import { AppShell } from "@/components/layout/app-shell"
import { LiveStreamPage } from "@/pages/live-stream-page"

// Secondary pages are code-split so the live stream (the landing page) loads first.
const KafkaInternalsPage = lazy(() =>
  import("@/pages/kafka-internals-page").then((m) => ({ default: m.KafkaInternalsPage })),
)

const OrdersPage = lazy(() => import("@/pages/orders-page").then((m) => ({ default: m.OrdersPage })))

const OrderDetailPage = lazy(() =>
  import("@/pages/order-detail-page").then((m) => ({ default: m.OrderDetailPage })),
)

const DeadLettersPage = lazy(() =>
  import("@/pages/dead-letters-page").then((m) => ({ default: m.DeadLettersPage })),
)

const pageFallback = <p className="py-16 text-center text-sm text-muted-foreground">Loading…</p>

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<LiveStreamPage />} />
        <Route
          path="kafka"
          element={
            <Suspense fallback={pageFallback}>
              <KafkaInternalsPage />
            </Suspense>
          }
        />
        <Route
          path="dead-letters"
          element={
            <Suspense fallback={pageFallback}>
              <DeadLettersPage />
            </Suspense>
          }
        />
        <Route
          path="orders"
          element={
            <Suspense fallback={pageFallback}>
              <OrdersPage />
            </Suspense>
          }
        />
        <Route
          path="orders/:orderId"
          element={
            <Suspense fallback={pageFallback}>
              <OrderDetailPage />
            </Suspense>
          }
        />
        {/* Later steps: /services */}
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
