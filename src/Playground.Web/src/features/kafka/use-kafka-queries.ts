import { useQuery } from "@tanstack/react-query"

import { api } from "@/lib/api"

/** Both panels poll every 2 s. TanStack Query pauses polling while the tab is hidden
 * (refetchIntervalInBackground defaults to false) and refetches when it becomes visible. */
export const KAFKA_REFRESH_MS = 2000

export function useKafkaTopics() {
  return useQuery({
    queryKey: ["kafka", "topics"],
    queryFn: ({ signal }) => api.getKafkaTopics(signal),
    refetchInterval: KAFKA_REFRESH_MS,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
  })
}

export function useConsumerGroups() {
  return useQuery({
    queryKey: ["kafka", "consumer-groups"],
    queryFn: ({ signal }) => api.getConsumerGroups(signal),
    refetchInterval: KAFKA_REFRESH_MS,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
  })
}
