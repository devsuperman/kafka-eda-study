import { ActivityIcon, ArchiveXIcon, BoxesIcon, PackageSearchIcon, ServerIcon, type LucideIcon } from "lucide-react"

export interface NavItem {
  to: string
  label: string
  icon: LucideIcon
  /** False renders a disabled placeholder. Flip to true when the page ships. */
  available: boolean
}

// Add new pages here and in routes.tsx.
export const NAV_ITEMS: NavItem[] = [
  { to: "/", label: "Live stream", icon: ActivityIcon, available: true },
  { to: "/orders", label: "Order detail", icon: PackageSearchIcon, available: false },
  { to: "/kafka", label: "Kafka internals", icon: BoxesIcon, available: true },
  { to: "/dead-letters", label: "Dead letters", icon: ArchiveXIcon, available: true },
  { to: "/services", label: "Service state", icon: ServerIcon, available: false },
]
