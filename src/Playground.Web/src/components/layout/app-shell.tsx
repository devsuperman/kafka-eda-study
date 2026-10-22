import { RadioTowerIcon } from "lucide-react"
import { NavLink, Outlet } from "react-router"

import { NAV_ITEMS } from "@/app/nav"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

import { ConnectionIndicator } from "./connection-indicator"
import { ThemeToggle } from "./theme-toggle"

const navItemClass =
  "inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors"

export function AppShell() {
  return (
    <div className="flex min-h-svh flex-col">
      <header className="sticky top-0 z-40 border-b bg-background/85 backdrop-blur supports-backdrop-filter:bg-background/70">
        <div className="mx-auto flex max-w-screen-2xl flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
          <div className="flex items-center gap-2">
            <RadioTowerIcon className="size-5 text-primary" aria-hidden />
            <span className="font-heading text-base font-semibold tracking-tight">EDA Playground</span>
          </div>

          <nav aria-label="Main" className="flex flex-wrap items-center gap-1">
            {NAV_ITEMS.map(({ to, label, icon: Icon, available }) =>
              available ? (
                <NavLink
                  key={to}
                  to={to}
                  end
                  className={({ isActive }) =>
                    cn(
                      navItemClass,
                      isActive
                        ? "bg-secondary text-secondary-foreground"
                        : "text-muted-foreground hover:bg-muted hover:text-foreground",
                    )
                  }
                >
                  <Icon className="size-4" aria-hidden />
                  {label}
                </NavLink>
              ) : (
                <Tooltip key={to}>
                  <TooltipTrigger asChild>
                    {/* span wrapper: disabled elements do not fire pointer events */}
                    <span
                      aria-disabled
                      tabIndex={0}
                      className={cn(navItemClass, "cursor-not-allowed text-muted-foreground/60")}
                    >
                      <Icon className="size-4" aria-hidden />
                      {label}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent>Coming in later steps</TooltipContent>
                </Tooltip>
              ),
            )}
          </nav>

          <div className="ml-auto flex items-center gap-2">
            <ConnectionIndicator />
            <ThemeToggle />
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-screen-2xl flex-1 px-4 py-6">
        <Outlet />
      </main>
    </div>
  )
}
