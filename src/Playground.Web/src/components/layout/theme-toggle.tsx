import { MonitorIcon, MoonIcon, SunIcon } from "lucide-react"

import { useTheme, type Theme } from "@/components/theme/use-theme"

import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"

const ORDER: Theme[] = ["system", "light", "dark"]

const META: Record<Theme, { icon: typeof SunIcon; label: string }> = {
  system: { icon: MonitorIcon, label: "Theme: system" },
  light: { icon: SunIcon, label: "Theme: light" },
  dark: { icon: MoonIcon, label: "Theme: dark" },
}

/** Cycles system -> light -> dark. Defaults to following the OS preference. */
export function ThemeToggle() {
  const { theme, setTheme } = useTheme()
  const next = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length]
  const { icon: Icon, label } = META[theme]

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button variant="ghost" size="icon" aria-label={`${label}. Switch to ${next}.`} onClick={() => setTheme(next)}>
          <Icon />
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label} (click for {next})</TooltipContent>
    </Tooltip>
  )
}
