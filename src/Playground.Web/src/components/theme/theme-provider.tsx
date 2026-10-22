import { useCallback, useEffect, useMemo, useState, useSyncExternalStore } from "react"

import { THEME_STORAGE_KEY, ThemeContext, type Theme } from "./use-theme"

const media = window.matchMedia("(prefers-color-scheme: dark)")

function subscribeToSystem(onChange: () => void) {
  media.addEventListener("change", onChange)
  return () => media.removeEventListener("change", onChange)
}

function readStored(): Theme {
  try {
    const v = localStorage.getItem(THEME_STORAGE_KEY)
    return v === "light" || v === "dark" ? v : "system"
  } catch {
    return "system"
  }
}

/**
 * Follows the OS color scheme by default; the header toggle can pin light/dark.
 * Toggles the `dark` class on <html> (shadcn convention). index.html applies the
 * class before first paint to avoid a flash.
 */
export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(readStored)
  const systemDark = useSyncExternalStore(subscribeToSystem, () => media.matches)
  const resolvedTheme = theme === "system" ? (systemDark ? "dark" : "light") : theme

  useEffect(() => {
    const root = document.documentElement
    root.classList.toggle("dark", resolvedTheme === "dark")
    root.style.colorScheme = resolvedTheme
  }, [resolvedTheme])

  const setTheme = useCallback((next: Theme) => {
    setThemeState(next)
    try {
      if (next === "system") localStorage.removeItem(THEME_STORAGE_KEY)
      else localStorage.setItem(THEME_STORAGE_KEY, next)
    } catch {
      // storage unavailable: theme still applies for this session
    }
  }, [])

  const value = useMemo(() => ({ theme, resolvedTheme, setTheme }), [theme, resolvedTheme, setTheme])
  return <ThemeContext value={value}>{children}</ThemeContext>
}
