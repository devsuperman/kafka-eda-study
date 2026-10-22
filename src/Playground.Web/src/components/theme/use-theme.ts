import { createContext, useContext } from "react"

export type Theme = "system" | "light" | "dark"

export interface ThemeState {
  /** What the user picked. */
  theme: Theme
  /** What is actually applied after resolving "system". */
  resolvedTheme: "light" | "dark"
  setTheme: (theme: Theme) => void
}

export const ThemeContext = createContext<ThemeState | null>(null)

export const THEME_STORAGE_KEY = "eda-playground-theme"

export function useTheme(): ThemeState {
  const ctx = useContext(ThemeContext)
  if (!ctx) throw new Error("useTheme must be used inside <ThemeProvider>")
  return ctx
}
