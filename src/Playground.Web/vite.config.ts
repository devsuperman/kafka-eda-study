import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(import.meta.dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Playground.Api (dotnet run) listens on http://localhost:5003.
      // SSE (/api/events/stream) works through this proxy as-is.
      '/api': {
        target: process.env.PLAYGROUND_API_URL ?? 'http://localhost:5003',
        changeOrigin: true,
        configure: (proxy) => {
          // If the API dies mid-stream, the dev proxy would otherwise keep the
          // browser's SSE connection open forever and the UI would still say
          // "Live". Tear it down so EventSource notices and reconnects.
          proxy.on('proxyRes', (proxyRes, _req, res) => {
            proxyRes.on('close', () => {
              if (!res.writableEnded) res.destroy()
            })
          })
        },
      },
    },
  },
})
