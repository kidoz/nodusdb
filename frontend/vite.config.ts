import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// Proxy the admin/HTTP API through the dev server so the SPA only ever calls its
// own origin — this avoids cross-origin requests to the backend being blocked by
// the browser or privacy extensions. Override the target for the Docker dev
// cluster via NODUS_API_TARGET (e.g. http://nodus1:8088); defaults to a server
// running on the host.
const apiTarget = process.env.NODUS_API_TARGET ?? 'http://127.0.0.1:8088'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      '/api': { target: apiTarget, changeOrigin: true },
    },
  },
})
