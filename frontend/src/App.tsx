import { useEffect, useState } from 'react'
import './index.css'
import { Dashboard, type ClusterOverview } from './components/Dashboard'

function App() {
  const [overview, setOverview] = useState<ClusterOverview | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState<boolean>(true)
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null)

  const fetchOverview = async () => {
    try {
      // Relative path: served same-origin in production and proxied to the
      // backend by the Vite dev server (see vite.config.ts).
      const response = await fetch('/api/v1/cluster/overview')
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }
      const data = await response.json()
      setOverview(data)
      setUpdatedAt(new Date())
      setError(null)
    } catch (error: unknown) {
      setError(error instanceof Error ? error.message : String(error))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchOverview()
    const interval = setInterval(fetchOverview, 5000)
    return () => clearInterval(interval)
  }, [])

  return (
    <Dashboard
      overview={overview}
      loading={loading}
      error={error}
      updatedAt={updatedAt}
    />
  )
}

export default App
