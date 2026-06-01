import { useEffect, useState } from 'react'
import './index.css'
import { Dashboard, type ClusterOverview } from './components/Dashboard'

function App() {
  const [overview, setOverview] = useState<ClusterOverview | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState<boolean>(true)

  const fetchOverview = async () => {
    try {
      const response = await fetch(
        'http://127.0.0.1:8088/api/v1/cluster/overview',
      )
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }
      const data = await response.json()
      setOverview(data)
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

  return <Dashboard overview={overview} loading={loading} error={error} />
}

export default App
