import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { Dashboard, type ClusterOverview } from './Dashboard'

const overview: ClusterOverview = {
  cluster_status: 'Healthy',
  nodes_live: 3,
  nodes_total: 3,
  shards_total: 12,
  shards_unavailable: 0,
  qps: 1843.2,
  active_alerts: 0,
  qps_delta_pct: 4.1,
  p99_latency_ms: 3.2,
  replication_lag_ms: 11,
}

describe('Dashboard', () => {
  it('exposes the loading state and primary navigation', () => {
    render(<Dashboard overview={null} loading />)

    expect(screen.getByText('Loading cluster overview...')).toBeVisible()
    expect(screen.getByRole('navigation', { name: 'Primary' })).toBeVisible()
  })

  it('renders cluster metrics in the accessible metrics region', () => {
    render(<Dashboard overview={overview} />)

    expect(
      screen.getByRole('region', { name: 'Cluster metrics' }),
    ).toBeVisible()
    expect(screen.getByText('1843.2')).toBeVisible()
    expect(screen.getByText('all checks passing')).toBeVisible()
  })

  it('renders connection errors visibly', () => {
    render(<Dashboard overview={null} error="HTTP 503" />)

    expect(
      screen.getByText(/Error connecting to NodusDB cluster:/),
    ).toBeVisible()
    expect(screen.getByText('HTTP 503')).toBeVisible()
  })
})
