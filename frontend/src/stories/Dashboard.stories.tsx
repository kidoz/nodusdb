import type { Meta, StoryObj } from '@storybook/react-vite'
import { Dashboard } from '../components/Dashboard'

const healthyOverview = {
  cluster_status: 'Healthy',
  nodes_live: 6,
  nodes_total: 6,
  shards_total: 128,
  shards_unavailable: 0,
  qps: 1843.2,
  active_alerts: 0,
  qps_delta_pct: 4.1,
  p99_latency_ms: 3.2,
  replication_lag_ms: 11,
}

const meta = {
  title: 'Screens/Dashboard',
  component: Dashboard,
  args: {
    overview: healthyOverview,
    loading: false,
    error: null,
    updatedAt: new Date(),
  },
} satisfies Meta<typeof Dashboard>

export default meta
type Story = StoryObj<typeof meta>

export const Healthy: Story = {}

export const Degraded: Story = {
  args: {
    overview: {
      ...healthyOverview,
      cluster_status: 'Degraded',
      nodes_live: 4,
      shards_unavailable: 3,
      qps: 219.7,
      qps_delta_pct: -38.2,
      p99_latency_ms: 24.6,
      replication_lag_ms: 940,
      active_alerts: 2,
    },
  },
}

export const Loading: Story = {
  args: {
    overview: null,
    loading: true,
  },
}

export const ConnectionError: Story = {
  args: {
    overview: null,
    error: 'Failed to fetch cluster overview',
  },
}
