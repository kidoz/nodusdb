import type { Meta, StoryObj } from '@storybook/react-vite'
import { Dashboard } from '../components/Dashboard'

const healthyOverview = {
  cluster_status: 'Healthy',
  nodes_live: 5,
  nodes_total: 5,
  shards_total: 48,
  shards_unavailable: 0,
  qps: 842.4,
  active_alerts: 0,
}

const meta = {
  title: 'Screens/Dashboard',
  component: Dashboard,
  args: {
    overview: healthyOverview,
    loading: false,
    error: null,
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
