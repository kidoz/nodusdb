import type { Meta, StoryObj } from '@storybook/react-vite'
import { NodeStrip } from '../components/NodeStrip'

const meta = {
  title: 'Components/NodeStrip',
  component: NodeStrip,
  args: {
    overview: {
      cluster_status: 'Healthy',
      nodes_live: 6,
      nodes_total: 6,
      shards_total: 128,
      shards_unavailable: 0,
      qps: 1843.2,
      active_alerts: 0,
    },
  },
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 900, padding: 20 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof NodeStrip>

export default meta
type Story = StoryObj<typeof meta>

export const Derived: Story = {}

export const ExplicitRoster: Story = {
  args: {
    overview: {
      cluster_status: 'Degraded',
      nodes_live: 5,
      nodes_total: 6,
      shards_total: 128,
      shards_unavailable: 21,
      qps: 612.5,
      active_alerts: 1,
      nodes: [
        { id: 'node-1', role: 'leader', shards: 22, healthy: true },
        { id: 'node-2', role: 'replica', shards: 21, healthy: true },
        { id: 'node-3', role: 'replica', shards: 21, healthy: true },
        { id: 'node-4', role: 'replica', shards: 0, healthy: false },
        { id: 'node-5', role: 'replica', shards: 22, healthy: true },
        { id: 'node-6', role: 'replica', shards: 21, healthy: true },
      ],
    },
  },
}
