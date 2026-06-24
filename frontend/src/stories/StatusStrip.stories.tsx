import type { Meta, StoryObj } from '@storybook/react-vite'
import { StatusStrip } from '../components/StatusStrip'

const meta = {
  title: 'Components/StatusStrip',
  component: StatusStrip,
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
    tone: 'healthy',
  },
  argTypes: {
    tone: {
      control: 'select',
      options: ['healthy', 'degraded', 'critical', 'neutral'],
    },
  },
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 900, padding: 20 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof StatusStrip>

export default meta
type Story = StoryObj<typeof meta>

export const Healthy: Story = {}

export const Degraded: Story = {
  args: {
    overview: {
      cluster_status: 'Degraded',
      nodes_live: 4,
      nodes_total: 6,
      shards_total: 128,
      shards_unavailable: 3,
      qps: 219.7,
      active_alerts: 2,
    },
    tone: 'degraded',
  },
}
