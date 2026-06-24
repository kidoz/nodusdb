import type { Meta, StoryObj } from '@storybook/react-vite'
import { StatCard } from '../components/StatCard'

const meta = {
  title: 'Components/StatCard',
  component: StatCard,
  args: {
    label: 'Queries / sec',
    value: '1843.2',
    detail: '▲ 4.1% vs 1h avg',
    tone: 'healthy',
    emphasis: false,
  },
  argTypes: {
    tone: {
      control: 'select',
      options: ['healthy', 'degraded', 'critical', 'neutral'],
    },
  },
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 260, padding: 20 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof StatCard>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Emphasis: Story = {
  args: {
    label: 'Active alerts',
    value: 0,
    detail: 'all checks passing',
    tone: 'healthy',
    emphasis: true,
  },
}

export const Critical: Story = {
  args: {
    label: 'Active alerts',
    value: 3,
    detail: 'needs attention',
    tone: 'critical',
    emphasis: true,
  },
}
