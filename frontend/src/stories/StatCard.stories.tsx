import type { Meta, StoryObj } from '@storybook/react-vite'
import { StatCard } from '../components/StatCard'

const meta = {
  title: 'Components/StatCard',
  component: StatCard,
  args: {
    label: 'Queries per second',
    value: '842.4',
    detail: 'current sample',
    tone: 'neutral',
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

export const Critical: Story = {
  args: {
    label: 'Active alerts',
    value: 3,
    detail: 'requires review',
    tone: 'critical',
  },
}
