import type { Meta, StoryObj } from '@storybook/react-vite'
import { StatusBadge } from '../components/StatusBadge'

const meta = {
  title: 'Components/StatusBadge',
  component: StatusBadge,
  args: {
    label: 'Healthy',
    tone: 'healthy',
  },
  argTypes: {
    tone: {
      control: 'select',
      options: ['healthy', 'degraded', 'critical', 'neutral'],
    },
  },
} satisfies Meta<typeof StatusBadge>

export default meta
type Story = StoryObj<typeof meta>

export const Healthy: Story = {}

export const Degraded: Story = {
  args: {
    label: 'Degraded',
    tone: 'degraded',
  },
}

export const Critical: Story = {
  args: {
    label: 'Unavailable',
    tone: 'critical',
  },
}
