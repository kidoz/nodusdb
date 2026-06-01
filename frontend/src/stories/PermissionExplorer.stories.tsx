import type { Meta, StoryObj } from '@storybook/react-vite'
import { PermissionExplorer } from '../components/PermissionExplorer'

const meta = {
  title: 'Components/PermissionExplorer',
  component: PermissionExplorer,
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 940, padding: 20 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof PermissionExplorer>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
