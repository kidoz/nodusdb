import type { Preview } from '@storybook/react-vite'
import '../src/index.css'

const preview: Preview = {
  parameters: {
    backgrounds: {
      options: {
        canvas: { name: 'Canvas', value: '#F8FAFC' },
        panel: { name: 'Panel', value: '#FFFFFF' },
        ink: { name: 'Ink', value: '#111827' },
      },
    },
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
    layout: 'fullscreen',
  },
}

export default preview
