import type { Meta, StoryObj } from '@storybook/react-vite'
import { BrandMark } from '../components/BrandMark'
import { brandColors, brandPrinciples } from '../components/brand'

function BrandStory() {
  return (
    <div className="brand-story">
      <section className="brand-story__section">
        <BrandMark />
        <div className="hero-panel">
          <div>
            <h1>NodusDB Console</h1>
            <p>
              Quiet operational UI for a PostgreSQL-wire-compatible distributed
              SQL database.
            </p>
          </div>
        </div>
      </section>

      <section className="brand-story__section">
        <h2>Color System</h2>
        <div className="brand-swatches">
          {brandColors.map((color) => (
            <article className="brand-swatch" key={color.name}>
              <div style={{ background: color.value }} />
              <dl>
                <dt>{color.name}</dt>
                <dd>{color.value}</dd>
                <dd>{color.usage}</dd>
              </dl>
            </article>
          ))}
        </div>
      </section>

      <section className="brand-story__section">
        <h2>Experience Principles</h2>
        <ul className="principle-list">
          {brandPrinciples.map((principle) => (
            <li key={principle}>{principle}</li>
          ))}
        </ul>
      </section>
    </div>
  )
}

const meta = {
  title: 'Brand/Foundation',
  component: BrandStory,
} satisfies Meta<typeof BrandStory>

export default meta
type Story = StoryObj<typeof meta>

export const Foundation: Story = {}
