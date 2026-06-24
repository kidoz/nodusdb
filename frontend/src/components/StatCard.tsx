import type { StatusTone } from './brand'

type StatCardProps = {
  label: string
  value: string | number
  detail?: string
  tone?: StatusTone
  /** Tints the whole card — use for the single most important state. */
  emphasis?: boolean
}

export function StatCard({
  label,
  value,
  detail,
  tone = 'neutral',
  emphasis = false,
}: StatCardProps) {
  const className = [
    'stat-card',
    `stat-card--${tone}`,
    emphasis ? 'stat-card--emphasis' : '',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <article className={className}>
      <p>{label}</p>
      <strong>{value}</strong>
      {detail ? <span>{detail}</span> : null}
    </article>
  )
}
