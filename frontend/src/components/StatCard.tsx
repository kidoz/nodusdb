import type { StatusTone } from './brand'

type StatCardProps = {
  label: string
  value: string | number
  detail?: string
  tone?: StatusTone
}

export function StatCard({
  label,
  value,
  detail,
  tone = 'neutral',
}: StatCardProps) {
  return (
    <article className={`stat-card stat-card--${tone}`}>
      <div>
        <p>{label}</p>
        <strong>{value}</strong>
      </div>
      {detail ? <span>{detail}</span> : null}
    </article>
  )
}
