import type { StatusTone } from './brand'
import type { ClusterOverview } from './Dashboard'

type StatusStripProps = {
  overview: ClusterOverview
  tone?: StatusTone
}

const TITLE: Record<string, string> = {
  Healthy: 'Cluster Healthy',
  Degraded: 'Cluster Degraded',
  Critical: 'Cluster Critical',
}

export function StatusStrip({ overview, tone = 'healthy' }: StatusStripProps) {
  const shardsReady = overview.shards_total - overview.shards_unavailable
  const title = TITLE[overview.cluster_status] ?? overview.cluster_status
  const subline =
    overview.active_alerts > 0
      ? `${overview.active_alerts} active alert${overview.active_alerts === 1 ? '' : 's'}`
      : 'no active incidents'

  return (
    <section className={`status-strip status-strip--${tone}`}>
      <div className="status-strip__main">
        <span className="status-strip__dot" aria-hidden="true" />
        <div>
          <div className="status-strip__title">
            <strong>{title}</strong>
            <span>{subline}</span>
          </div>
          <p>PostgreSQL-wire distributed SQL · all replicas in sync</p>
        </div>
      </div>
      <div className="status-strip__vitals">
        <div className="vital">
          <strong>
            {overview.nodes_live}/{overview.nodes_total}
          </strong>
          <span>nodes live</span>
        </div>
        <div className="vital">
          <strong>
            {shardsReady}/{overview.shards_total}
          </strong>
          <span>shards ready</span>
        </div>
      </div>
    </section>
  )
}
