import { BrandMark } from './BrandMark'
import { PermissionExplorer } from './PermissionExplorer'
import { StatCard } from './StatCard'
import { StatusBadge } from './StatusBadge'

export interface ClusterOverview {
  cluster_status: string
  nodes_live: number
  nodes_total: number
  shards_total: number
  shards_unavailable: number
  qps: number
  active_alerts: number
}

type DashboardProps = {
  overview: ClusterOverview | null
  loading?: boolean
  error?: string | null
}

export function Dashboard({
  overview,
  loading = false,
  error = null,
}: DashboardProps) {
  const clusterTone =
    overview?.cluster_status === 'Healthy' ? 'healthy' : 'degraded'
  const shardTone =
    overview && overview.shards_unavailable > 0 ? 'critical' : 'neutral'
  const alertTone =
    overview && overview.active_alerts > 0 ? 'critical' : 'healthy'

  return (
    <div className="app-shell">
      <header className="topbar">
        <BrandMark />
        <nav aria-label="Primary">
          <a href="#overview">Overview</a>
          <a href="#permissions">Permissions</a>
          <a href="#audit">Audit</a>
        </nav>
      </header>

      <main>
        <section className="hero-panel" id="overview">
          <div>
            <StatusBadge
              label={overview?.cluster_status ?? 'Connecting'}
              tone={overview ? clusterTone : 'neutral'}
            />
            <h1>PostgreSQL-wire operations for distributed SQL</h1>
            <p>
              A control surface for cluster health, shard readiness, query
              activity, and access review.
            </p>
          </div>
          <div className="hero-meter" aria-label="Cluster readiness summary">
            <span>{overview ? `${overview.nodes_live}/${overview.nodes_total}` : '-'}</span>
            <small>live nodes</small>
          </div>
        </section>

        {loading && !overview ? (
          <div className="notice notice--loading">Loading cluster overview...</div>
        ) : null}

        {error ? (
          <div className="notice notice--error">
            <strong>Error connecting to NodusDB cluster:</strong> {error}
          </div>
        ) : null}

        {overview ? (
          <section className="metrics-grid" aria-label="Cluster metrics">
            <StatCard
              label="Cluster status"
              value={overview.cluster_status}
              tone={clusterTone}
            />
            <StatCard
              label="Nodes"
              value={`${overview.nodes_live} / ${overview.nodes_total}`}
              detail="live / total"
            />
            <StatCard
              label="Shards available"
              value={`${overview.shards_total - overview.shards_unavailable} / ${overview.shards_total}`}
              tone={shardTone}
            />
            <StatCard
              label="Queries per second"
              value={overview.qps.toFixed(1)}
              detail="current sample"
            />
            <StatCard
              label="Active alerts"
              value={overview.active_alerts}
              tone={alertTone}
            />
          </section>
        ) : null}

        <div id="permissions">
          <PermissionExplorer />
        </div>
      </main>
    </div>
  )
}
