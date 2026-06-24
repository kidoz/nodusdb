import { useEffect, useState } from 'react'
import { BrandMark } from './BrandMark'
import { NodeStrip } from './NodeStrip'
import { PermissionExplorer } from './PermissionExplorer'
import { StatCard } from './StatCard'
import { StatusStrip } from './StatusStrip'

export interface NodeInfo {
  id: string
  role: 'leader' | 'replica'
  shards: number
  healthy: boolean
}

export interface ClusterOverview {
  cluster_status: string
  nodes_live: number
  nodes_total: number
  shards_total: number
  shards_unavailable: number
  qps: number
  active_alerts: number
  // Optional extended telemetry. The UI degrades gracefully when these
  // are absent, so the backend can add them incrementally.
  qps_delta_pct?: number
  p99_latency_ms?: number
  replication_lag_ms?: number
  nodes?: NodeInfo[]
}

type DashboardProps = {
  overview: ClusterOverview | null
  loading?: boolean
  error?: string | null
  /** When the overview was last fetched, used for the freshness indicator. */
  updatedAt?: Date | null
}

function useRelativeTime(updatedAt?: Date | null) {
  const [label, setLabel] = useState<string | null>(null)
  useEffect(() => {
    if (!updatedAt) {
      setLabel(null)
      return
    }
    // Recompute on a timer inside the effect so the (impure) clock read never
    // happens during render.
    const compute = () => {
      const secs = Math.max(
        0,
        Math.round((Date.now() - updatedAt.getTime()) / 1000),
      )
      if (secs < 2) return 'just now'
      if (secs < 60) return `${secs}s ago`
      return `${Math.round(secs / 60)}m ago`
    }
    setLabel(compute())
    const t = setInterval(() => setLabel(compute()), 1000)
    return () => clearInterval(t)
  }, [updatedAt])
  return label
}

export function Dashboard({
  overview,
  loading = false,
  error = null,
  updatedAt = null,
}: DashboardProps) {
  const relative = useRelativeTime(updatedAt)

  const clusterTone =
    overview?.cluster_status === 'Healthy' ? 'healthy' : 'degraded'
  const shardTone =
    overview && overview.shards_unavailable > 0 ? 'critical' : 'healthy'
  const alertTone =
    overview && overview.active_alerts > 0 ? 'critical' : 'healthy'

  return (
    <div className="app-shell">
      <header className="topbar">
        <BrandMark />
        <div className="topbar__right">
          {overview ? (
            <span className="freshness">
              <span className="freshness__dot" aria-hidden="true" />
              Live{relative ? ` · updated ${relative}` : ''}
            </span>
          ) : null}
          <nav aria-label="Primary">
            <a href="#overview" className="is-active">
              Overview
            </a>
            <a href="#permissions">Permissions</a>
            <a href="#audit">Audit</a>
          </nav>
        </div>
      </header>

      <main>
        {loading && !overview ? (
          <div className="notice notice--loading">
            Loading cluster overview...
          </div>
        ) : null}

        {error ? (
          <div className="notice notice--error">
            <strong>Error connecting to NodusDB cluster:</strong> {error}
          </div>
        ) : null}

        {overview ? (
          <>
            <section id="overview">
              <StatusStrip overview={overview} tone={clusterTone} />
            </section>

            <section className="metrics-grid" aria-label="Cluster metrics">
              <StatCard
                label="Queries / sec"
                value={overview.qps.toFixed(1)}
                detail={
                  overview.qps_delta_pct !== undefined
                    ? `${overview.qps_delta_pct >= 0 ? '▲' : '▼'} ${Math.abs(overview.qps_delta_pct).toFixed(1)}% vs 1h avg`
                    : 'current sample'
                }
                tone={
                  overview.qps_delta_pct === undefined ? 'neutral' : 'healthy'
                }
              />
              <StatCard
                label="p99 latency"
                value={
                  overview.p99_latency_ms !== undefined
                    ? `${overview.p99_latency_ms.toFixed(1)}ms`
                    : '—'
                }
                detail="within SLO (< 10ms)"
                tone={
                  overview.p99_latency_ms !== undefined &&
                  overview.p99_latency_ms < 10
                    ? 'healthy'
                    : 'neutral'
                }
              />
              <StatCard
                label="Replication lag"
                value={
                  overview.replication_lag_ms !== undefined
                    ? `${overview.replication_lag_ms}ms`
                    : '—'
                }
                detail={`max across ${overview.nodes_total} nodes`}
                tone="healthy"
              />
              <StatCard
                label="Active alerts"
                value={overview.active_alerts}
                detail={
                  overview.active_alerts > 0
                    ? 'needs attention'
                    : 'all checks passing'
                }
                tone={alertTone}
                emphasis
              />
            </section>

            <NodeStrip overview={overview} shardTone={shardTone} />
          </>
        ) : null}

        <div id="permissions">
          <PermissionExplorer />
        </div>
      </main>
    </div>
  )
}
