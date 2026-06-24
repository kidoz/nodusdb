import type { StatusTone } from './brand'
import type { ClusterOverview, NodeInfo } from './Dashboard'

type NodeStripProps = {
  overview: ClusterOverview
  shardTone?: StatusTone
}

/**
 * Falls back to a synthesized roster when the backend does not yet return a
 * per-node array, so the strip renders something useful from nodes_live/total.
 */
function deriveNodes(overview: ClusterOverview): NodeInfo[] {
  if (overview.nodes && overview.nodes.length) return overview.nodes
  const total = overview.nodes_total
  const live = overview.nodes_live
  const perNode = total > 0 ? Math.round(overview.shards_total / total) : 0
  return Array.from({ length: total }, (_, i) => ({
    id: `node-${i + 1}`,
    role: i === 0 ? 'leader' : 'replica',
    shards: perNode,
    healthy: i < live,
  }))
}

export function NodeStrip({ overview }: NodeStripProps) {
  const nodes = deriveNodes(overview)

  return (
    <section className="node-strip" aria-label="Cluster nodes">
      <div className="node-strip__head">
        <p>Nodes · {overview.nodes_live} live</p>
        <span>
          {overview.shards_total} shards ·{' '}
          {Math.round(
            overview.shards_total / Math.max(1, overview.nodes_total),
          )}{' '}
          per node
        </span>
      </div>
      <div className="node-grid">
        {nodes.map((node) => (
          <div className="node" key={node.id}>
            <div className="node__top">
              <span
                className={`node__dot${node.healthy ? '' : ' node__dot--down'}`}
                aria-hidden="true"
              />
              <strong>{node.id}</strong>
            </div>
            <span>
              {node.role} · {node.shards} sh
            </span>
          </div>
        ))}
      </div>
    </section>
  )
}
