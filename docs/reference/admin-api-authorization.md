# Admin API authorization

Every route under `/api/v1/` is authenticated, mapped to a privilege, and — when
it mutates state — audited. The liveness endpoints (`/healthz`, `/readyz`) and
`/metrics` are outside this path.

## Authentication schemes

| Scheme | Header | Resolves to |
| --- | --- | --- |
| Admin token | `Authorization: Bearer <admin.token>` | The `nodus` superuser principal. The token is compared in constant time. |
| Database credentials | `Authorization: Basic <base64(user:password)>` | The authenticated database user. |
| None | *(no header)* | `401 Unauthorized`, unless `[admin] token` is unset **and** `[admin] allow_insecure = true`, in which case the request runs as the `nodus` superuser. |

The anonymous fallback is deliberately narrow: an unauthenticated request is
never silently elevated to superuser just because it arrived on loopback. A
configuration that binds the admin port beyond loopback without a token is
rejected at startup unless `allow_insecure` is set explicitly.

An invalid token, an undecodable Basic header, or credentials that fail
authentication all return `401 Unauthorized`.

## Internal Raft routes (`/raft/...`)

The inter-node Raft endpoints (consensus RPCs, forwarded writes, read-index
probes) are authenticated too, because a forwarded write can carry shard-map
and transaction commands:

- When `cluster.raft_listen_addr` is **unset** (the default), the Raft routes
  share the public HTTP listener and require the admin bearer token — the same
  credential as `/api/v1/*`. Internode RPCs and forwarded writes attach it
  automatically. With no token configured, anonymous Raft access is possible
  only under the same explicit `[admin] allow_insecure = true` opt-in that
  opens the admin API; otherwise every Raft request is refused with `401`.
- When `cluster.raft_listen_addr` **is** set, the dedicated peer listener
  speaks mandatory client-certificate mTLS (`cluster.tls`), and the
  cluster-CA-signed certificate is the peer credential — no bearer token is
  required there, so rolling upgrades that mix with older binaries (which
  present certificates but no token) keep working. A dedicated listener
  *without* `cluster.tls` falls back to the bearer-token requirement above.

The configured bootstrap password (`admin.password`) is an operator credential
for `nodus`, shared by SQL SCRAM authentication and HTTP Basic authentication.
New logins resolve the current catalog identity only if it is an active global
user named `nodus` with a direct `ALL` grant on `System`. This allows a node to
keep accepting bootstrap logins after a Raft snapshot replaces its locally
created administrator ID with the cluster's ID. Login never creates that user
or restores its grants. Removing the user or revoking that grant disables new
bootstrap password logins while the server is running; the existing startup
bootstrap still creates/grants the administrator when the server restarts.

Ordinary passwords remain bound to immutable principal IDs. Deleting a user or
replacing it with a same-name user does not transfer its password. Existing SQL
sessions also keep their original IDs and subsequent privileged operations must
pass authorization against the current catalog. Reconnect after snapshot identity
replacement. These rules do not change the separate admin-token mechanism.

## Route privileges

| Route prefix | Required action |
| --- | --- |
| `/api/v1/sessions` | `ManageSessions` |
| `/api/v1/audit` | `ReadAudit` |
| `/api/v1/authz/explain` | `ReadAudit` |
| `/api/v1/queries` | `ReadAudit` |
| `/api/v1/roles` | `ManageGrants` |
| `/api/v1/grants` | `ManageGrants` |
| `/api/v1/backups` | `ManageBackups` |
| `/api/v1/import` | `ManageBackups` |
| `/api/v1/upgrade` | `ManageUpgrades` |
| `/api/v1/shards` | `ManageShards` |
| `/api/v1/catalog` | `ManageShards` |
| `/api/v1/node` | `ManageNode` |
| `/api/v1/cluster` | `ManageCluster` |
| any other `/api/v1/` path | — request is refused with `403 Forbidden` |

Authorization is deny-by-default: an unmapped path has no action to check, so it
is refused rather than allowed.

## Status codes

| Code | Meaning |
| --- | --- |
| `401 Unauthorized` | No credentials, or credentials that failed authentication. |
| `403 Forbidden` | Authenticated, but the principal lacks the required action — or the path maps to no known action. |
| `500 Internal Server Error` | The authorization engine itself failed. |

## Auditing

Every mutating request — anything other than `GET`, `HEAD`, or `OPTIONS` —
records an audit event carrying the actor, the action, the method and path, and
the result. Denials are audited as `Denied` with the reason, so a rejected
privileged request is as visible as a successful one.

## Cluster membership: join, remove, drain

Membership mutations live under `/api/v1/cluster` (`ManageCluster`) and are
audited like every other mutation.

- `POST /api/v1/cluster/join` — adds a node (`node_id`, `raft_advertise_addr`)
  to the meta group. Idempotent; retryable `503` until the meta leader is
  reachable.
- `POST /api/v1/cluster/remove` — expels a permanently dead node (`node_id`)
  from cluster membership: the meta group's voter set shrinks with
  `retain = false` (the roster reconcile reads, so the node is never folded
  back in), and every data group this node leads is shrunk in the same call;
  groups led by other live nodes converge on their leaders' next reconcile
  pass. The target is never contacted, so a dead node cannot block its own
  expulsion. Guards: `404` for a node that is not a member, `409` when asked to
  remove the node serving the request (issue the removal against a surviving
  peer), when it would remove the last meta voter, or while an upgrade has
  frozen the roster (`Idle` phase required); `503` when this node is not the
  meta leader. Removal does **not** move data — shard placements are unchanged,
  so shards whose primary was the removed node keep serving through their
  failover leaders.
- `POST /api/v1/node/drain` (`ManageNode`) — for a *live* node: stops new
  traffic, cancels sessions, and transfers leadership off the node, but keeps
  its membership. Drain a live node before decommissioning it; remove a node
  only when it is gone for good.

`nodusctl cluster remove-node --id <N>` maps to the remove endpoint (like the
rest of `nodus_cli`, it sends no `Authorization` header — see below).

## Client support

`nodus_cli` sends no `Authorization` header. Its `/api/v1/` subcommands
(`backup`, `import`, `shard`, `role`, `grant`, `session`, `audit`, `upgrade`,
`node`, `queries`, `cluster`) therefore return `401 Unauthorized` against any
server with an admin token configured. `nodus_cli health` works because
`/healthz` is not part of the authorized surface. Use `curl` with an explicit
header meanwhile.
