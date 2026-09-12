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

## Client support

`nodus_cli` sends no `Authorization` header. Its `/api/v1/` subcommands
(`backup`, `import`, `shard`, `role`, `grant`, `session`, `audit`, `upgrade`,
`node`, `queries`, `cluster`) therefore return `401 Unauthorized` against any
server with an admin token configured. `nodus_cli health` works because
`/healthz` is not part of the authorized surface. Use `curl` with an explicit
header meanwhile.
