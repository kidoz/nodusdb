# NodusDB deployment

> Early-stage scaffolding. Not yet production-hardened.

## Docker

```bash
docker build -f deploy/docker/Dockerfile -t nodusdb:dev .
docker run --rm \
  -e NODUS_ADMIN__PASSWORD=nodus \
  -e NODUS_ADMIN__TOKEN=nodus-dev-token \
  -p 5432:5432 -p 8088:8088 \
  nodusdb:dev
```

The production image builds the console with Node 24 and packages the static
bundle at `/frontend/dist`; the server serves it from `/` on the HTTP listener.
Build and smoke-test the image, authenticated admin behavior, UID 10001, a SQL
round trip, and every referenced console asset with:

```bash
just docker-smoke
```

## Local dev stack (Compose)

Runs one `nodusd` node, Prometheus, and MinIO (for backup testing):

```bash
docker compose -f deploy/docker-compose.yml up --build
```

- PostgreSQL wire protocol: `localhost:5432`
- Admin / metrics / console: `http://localhost:8088`
- Prometheus: `http://localhost:9090`
- MinIO console: `http://localhost:9001`
- Local test database user: `nodus`
- Local test database password: `nodus`
- Local test admin token: `nodus-dev-token`

The Compose stack mounts `deploy/nodus.example.toml` into the container and
overrides container bind addresses plus local-only test credentials through
environment variables. Its MinIO service builds the
[official source release](https://github.com/minio/minio/releases/tag/RELEASE.2025-10-15T17-29-55Z)
pinned in `deploy/docker/minio.Dockerfile`; no external MinIO image pull is
required.

## Kubernetes (Helm)

```bash
helm install nodus deploy/helm/nodusdb \
  --set config.adminPassword='<replace-me>' \
  --set config.adminToken='<replace-me>'
```

The chart renders a `StatefulSet`, a headless `Service`, a `ConfigMap` for
non-secret `nodus.toml` settings, an admin credential `Secret`, an optional TLS
`Secret`, a `PodDisruptionBudget`, and optional Prometheus `ServiceMonitor` and
`PrometheusRule` resources.

The safe default is one replica. `replicaCount > 1` starts additional nodes with
unique node IDs and seed-node join settings, but this is not production HA yet:
distributed data-shard execution is still partial.

If `config.adminPassword` or `config.adminToken` are omitted, Helm generates and
stores them in the admin `Secret`. Retrieve them with:

```bash
kubectl get secret nodus-nodusdb-admin \
  -o jsonpath='{.data.admin-password}' | base64 -d
kubectl get secret nodus-nodusdb-admin \
  -o jsonpath='{.data.admin-token}' | base64 -d
```

### Backup storage and recovery verification

For a filesystem repository, `backupPersistence.enabled=true` creates a backup
PVC separate from the data PVC and mounts it at `/var/lib/nodus/backups`. Size it
from retained full backups, incremental chains, archived WAL, and enough headroom
for one new full backup plus verification. A same-node filesystem backup is not
node-loss protection; replicate it off-node or use S3 for production.

Off-cluster S3 is the production default. Set a unique repository prefix per
cluster and inject credentials through an existing Kubernetes `Secret`; do not
put them in `nodus.toml`:

```yaml
config:
  backupRepositoryUri: s3://production-backups/cluster-a
backupPersistence:
  enabled: false
extraEnv:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: production-backup-s3
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: production-backup-s3
        key: secret-access-key
  - name: AWS_REGION
    value: us-east-1
  - name: AWS_ENDPOINT_URL
    value: https://s3.example.internal
```

Use TLS, encryption at rest, separate write/delete permissions, and immutable
retention such as Object Lock. Keep a full backup, every incremental in its
chain, and all WAL required through the latest allowed recovery point. Restored
nodes need a new empty repository prefix before normal archiving resumes.

Run the [full, incremental, and PITR restore drills](../docs/how-to/back-up-and-restore.md#run-restore-drills)
on a schedule and publish their `.prom` files through your metrics pipeline.
Enable `restoreDrillAlerts.enabled=true` to render missing, stale, and failed
restore-drill alerts; `staleAfterSeconds` defaults to 26 hours. Enabling the
rules without the drill metrics pipeline will correctly report missing evidence,
not a healthy backup.

## systemd

```bash
sudo cp target/release/nodus_server /usr/local/bin/
sudo cp deploy/nodus.example.toml /etc/nodus/nodus.toml
sudo cp deploy/systemd/nodusd.service /etc/systemd/system/
sudo id -u nodus >/dev/null 2>&1 || \
  sudo useradd --system --home-dir /var/lib/nodus --shell /usr/sbin/nologin nodus
sudo systemctl enable --now nodusd
```

## Configuration

All settings in `deploy/nodus.example.toml` are overridable via `NODUS_`-prefixed
environment variables (double underscore for nesting), e.g.
`NODUS_SERVER__HTTP_ADDR=0.0.0.0:8088`.

For host-local development outside Docker, use the repository-root
`nodus.toml.example`:

```bash
NODUS_CONFIG=nodus.toml.example cargo run --bin nodus_server
```
