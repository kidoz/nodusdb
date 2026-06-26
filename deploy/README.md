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
environment variables.

## Kubernetes (Helm)

```bash
helm install nodus deploy/helm/nodusdb \
  --set config.adminPassword='<replace-me>' \
  --set config.adminToken='<replace-me>'
```

The chart renders a `StatefulSet`, a headless `Service`, a `ConfigMap` for
non-secret `nodus.toml` settings, an admin credential `Secret`, an optional TLS
`Secret`, a `PodDisruptionBudget`, and an optional Prometheus `ServiceMonitor`.

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
