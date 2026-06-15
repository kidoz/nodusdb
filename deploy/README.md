# NodusDB deployment

> Early-stage scaffolding. Not yet production-hardened.

## Docker

```bash
docker build -f deploy/docker/Dockerfile -t nodusdb:dev .
docker run -p 5432:5432 -p 8088:8088 nodusdb:dev
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
helm install nodus deploy/helm/nodusdb
```

The chart renders a `StatefulSet` (configurable replicas), a headless `Service`,
a `ConfigMap` for `nodus.toml`, an optional TLS `Secret`, a `PodDisruptionBudget`,
and an optional Prometheus `ServiceMonitor`.

## systemd

```bash
sudo cp target/release/nodus_server /usr/local/bin/
sudo cp deploy/nodus.example.toml /etc/nodus/nodus.toml
sudo cp deploy/systemd/nodusd.service /etc/systemd/system/
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
