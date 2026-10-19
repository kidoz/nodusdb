# Multi-stage build for the NodusDB server, CLI, and web console.
FROM node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5 AS frontend-builder
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release --bin nodus_server --bin nodus_cli

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gosu \
    && rm -rf /var/lib/apt/lists/*

# Separate data, backup, and audit directories (operational default).
RUN groupadd --system --gid 10001 nodus \
    && useradd --system --uid 10001 --gid nodus --home-dir /var/lib/nodus --shell /usr/sbin/nologin nodus \
    && mkdir -p /var/lib/nodus/data /var/lib/nodus/backups /var/log/nodus /etc/nodus \
    && chown -R nodus:nodus /var/lib/nodus /var/log/nodus

COPY --from=builder /src/target/release/nodus_server /usr/local/bin/nodus_server
COPY --from=builder /src/target/release/nodus_cli /usr/local/bin/nodusctl
COPY --from=frontend-builder /src/frontend/dist /frontend/dist
COPY deploy/nodus.example.toml /etc/nodus/nodus.toml
COPY deploy/docker/entrypoint.sh /usr/local/bin/nodus-entrypoint

ENV NODUS_CONFIG=/etc/nodus/nodus.toml
# Bind to all interfaces inside the container; restrict via published ports.
# Because the admin port is non-loopback, the server refuses to start unless a
# token (NODUS_ADMIN__TOKEN) or NODUS_ADMIN__ALLOW_INSECURE=true is provided at
# runtime. The bundled docker-compose sets a token; set one in production.
ENV NODUS_SERVER__PGWIRE_ADDR=0.0.0.0:5432
ENV NODUS_SERVER__HTTP_ADDR=0.0.0.0:8088

EXPOSE 5432 8088

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD nodusctl health --addr http://127.0.0.1:8088 || exit 1

ENTRYPOINT ["/usr/local/bin/nodus-entrypoint"]
CMD ["/usr/local/bin/nodus_server"]
