# Multi-stage build for the NodusDB server and CLI.
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin nodus_server --bin nodus_cli

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Separate data and backup directories (operational default).
RUN mkdir -p /var/lib/nodus/data /var/lib/nodus/backups /etc/nodus

COPY --from=builder /src/target/release/nodus_server /usr/local/bin/nodus_server
COPY --from=builder /src/target/release/nodus_cli /usr/local/bin/nodusctl
COPY deploy/nodus.example.toml /etc/nodus/nodus.toml

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

ENTRYPOINT ["/usr/local/bin/nodus_server"]
