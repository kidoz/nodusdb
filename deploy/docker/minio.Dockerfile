# MinIO no longer publishes its community registry image; build the reviewed
# source commit instead of replacing MinIO in the development stack.
FROM golang:1.25-bookworm@sha256:3b4a11519ad929d1e1d261a12cff056f0c85b735253d7d861346b9c6f8b36437 AS builder
ARG MINIO_COMMIT=9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
RUN go install github.com/minio/minio@${MINIO_COMMIT}

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 minio \
    && useradd --system --uid 10001 --gid minio --home-dir /data --shell /usr/sbin/nologin minio \
    && mkdir -p /data \
    && chown -R minio:minio /data
COPY --from=builder /go/bin/minio /usr/local/bin/minio
USER 10001
EXPOSE 9000 9001
ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data", "--console-address", ":9001"]
