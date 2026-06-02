default: help

# Show available commands
help:
    @just --list

# Run rustfmt
fmt:
    cargo fmt --all

# Run clippy
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Run tests
test:
    cargo test --workspace

# Run deterministic simulation tests
test-sim:
    RUSTFLAGS="--cfg madsim" cargo test -p nodus_testkit --test sim_test -- --ignored

# Run fmt, clippy, and test
check: fmt clippy test

# Run the nodusd server
run:
    cargo run --bin nodus_server

# Build the Docker image
docker-build:
    docker build -f deploy/docker/Dockerfile -t nodusdb:dev .

# Bring up the local dev stack (nodusd + Prometheus + MinIO)
compose-up:
    docker compose -f deploy/docker-compose.yml up --build

# Tear down the local dev stack
compose-down:
    docker compose -f deploy/docker-compose.yml down