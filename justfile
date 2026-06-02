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

# Run product integration tests
test-integration:
    cargo test -p nodus_integration_tests

# Run PostgreSQL client and wire compatibility tests
test-compat:
    cargo test -p nodus_compatibility_tests

# Run SQL golden tests
test-sql:
    cargo test -p nodus_sqllogictest

# Run crash and fault-injection tests
test-fault:
    cargo test -p nodus_fault_tests

# Run all normal cross-crate test suites
test-cross: test-integration test-compat test-sql

# Run deterministic simulation tests
test-sim:
    RUSTFLAGS="--cfg madsim" cargo test -p nodus_distributed_tests --test sim_test -- --ignored

# Run loom model-checked concurrency tests for the transaction manager
test-loom:
    RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p nodus_txn --release loom_

# Run criterion benchmarks for the storage engines (btree + lsm)
bench:
    cargo bench -p nodus_storage_btree -p nodus_storage_lsm

# Build fuzz targets without running the fuzzers
fuzz-check:
    cargo check --manifest-path tests/fuzz/Cargo.toml

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
