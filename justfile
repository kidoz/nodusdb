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

# Audit SQL compatibility using only the Rust PostgreSQL driver (no Java/.NET)
test-compat-rust:
    cargo test --locked --no-fail-fast \
        -p nodus_compatibility_tests -p nodus_integration_tests -p nodus_sqllogictest \
        --test pg18_admin --test pg18_adv_types --test pg18_catalog \
        --test pg18_dql --test pg18_indexes --test pg18_schema_table \
        --test pg18_types_constraints --test pg18_views --test pgwire_smoke \
        --test scram_auth --test tls_handshake --test pg_client_coverage \
        --test run_slt --test rust_driver_sql

# Run SQL golden tests
test-sql:
    cargo test -p nodus_sqllogictest

# Run crash and fault-injection tests
test-fault:
    cargo test -p nodus_fault_tests

# Run all normal cross-crate test suites
test-cross: test-integration test-compat test-sql

# Run the Raft partition regression over real TCP (ordinary Tokio runtime)
test-partition:
    cargo test -p nodus_distributed_tests --test sim_test --locked -- --list | grep -Fx 'test_cluster_partition_linearizability: test'
    cargo test -p nodus_distributed_tests --test sim_test --locked

# Compatibility alias; this harness is not a deterministic simulation
test-sim: test-partition

# Run loom model-checked concurrency tests for the transaction manager
test-loom:
    RUSTFLAGS="--cfg loom" LOOM_MAX_PREEMPTIONS=3 cargo test -p nodus_txn --release loom_

# Run criterion benchmarks for the storage engine
bench:
    cargo bench -p nodus_storage_lsm

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

# Rebuild and rerun the root Docker Compose test stack
compose-rerun:
    docker compose -f compose.yaml down --remove-orphans
    docker compose -f compose.yaml up --build --force-recreate

# Build pinned old/new servers and run the process rolling-upgrade matrix
test-mixed-binary:
    python3 tools/testing/mixed_binary.py
