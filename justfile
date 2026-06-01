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

# Run fmt, clippy, and test
check: fmt clippy test

# Run the nodusd server
run:
    cargo run --bin nodus_server