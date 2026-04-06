default: check

init:
    git config core.hooksPath .githooks
    cargo fetch

build:
    cargo build --release

test:
    cargo test --workspace

lint:
    cargo clippy --workspace -- -D warnings

fmt:
    cargo fmt --all

check: fmt lint test
