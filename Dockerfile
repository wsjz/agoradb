# AgoraDB test image: builds the workspace and runs the full test suite,
# including TPC-H Q1-Q5 against freshly generated SF 0.001 data.
#
#   docker build -t agoradb-tests .
#   docker run --rm agoradb-tests

# Stage 1: Builder
FROM rust:latest AS builder

WORKDIR /app

# duckdb / rusqlite `bundled` compile C and C++ sources.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev clang \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .clippy.toml ./
COPY .cargo ./.cargo
COPY crates ./crates
COPY tests ./tests

# Build every test binary (the DuckDB C++ build dominates: several minutes).
RUN cargo test --workspace --no-run

# Generate the TPC-H fixture used by tests/tests/tpch_queries.rs.
RUN cargo run -p test-data-gen --bin generate-test-data -- --scale-factor 0.001 --force

CMD ["cargo", "test", "--workspace"]
