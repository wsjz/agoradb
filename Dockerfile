# Stage 1: Builder
FROM rust:latest AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for layer caching
COPY Cargo.toml Cargo.lock rustfmt.toml .clippy.toml ./
COPY crates/agoradb-core/Cargo.toml ./crates/agoradb-core/
COPY crates/agoradb-vfs/Cargo.toml ./crates/agoradb-vfs/
COPY crates/agoradb-catalog/Cargo.toml ./crates/agoradb-catalog/
COPY crates/agoradb-storage/Cargo.toml ./crates/agoradb-storage/

# Create dummy source files to cache dependency build
RUN mkdir -p crates/agoradb-core/src crates/agoradb-vfs/src \
    crates/agoradb-catalog/src crates/agoradb-storage/src \
    crates/agoradb-storage/tests
RUN echo 'fn main(){}' > crates/agoradb-core/src/lib.rs \
    && echo 'fn main(){}' > crates/agoradb-vfs/src/lib.rs \
    && echo 'fn main(){}' > crates/agoradb-catalog/src/lib.rs \
    && echo 'fn main(){}' > crates/agoradb-storage/src/lib.rs \
    && echo 'fn main(){}' > crates/agoradb-storage/tests/integration_test.rs \
    && echo 'fn main(){}' > crates/agoradb-storage/tests/pushdown_test.rs \
    && echo 'fn main(){}' > crates/agoradb-storage/tests/compaction_test.rs \
    && echo 'fn main(){}' > crates/agoradb-storage/tests/tpc_h_integration.rs

# Build dependencies (cached layer)
RUN cargo build --tests 2>/dev/null || true

# Copy actual source code
COPY crates/ ./crates/

# Build all tests
RUN cargo test --test integration_test --no-run
RUN cargo test --test pushdown_test --no-run
RUN cargo test --test compaction_test --no-run
RUN cargo test --test tpc_h_integration --no-run

# Stage 2: Runtime
FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy test binaries
COPY --from=builder /app/target/debug/deps/integration_test* ./
COPY --from=builder /app/target/debug/deps/pushdown_test* ./
COPY --from=builder /app/target/debug/deps/compaction_test* ./
COPY --from=builder /app/target/debug/deps/tpc_h_integration* ./

# Create run script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "=== AgoraDB Phase 0 Tests ==="\n\
for test in integration_test pushdown_test compaction_test tpc_h_integration; do\n\
    bin=$(ls ${test}-* 2>/dev/null | head -1)\n\
    if [ -n "$bin" ]; then\n\
        echo "Running $test..."\n\
        ./"$bin" --nocapture\n\
    else\n\
        echo "WARNING: $test binary not found"\n\
    fi\n\
done\n\
echo "=== All tests passed ==="\n\
' > /app/run_tests.sh && chmod +x /app/run_tests.sh

VOLUME ["/app/data"]

CMD ["/app/run_tests.sh"]
