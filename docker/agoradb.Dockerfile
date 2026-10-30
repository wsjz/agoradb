# AgoraDB node image: the `agoradb` binary and the prebuilt libduckdb it
# links. Build from the repository root:
#
#   docker build -f docker/agoradb.Dockerfile -t agoradb .
#   docker run --rm -v agora-data:/data agoradb identity
#   docker run -d -v agora-data:/data -p 50051:50051 -p 4001:4001 agoradb \
#       serve --listen 0.0.0.0:50051 --p2p-listen /ip4/0.0.0.0/tcp/4001
#
# The test image is the Dockerfile at the repository root.

# Stage 1: build
FROM rust:bookworm AS builder

WORKDIR /app

# rusqlite `bundled` compiles C sources; DuckDB is downloaded prebuilt
# (DUCKDB_DOWNLOAD_LIB in .cargo/config.toml).
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config clang \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .clippy.toml ./
COPY .cargo ./.cargo
COPY crates ./crates
COPY tests ./tests

RUN cargo build --release --locked -p agoradb-server --bin agoradb \
    && mkdir -p /out \
    && cp target/release/agoradb /out/ \
    && cp target/duckdb-download/*/*/libduckdb.so /out/

# Stage 2: runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/agoradb /usr/local/bin/agoradb
COPY --from=builder /out/libduckdb.so /usr/local/lib/libduckdb.so
RUN ldconfig

# Node data: Spaces, registries and the node key (.agora/identity).
ENV AGORADB_ROOT=/data
VOLUME /data
# Arrow Flight (TLS) and libp2p discovery.
EXPOSE 50051 4001

ENTRYPOINT ["agoradb"]
CMD ["serve", "--listen", "0.0.0.0:50051", "--p2p-listen", "/ip4/0.0.0.0/tcp/4001"]
