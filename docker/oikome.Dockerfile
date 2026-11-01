# Oikome node image: the `oikome` binary and the prebuilt libduckdb it
# links. Build from the repository root:
#
#   docker build -f docker/oikome.Dockerfile -t oikome .
#   docker run --rm -v oikome-data:/data oikome identity
#   docker run -d -v oikome-data:/data -p 50051:50051 -p 4001:4001 oikome \
#       serve --listen 0.0.0.0:50051 --p2p-listen /ip4/0.0.0.0/tcp/4001
#
# The test image is the Dockerfile at the repository root.

# Stage 1: build
FROM rust:bookworm AS builder

WORKDIR /app

# rusqlite `bundled` compiles C sources; DuckDB is downloaded prebuilt
# (DUCKDB_DOWNLOAD_LIB in .cargo/config.toml).
# cmake: protoc is built from source for the lance manifest reader.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config clang cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .clippy.toml ./
COPY .cargo ./.cargo
COPY crates ./crates
# Vendored crates patched in through [patch.crates-io] (third_party/README.md).
COPY third_party ./third_party
COPY tests ./tests

RUN cargo build --release --locked -p oikome-server --bin oikome \
    && mkdir -p /out \
    && cp target/release/oikome /out/ \
    && cp target/duckdb-download/*/*/libduckdb.so /out/

# The DuckDB extensions nodes load (sqlite, lance, duckpgq), signed by DuckDB.
RUN LD_LIBRARY_PATH=/out /out/oikome --root /tmp/node --extension-dir /out/extensions \
        extensions install

# Stage 2: runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/oikome /usr/local/bin/oikome
COPY --from=builder /out/libduckdb.so /usr/local/lib/libduckdb.so
COPY --from=builder /out/extensions /usr/local/lib/oikome/extensions
RUN ldconfig

# Node data: Spaces, registries and the node key (.oikome/identity).
ENV OIKOME_ROOT=/data
ENV OIKOME_EXTENSION_DIR=/usr/local/lib/oikome/extensions
VOLUME /data
# Arrow Flight (TLS) and libp2p discovery.
EXPOSE 50051 4001

ENTRYPOINT ["oikome"]
CMD ["serve", "--listen", "0.0.0.0:50051", "--p2p-listen", "/ip4/0.0.0.0/tcp/4001"]
