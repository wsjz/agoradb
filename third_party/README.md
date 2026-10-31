# Third-party code

## datafusion-federation 0.5.5

- Source: https://crates.io/crates/datafusion-federation/0.5.5
  (https://github.com/datafusion-contrib/datafusion-federation), Apache License 2.0.
- Used through `[patch.crates-io]` in the workspace `Cargo.toml`.
- Change: `[dependencies.datafusion]` in `Cargo.toml` sets
  `default-features = false`. Version 0.5.5 depends on DataFusion with its
  default features, which forces the C-backed `compression` and `parquet`
  features on for every crate and blocks the wasm32 build (architecture v3
  §10). 0.5.7 makes the same change upstream but requires DataFusion 55 /
  Arrow 59, which the pinned DuckDB and Iceberg crates do not support yet.
  No source file is modified.
- Drop this copy once the workspace moves to DataFusion 55 and
  datafusion-federation ≥ 0.5.7.
