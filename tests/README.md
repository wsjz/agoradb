# AgoraDB Integration Tests

This crate (`agoradb-tests`) runs system-level tests against a fixed,
deterministically generated TPC-H dataset. Queries go through
`AgoraSession`, which pushes them down to DuckDB.

## Directory Layout

```
tests/
├── Cargo.toml                  # agoradb-tests crate
├── src/
│   ├── lib.rs
│   └── framework/
│       └── catalog.rs          # setup_catalog(), TPCH_SPACE, GENERATE_CMD
├── tests/
│   └── tpch_queries.rs         # TPC-H Q1–Q5 via AgoraSession (DuckDB)
├── sql-tests/                  # Reserved for convention-based SQL tests
├── tools/
│   └── data-gen/               # Test data generator (test-data-gen crate)
└── agora-local/                # Generated test data (gitignored)
    ├── .agora/
    │   ├── spaces.json         # Space registry: analytical Space "tpch"
    │   └── locations.json
    └── tpch/                   # Iceberg namespace of Space "tpch"
        ├── .namespace.properties
        ├── region/{data,metadata}/
        ├── nation/…  customer/…  orders/…  lineitem/…
        └── supplier/…  part/…  partsupp/…
```

## Generating Test Data

Run from the workspace root:

```bash
# What the TPC-H tests expect (SF 0.001, a few thousand rows)
cargo run -p test-data-gen --bin generate-test-data -- --scale-factor 0.001 --force

# Larger dataset
cargo run -p test-data-gen --bin generate-test-data -- --scale-factor 0.1 --force

# Custom output directory / Space name
cargo run -p test-data-gen --bin generate-test-data -- --output-dir /tmp/agora-test --space tpch
```

`--force` deletes both the Space's data and the `.agora/` registry in the
output directory; without it the generator refuses to overwrite existing data.

## Scale Factor Reference

| SF    | region | nation | customer | orders  | lineitem  | supplier | part    | partsupp |
|-------|--------|--------|----------|---------|-----------|----------|---------|----------|
| 0.001 | 5      | 25     | 15       | 150     | ~600      | 10       | 200     | 800      |
| 0.01  | 5      | 25     | 150      | 1,500   | ~6,000    | 100      | 2,000   | 8,000    |
| 0.1   | 5      | 25     | 1,500    | 15,000  | ~60,000   | 1,000    | 20,000  | 80,000   |
| 1.0   | 5      | 25     | 15,000   | 150,000 | ~600,000  | 10,000   | 200,000 | 800,000  |

Each 1024-row batch is flushed as its own Parquet file and Iceberg snapshot.

## Deterministic Generation

Every table uses a fixed-seed PRNG, so the same `--scale-factor` always
produces the same rows on every machine.

## Running Integration Tests

```bash
cargo test -p agoradb-tests
```

Tests address tables as `tpch.<table>` or, with the session's default Space
set to `tpch`, as bare `<table>`.

## Manual Inspection

The data files are plain Parquet:

```bash
duckdb -c "SELECT count(*) FROM 'tests/agora-local/tpch/lineitem/data/*.parquet'"
```
