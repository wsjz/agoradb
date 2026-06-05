# AgoraDB Integration Test Data

This directory contains fixed test datasets used by integration tests.
All data is generated deterministically so every test run uses the exact
same Parquet files and Iceberg catalog metadata.

## Directory Layout

```
tests/
├── Cargo.toml                          # agoradb-tests crate (integration test framework)
├── src/
│   ├── lib.rs
│   └── framework/                      # Shared test utilities
│       ├── catalog.rs                  # Catalog setup helpers
│       ├── schema.rs                   # TPC-H schema definitions
│       └── runner.rs                   # Full pipeline runner
├── tests/
│   └── sql_pipeline_test.rs            # End-to-end SQL integration tests
├── sql-tests/                          # Reserved for convention-based SQL tests
│   └── README.md
├── tools/
│   └── data-gen/                       # Test data generator
│       ├── Cargo.toml
│       └── src/main.rs
└── agora-local/                        # Generated test data (gitignored)
    └── default/
        ├── .namespace.properties
        ├── region/
        │   ├── data/
        │   └── metadata/
        ├── nation/
        │   ├── data/
        │   └── metadata/
        ├── customer/
        │   ├── data/
        │   └── metadata/
        ├── orders/
        │   ├── data/
        │   └── metadata/
        └── lineitem/
            ├── data/
            └── metadata/
```

## Generating Test Data

```bash
# Default: SF=0.1 (~76K rows, fast)
cargo run -p test-data-gen --bin generate-test-data

# Force regeneration (overwrite existing data)
cargo run -p test-data-gen --bin generate-test-data -- --force

# Larger dataset: SF=0.1 (~760K rows)
cargo run -p test-data-gen --bin generate-test-data -- --scale-factor 0.1 --force

# Custom output directory
cargo run -p test-data-gen --bin generate-test-data -- --scale-factor 0.1 --output-dir /tmp/agora-test
```

## Scale Factor Reference

| SF     | region | nation | customer | orders  | lineitem | Total     |
|--------|--------|--------|----------|---------|----------|-----------|
| 0.001  | 5      | 25     | 15       | 150     | ~600     | ~795      |
| 0.01   | 5      | 25     | 150      | 1,500   | ~6,000   | ~7,680    |
| 0.1    | 5      | 25     | 1,500    | 15,000  | ~60,000  | ~76,530   |
| 1.0    | 5      | 25     | 15,000   | 150,000 | ~600,000 | ~765,030  |

Row group size is fixed at 1024 rows, so `lineitem` at SF=0.1 produces
~60 row groups (enough to exercise morsel-level parallelism).

## Tables

All tables follow the TPC-H schema:

| Table      | Primary Key      | Description                |
|------------|------------------|----------------------------|
| `region`   | `r_regionkey`    | 5 regions (Africa, etc.)   |
| `nation`   | `n_nationkey`    | 25 nations                 |
| `customer` | `c_custkey`      | Customers with demographics|
| `orders`   | `o_orderkey`     | Purchase orders            |
| `lineitem` | `(l_orderkey,l_linenumber)` | Order line items  |

## Deterministic Generation

Data is generated with fixed-seed PRNGs (seed per table).
Running the generator with the same `--scale-factor` always produces
bit-identical output, so tests are reproducible across machines and time.

## Running Integration Tests

```bash
# Run all integration tests
cargo test -p agoradb-tests

# Run specific test
cargo test -p agoradb-tests --test sql_pipeline_test
```

## Manual Inspection

You can inspect the generated Parquet files with any parquet reader:

```bash
# Python + pyarrow
python -c "import pyarrow.parquet as pq; print(pq.read_table('tests/agora-local/default/lineitem/data/*.parquet').to_pandas())"

# DuckDB
duckdb -c "SELECT COUNT(*) FROM 'tests/agora-local/default/lineitem/data/*.parquet'"
```
