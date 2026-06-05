# SQL Convention Tests

This directory is reserved for future convention-based SQL integration tests.

## Planned Structure

```
sql-tests/
├── datasets/
│   ├── nation.data          # Raw tabular data
│   ├── nation.ddl           # CREATE TABLE statement
│   └── nation.revision      # Data version marker
└── testcases/
    ├── aggregate/
    │   ├── test_count.sql
    │   └── test_count.result
    ├── joins/
    │   ├── test_inner_join.sql
    │   └── test_inner_join.result
    └── tpch/
        ├── q01.sql
        └── q01.result
```

## Test Case Format

Each `.sql` file contains a query. Each `.result` file contains the expected
output with a metadata header:

```
-- delimiter: |; ignoreOrder: false; types: INTEGER|VARCHAR
1|ALGERIA
2|ARGENTINA
```

## Status

Not yet implemented. Currently all SQL integration tests are code-based and
live in `tests/tests/integration/`.
