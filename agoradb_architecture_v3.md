# Agora DB — Architecture v3.0

> **Version**: v3.0
> **Date**: 2026-09-27
> **Codename**: agoradb
> **Positioning**: A Decentralized Local Federated Query Semantic Layer — Embedded, Sovereign, and P2P-Native
> **Core Change**: AgoraDB no longer owns query *computation*. Computation is delegated to pluggable embedded engines (DuckDB for analytics, SQLite for transactions; browser-hosted engines in WASM). DataFusion is retained **only** as the federation coordinator that merges results across engines and nodes. AgoraDB's own value concentrates in the semantic layer (naming, policy, views), the federation layer (routing, pushdown, merge), identity/authorization (DID/UCAN) and P2P (discovery, snapshot sync, Arrow Flight).
> **Status**: 3.0-A (engines), 3.0-B (local federation), 3.0-C (semantic layer) and 3.0-D (publishing) implemented on `feat/v3-engines`. Supersedes v2.0.

---

## v2 → v3 Change Summary

| Dimension | v2 (DataFusion Core) | v3 (Semantic Layer + Pluggable Engines) |
|-----------|----------------------|------------------------------------------|
| Identity of the project | Multimodal columnar data lake | Decentralized federated query **semantic layer** |
| Computation | DataFusion executes everything | **Engines** compute: DuckDB (OLAP), SQLite (OLTP), host-provided (browser) |
| DataFusion role | Parser + optimizer + executor | **Federation coordinator only**: merges Arrow streams from engines and remote nodes |
| Storage model | Columnar-only (Iceberg + Parquet) | **Two storage classes**: analytical (Iceberg + Parquet) and transactional (SQLite file) |
| Space ↔ engine | Implicit (always DataFusion) | **Explicit per Space**: `KIND = analytical \| transactional` selects engine + storage class |
| Space ↔ path | 1 : 1 | **Space (logical) and Location (physical) are decoupled**; N Spaces may bind one Location, at most one writable |
| Custom operators / indexes | ART, CSR, HNSW, Cypher planned | **Dropped**. Multimodal delegated to engine extensions (DuckDB `vss`/`fts`/DuckPGQ, SQLite FTS5/`sqlite-vec`) |
| P2P sync unit | Iceberg snapshots | Iceberg snapshots (unchanged); transactional Spaces **publish** Parquet snapshots |
| Browser node | DataFusion compiled to WASM (~10MB, risky) | Rust core (catalog, identity, federation planner) in WASM; **engines provided by JS host** (duckdb-wasm, wa-sqlite on OPFS) |
| Engine bridging code | Self-written `IcebergTableProvider` | One generic `EngineSqlExecutor` implementing `datafusion-federation`'s `SQLExecutor` over any `QueryEngine`; engines own their connections |

**What stays the same**: Space sovereignty, single write master per Space, DID/UCAN, deny-by-default, Iceberg catalog for analytical Spaces, Parquet as the interchange/publication format, VFS (opendal), Arrow Flight + libp2p for the network layer.

**Rescinded v1 constraints**: #2 *Columnar-Only Storage* (SQLite is row-oriented) and the Layer 3 *self-built vectorized engine*.

---

## 1. Positioning

> **AgoraDB decides *who may see what*, *where the data lives* and *where a query is sent*. It does not decide *how to compute*.**

Analogy: AgoraDB is to embedded engines what a service mesh is to services. DuckDB and SQLite are excellent local engines but know nothing about identity, capability delegation, peers, or multi-Space namespaces. AgoraDB wraps them into a sovereign, federated, P2P-addressable data layer without re-implementing any of their query processing.

### 1.1 Design Principles (v3, Immutable)

1. **Compute is delegated.** AgoraDB never implements scan, join, aggregate, or index algorithms. If an engine cannot do something, we pick or configure an engine — we do not write an operator.
2. **Single Write Master per Space** (unchanged). Write delegation is explicit via UCAN.
3. **Space is the sovereignty unit; Location is the physical unit.** They are decoupled. Authorization, engine selection and sync policy attach to the Space; format and residency attach to the Location.
4. **Immutable files are the only thing that crosses the network.** P2P synchronization exchanges Iceberg snapshots (Parquet + manifests). Transactional state never replicates live; it is *published* as snapshots.
5. **Deny by default** (unchanged). No peer relationship → no catalog visibility → no query possibility. Unauthorized tables "do not exist".
6. **Engines are host-replaceable.** The `QueryEngine` contract must be implementable by a native Rust crate *or* by the embedding host (JS in the browser, FFI elsewhere).
7. **Format interoperability** (unchanged). Parquet / Arrow / Iceberg / SQLite files remain directly usable by external tools with zero export.

---

## 2. Layered Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ L5  Interface        SQL API · CLI · HTTP · WASM/JS binding · MCP     │
├──────────────────────────────────────────────────────────────────────┤
│ L4  Semantic Layer ★                                                  │
│     Unified namespace  space://<did>/<space>.<table>                  │
│     Views · Row/column policies (UCAN → SQL rewrite) · Enumeration    │
│     resistance · Statement classification (DDL / DML / TCL / query)   │
├──────────────────────────────────────────────────────────────────────┤
│ L3  Federation Layer ★  (DataFusion as coordinator)                   │
│     sqlparser → resolve table refs → per-Space subtrees               │
│     → EngineTableProvider (pushdown as engine-dialect SQL)            │
│     → RemoteTableProvider (Arrow Flight, query shipping)              │
│     → DataFusion merges Arrow streams (join / agg across Spaces)      │
├──────────────────────────────────────────────────────────────────────┤
│ L2  Engine Abstraction ★                                              │
│     QueryEngine trait · Capabilities · Dialect                        │
│     ├─ DuckDbEngine   (analytical; reads Parquet lists, .sqlite)      │
│     ├─ SqliteEngine   (transactional; DML + transactions)             │
│     └─ HostEngine     (browser: duckdb-wasm / wa-sqlite via JS)       │
├──────────────────────────────────────────────────────────────────────┤
│ L1  Catalog & Storage                                                 │
│     Space registry · Location registry · Iceberg metadata            │
│     Parquet write path (AppendBuffer → Parquet → Iceberg commit)      │
│     SQLite → Parquet publisher · Compaction · VFS (opendal)           │
├──────────────────────────────────────────────────────────────────────┤
│ L0  Identity & Network ★                                              │
│     DID · UCAN issue/verify/revoke · Peer state machine               │
│     libp2p discovery · Snapshot sync · Arrow Flight RPC               │
└──────────────────────────────────────────────────────────────────────┘
        ★ = AgoraDB core value.  Others are thin glue.
```

---

## 3. Core Abstractions

### 3.1 Space — Logical Sovereignty Unit

```rust
pub struct Space {
    pub uri: SpaceUri,              // space://did:agora:7xK9y/blog
    pub kind: SpaceKind,            // Analytical | Transactional
    pub engine: EngineKind,         // DuckDb | Sqlite | Host(String)   (default derived from kind)
    pub location: LocationId,       // binding to a physical Location
    pub access: AccessMode,         // Writable | ReadOnly
    pub owner: Did,                 // write master
    pub policies: Vec<PolicyId>,    // row/column policies (semantic layer)
    pub views: Vec<ViewDef>,        // named SQL views over this Space
    pub publish: Option<PublishPolicy>, // transactional only: how/when to emit Parquet snapshots
}

pub enum SpaceKind { Analytical, Transactional }
```

`CREATE SPACE` syntax (extends v2):

```sql
CREATE SPACE blog   WITH KIND = 'analytical',    STORAGE = 'disk';        -- Iceberg + Parquet, engine DuckDB
CREATE SPACE orders WITH KIND = 'transactional', STORAGE = 'disk';        -- orders.sqlite, engine SQLite
CREATE SPACE orders_ro WITH KIND = 'analytical', LOCATION = 'orders', ACCESS = 'readonly';
--  ↑ second Space over the *same* Location (orders.sqlite), served by DuckDB via sqlite_scanner
```

### 3.2 Location — Physical Residency

```rust
pub struct Location {
    pub id: LocationId,
    pub format: LocationFormat,      // IcebergParquet { metadata_root } | SqliteFile { path }
    pub vfs: VfsScheme,              // file:// | s3:// | opfs:// | idb://
    pub residency: Vec<Residency>,   // browser | disk | s3 (v1 storage strategy)
    pub writer: Option<SpaceUri>,    // at most ONE writable Space per Location
}
```

**Binding rules** (enforced by the catalog at `CREATE SPACE` / `ALTER SPACE`):

| Rule | Description |
|------|-------------|
| Format ↔ engine compatibility | The Space's engine must be able to read the Location's format (see §4.3 matrix). Violations are rejected at DDL time. |
| Single writer | `Location.writer` is `None` or exactly one Space. Any further binding must be `ACCESS = 'readonly'`. |
| Kind ↔ format | `Transactional` requires `SqliteFile`. `Analytical` accepts `IcebergParquet` or (read-only) `SqliteFile`. |
| Independent policies | Two Spaces over one Location carry independent UCAN scopes, policies and views. This is the intended way to expose the same data under different governance. |

### 3.3 Node Types

Unchanged from v1 (Full / Edge / Browser), with one refinement: the node type no longer changes *what engine code ships*; it changes *which `QueryEngine` implementations are registered*.

| Node | Registered engines | Coordinator |
|------|--------------------|-------------|
| Full / Edge | `DuckDbEngine`, `SqliteEngine` (native, bundled) | DataFusion (native) |
| Browser | `HostEngine(duckdb-wasm)`, `HostEngine(wa-sqlite)` | DataFusion (WASM, `sql` + minimal features; only merges Arrow, never scans files) |

### 3.4 Catalog

Per-node registry with three tables: **Spaces**, **Locations**, **Peers**. Plus per-analytical-Space Iceberg metadata (unchanged). New in v3: `policies`, `views`, `engine` and `publish` are catalog entities, versioned with the Space.

---

## 4. Engine Abstraction Layer (L2)

### 4.1 The `QueryEngine` Contract

```rust
#[async_trait]
pub trait QueryEngine: Send + Sync {
    fn kind(&self) -> EngineKind;
    fn dialect(&self) -> Dialect;                 // sqlparser dialect used for SQL generation
    fn capabilities(&self) -> Capabilities;

    /// Make a physical source visible to the engine under `name`.
    async fn attach(&self, name: &str, source: TableSource) -> Result<()>;
    async fn detach(&self, name: &str) -> Result<()>;

    /// Run a query; results stream back as Arrow.
    async fn query(&self, sql: &str, params: &[ScalarValue]) -> Result<SendableRecordBatchStream>;

    /// Run a statement without a result set (DML / DDL / TCL). Transactional engines only.
    async fn execute(&self, sql: &str, params: &[ScalarValue]) -> Result<u64>;
    async fn begin(&self) -> Result<TxHandle>;    // Err(Unsupported) for analytical engines
}

pub enum TableSource {
    ParquetFiles { files: Vec<VfsPath>, schema: SchemaRef },   // resolved from an Iceberg snapshot
    SqliteFile   { path: VfsPath, table: String },
    ArrowBatches { schema: SchemaRef, stream: SendableRecordBatchStream }, // remote results, temp tables
}

bitflags! Capabilities {
    OLAP, OLTP, TRANSACTIONS, READ_PARQUET, READ_SQLITE, ARROW_OUT,
    EXT_VECTOR, EXT_FTS, EXT_GRAPH, PUSHDOWN_JOIN, PUSHDOWN_AGG
}
```

### 4.2 Implementations

| Engine | Crate (license) | Notes |
|--------|-----------------|-------|
| `DuckDbEngine` | `duckdb` 1.10505 (MIT, `bundled`) | Parquet via `read_parquet([...])`; SQLite via `sqlite_scanner` extension; Arrow out natively. Extensions loaded on demand: `vss`, `fts`, `duckpgq`. |
| `SqliteEngine` | `rusqlite` 0.40 (MIT, `bundled`) | WAL mode; FTS5 built in; `sqlite-vec` loadable. Row results converted to Arrow batches (small result sets by design — OLTP). |
| `HostEngine` | wasm-bindgen bridge | The JS host implements the same contract; Arrow IPC over the boundary. |

Both native engines are `feature`-gated (`engine-duckdb`, `engine-sqlite`) so a node can be built with only what it needs. DuckDB `bundled` compile time (minutes) and binary size (~tens of MB) are accepted for Full/Edge nodes.

### 4.3 Format ↔ Engine Compatibility

| Location format | DuckDB | SQLite | DataFusion (coordinator) |
|-----------------|--------|--------|--------------------------|
| `IcebergParquet` | ✅ read/write (write via AgoraDB Parquet path, not engine) | ❌ | reads only *Arrow streams*, never files (by policy) |
| `SqliteFile` | ✅ read-only (`sqlite_scanner`) | ✅ read/write + transactions | same |

### 4.4 Statement Routing (Semantic Layer → Engine)

Every statement is classified before planning:

| Class | Target Space kind | Route |
|-------|-------------------|-------|
| DDL on Space (`CREATE SPACE`, `ALTER SPACE`, `GRANT`) | — | AgoraDB catalog, never an engine |
| DDL on table (`CREATE TABLE`) | analytical | Iceberg schema commit (AgoraDB) |
| DDL on table | transactional | SQLite engine `execute` + catalog mirror |
| DML (`INSERT/UPDATE/DELETE`) | transactional | **Directly** to `SqliteEngine`, bypassing DataFusion |
| DML (`INSERT`) | analytical | AppendBuffer → Parquet → Iceberg commit (v2 write path) |
| TCL (`BEGIN/COMMIT/ROLLBACK`) | transactional | `SqliteEngine::begin` — session holds the `TxHandle` |
| Query (`SELECT`) touching **one** local Space | any | **Pushed entirely** to that Space's engine; DataFusion is a pass-through |
| Query touching **N** Spaces / remote Spaces | any | Federation Layer (§5) |

Each transactional Space has one shared SQLite connection for autocommit statements. `BEGIN` gives the session a **private connection** to the Space's file, and until `COMMIT`/`ROLLBACK` every statement of that session on that Space, including federated queries, runs on it, so the session sees its own uncommitted rows and nobody else does. A writer on another connection waits up to 5 s for the write lock (`busy_timeout`); dropping a session with an open transaction rolls it back.

---

## 5. Federation Layer (L3)

### 5.1 Role of DataFusion

DataFusion is the **coordinator**, not the engine:

- It never reads Parquet or SQLite files directly. Every leaf of the plan is a `TableProvider` whose `scan` returns Arrow batches produced by an engine or a remote node.
- Its optimizer is used for one thing that matters: `datafusion-federation` groups the largest possible subtree per source and hands it to the source as *SQL in the source's dialect*. So a two-Space join becomes two pushed-down SQL statements plus one DataFusion `HashJoin` over two Arrow streams.
- Its execution runs only the residual operators (cross-source joins, final aggregates, ordering, limits).

```
SELECT o.customer, sum(o.amount), b.title
FROM   orders.orders o                     -- transactional Space, SQLite
JOIN   blog.posts  b ON b.author = o.customer  -- analytical Space, DuckDB
JOIN   space://did:x/pub.metrics m ON ...  -- remote Space, Arrow Flight
GROUP BY ...

DataFusion plan (after federation optimizer):
  Aggregate
   └─ HashJoin
       ├─ HashJoin
       │   ├─ SqliteScan  "SELECT customer, amount FROM orders WHERE ..."   ← SqliteEngine
       │   └─ DuckDbScan  "SELECT author, title FROM read_parquet([...])"   ← DuckDbEngine
       └─ FlightScan     "SELECT ... FROM metrics WHERE ..."                 ← remote node's engine
```

### 5.2 Providers

| Provider | Source | Backed by |
|----------|--------|-----------|
| `EngineSqlExecutor` + `AgoraRemoteTable` | local Space | `datafusion-federation` `SQLFederationProvider` over our `SQLExecutor`, which runs pushed-down SQL on the Space's `QueryEngine`. For analytical Spaces AgoraDB first resolves the pinned Iceberg snapshot to a file list and attaches it as a DuckDB view. `compute_context` = engine instance id, so all analytical Spaces (one shared DuckDB) federate as one source. |
| `RemoteTableProvider` | remote Space (query shipping) | Arrow Flight `DoGet` with UCAN in headers; implements `SQLExecutor` from `datafusion-federation` so subtrees push down to the remote node. |
| `SubscribedTableProvider` | subscribed Space (data shipping) | Local replica of a remote snapshot in VFS cache → attached to the local DuckDB like any analytical Space. |

### 5.3 Query Shipping vs Data Shipping

Decided per remote Space by the UCAN held:

| UCAN capability | Strategy | Behaviour |
|-----------------|----------|-----------|
| `space/query` | Query shipping | Sub-SQL sent to the owner node; only result rows return. Owner applies its own policies. |
| `space/read` | Data shipping | Snapshot files replicated to local VFS (subscription); executed locally. Offline-capable. |
| both | Planner choice | Prefer local replica if the local snapshot is at or ahead of the requested snapshot; else ship the query. |

### 5.4 Snapshot Pinning

A federated query pins one Iceberg snapshot ID per analytical Space (local or remote) at planning time and threads it through every provider, so all subtrees observe a consistent point in time. Transactional Spaces have no snapshot; they observe the latest committed WAL state at scan start.

---

## 6. Semantic Layer (L4)

This is where AgoraDB's "semantic" claim is cashed out. Everything here is a **SQL → SQL rewrite** over the `sqlparser` AST (`agoradb-semantic::rewrite`), before an engine or the federation planner sees the query. Both rewrites replace a relation reference `<space>.<rel> [AS a]` with a derived table `(<query>) AS a`, so whatever they add ends up *inside* the SQL an engine executes.

### 6.1 Statements

```sql
CREATE [OR REPLACE] VIEW blog.recent AS SELECT id, title FROM blog.posts WHERE NOT draft;
DROP VIEW [IF EXISTS] blog.recent;

GRANT SELECT [(id, title)] ON blog.posts TO alice, "did:key:z6Mk...";
REVOKE SELECT ON blog.posts FROM alice;

CREATE POLICY own ON orders.orders [FOR SELECT] TO alice, bob USING (customer = current_user);
DROP POLICY [IF EXISTS] own ON orders.orders;
```

Views, grants and policies live in the node registry (`.agora/views.json`, `grants.json`, `policies.json`) and are removed with their Space (and grants/policies with their view). Every definition is validated by running it with `LIMIT 0` before it is stored, so a view, a grant's column list or a policy predicate that references something missing is rejected up front; a view that would expand into itself is rejected as recursive.

### 6.2 Principals

`SessionConfig::principal` says who a session acts for. `None` is the node owner: full access, views inlined, no policies. `Some(p)` is a peer (a DID once 3.1 lands): **read-only** (any DDL, DML, TCL, GRANT or Space statement is `PermissionDenied`) and **deny-by-default**.

| Feature | Mechanism |
|---------|-----------|
| Unified namespace | `<space>.<table>`, or bare `<table>` qualified with the session's `SET SPACE`. CTE names are left alone. `space://did/...` addressing arrives with 3.1. |
| Views | Inlined at rewrite time, nested up to 16 levels. May span Spaces (then federated). They are the way to build a semantic model over several physical sources. |
| Column grants | Each referenced relation becomes `(SELECT <granted columns> FROM …)`, so `*` expands to granted columns only and any other column simply does not exist. |
| Row policies | Once a relation has any policy, a principal sees rows matching at least one policy naming it (`OR`, as PostgreSQL permissive policies), none if no policy names it. `current_user` is replaced by the principal. The predicate is inside the derived table, so it runs in the engine; `explain()` shows it in the pushed-down SQL. |
| View rights | Access control runs first, view inlining second: a principal needs a grant on the view it names, and the view body reads its base tables with the owner's rights (PostgreSQL's default). Granting a view does not expose the tables behind it. |
| Enumeration resistance | An ungranted relation, a missing relation and a relation in a missing Space all fail with the same `Table not found: <space>.<rel>`; `SET SPACE` fails with `Space not found` for a Space the principal has no grant in. |
| Dialect normalisation | User SQL is parsed with a generic dialect; each pushed-down subtree is re-emitted in the target engine's dialect by `datafusion-federation`'s unparser. |
| Statement classification | §4.4, plus `ViewDdl` and `Acl` classes for the statements above. |

Not yet: writes by principals (UCAN write delegation, 3.1), column-level `REVOKE`, `RESTRICTIVE` policies, policies on commands other than `SELECT`, view column lists and materialized views.

---

## 7. Catalog & Storage (L1)

### 7.1 Analytical Spaces (unchanged core)

Iceberg metadata + Parquet data files via VFS. Write path: `AppendBuffer → Parquet flush → Iceberg fast-append commit` (`agoradb-storage`). iceberg-rust 0.10 has no public overwrite action, so `AgoraCatalog::replace_data_files` produces `overwrite` snapshots itself (manifest + manifest list + optimistic metadata commit, retried on conflict); compaction and publishing both use it, so compaction no longer duplicates rows. **Read path**: the snapshot's data-file list is handed to DuckDB, not read by DataFusion `ParquetExec`.

### 7.2 Transactional Spaces (new)

- One SQLite database file per Space, WAL mode, at `Location.path`.
- Writes go straight to SQLite; AgoraDB does not intercept or log them.
- Table names come from the engine (`sqlite_master`). `CREATE/ALTER/DROP TABLE` pass through to SQLite.

### 7.3 Publishing: SQLite → Parquet snapshots

Transactional Spaces become visible to peers only through **published snapshots**:

```sql
PUBLISH SPACE orders [TABLES (orders, items)] [TO orders_published]
```

- The target defaults to `<space>_published`. It is created on first publish as an ordinary analytical Space marked `published_from = <space>`; only `PUBLISH SPACE` writes it (DML/DDL on it fail with `PublishedSpace`), while views, grants and policies on it work as on any Space. It participates in federation (and later P2P sync) like any other analytical Space.
- Every selected table is read inside **one read transaction on a private connection**: the publication is a consistent committed state of all tables, never includes uncommitted writes, and does not block or disturb open transactions.
- Each table gets **one new Iceberg `overwrite` snapshot** replacing its data (`AgoraCatalog::replace_data_files`), so the published table is an exact copy as of the publish, and every earlier publication remains readable at its snapshot. Commits are atomic per table, not across tables.
- A full publish (no `TABLES`) mirrors the source: tables dropped in SQLite are dropped from the target. A table whose SQLite schema changed is recreated with the new schema (its earlier snapshots are lost with it).
- `spawn_publisher(session, request, every)` republishes on an interval; failures are logged and retried at the next tick. Commit-count triggers and WAL-level replication (Litestream-style) are **out of scope for v3.0**.

### 7.4 VFS

Unchanged (`opendal`: fs, s3; OPFS/IDB for browser). One addition: DuckDB and SQLite need *real file paths* (or DuckDB `httpfs` URLs). The VFS therefore exposes `materialize(path) -> LocalPath` — for `file://` it is the identity; for `s3://` and OPFS it is a cache-backed download. DuckDB `httpfs` may be used directly for S3-resident Parquet when the node has credentials, bypassing the cache.

---

## 8. Multimodal via Engine Extensions

| Modality | DuckDB (analytical) | SQLite (transactional) | AgoraDB responsibility |
|----------|---------------------|------------------------|------------------------|
| VECTOR | `vss` extension (HNSW) | `sqlite-vec` | `Mode` registry entry; capability check; pass SQL through |
| FTS | `fts` extension | FTS5 | same |
| GRAPH | DuckPGQ (SQL/PGQ `MATCH`) | — | same; Cypher is **dropped** |
| BLOB | CID columns + gateway dereference | same | unchanged from v1 |

Indexes built by extensions are engine-local and **not synchronized**; a subscribing node rebuilds them from the Parquet snapshot. The `Mode` enum survives only as a declaration of which extensions a Space expects, used for capability checks at planning time.

---

## 9. Identity & Network (L0)

Unchanged in intent from v1 §8: DID (Ed25519), UCAN (issue/verify/revoke/CRL), peer state machine, libp2p discovery, Arrow Flight RPC. Two clarifications for v3:

- Arrow Flight `DoGet` carries **SQL in the receiver's semantic namespace**; the receiver runs it through its own semantic layer and engines. A node never executes SQL it did not rewrite itself.
- Snapshot sync exchanges only `IcebergParquet` Locations. SQLite files never leave a node.

---

## 10. Browser Node

```
 JS host                      WASM (Rust)
 ┌───────────────┐   IPC     ┌──────────────────────────────┐
 │ duckdb-wasm   │◄────────►│ HostEngine(duckdb)            │
 │ wa-sqlite/OPFS│◄────────►│ HostEngine(sqlite)            │
 │ libp2p-js /   │◄────────►│ catalog · identity · semantic │
 │ WebRTC        │          │ federation (DataFusion, lean) │
 └───────────────┘          └──────────────────────────────┘
```

DataFusion in WASM is built with `sql` only (no parquet, no object_store), because in v3 it never touches files — this is what makes the < 5MB target realistic. Heavy queries still delegate to a Full Node via `RemoteTableProvider` as in v1 §7.2.

---

## 11. Crate Layout & Migration from v2

```
crates/
├── agoradb-core            errors, SpaceUri, SpaceKind/EngineKind/AccessMode, CreateSpaceRequest   [done]
├── agoradb-vfs             opendal VFS (not wired yet; materialize() in 3.1)                        [kept]
├── agoradb-catalog         Iceberg Catalog + Space/Location registries (.agora/*.json) + resolve_table [done]
├── agoradb-storage         Parquet write path (keyed by TableIdent), compaction                     [done]
├── agoradb-engine          QueryEngine trait, TableSource, Capabilities, BlockingWorker             [done]
├── agoradb-engine-duckdb   DuckDbEngine (bundled DuckDB + parquet)                                  [done]
├── agoradb-engine-sqlite   SqliteEngine + SQLite rows → Arrow                                       [done]
├── agoradb-semantic        CREATE/DROP/SET SPACE parser, classification, view + access-control rewrites [done]
├── agoradb-federation      EngineSqlExecutor, AgoraRemoteTable, Coordinator                         [done]
├── agoradb-node            AgoraSession, EngineRegistry, routing, DDL/DML                           [done]
├── agoradb-identity        DID keys, UCAN issue/verify/revoke, peer FSM                             [3.1]
├── agoradb-network         Arrow Flight server/client, libp2p                                       [3.1]
└── agoradb-cli                                                                                      [later]
```

`agoradb-query` was deleted; its `CREATE SPACE` parser moved to `agoradb-semantic`.

### 11.1 Dependency Changes

The four Arrow-line crates must move together: DataFusion 55 and `datafusion-federation` ≥ 0.5.6 require Arrow 59, which DuckDB 1.10505 and Iceberg 0.10 do not support yet.

| Action | Crate | Version | License |
|--------|-------|---------|---------|
| Upgrade | `datafusion` | 51 → **54** | Apache-2.0 |
| Upgrade | `arrow*`, `parquet` | 57 → **58** | Apache-2.0 |
| Upgrade | `iceberg` | 0.9 → **0.10.1** | Apache-2.0 |
| Upgrade | `sqlparser` | 0.59 → **0.62** (same as DataFusion 54) | Apache-2.0 |
| Add | `datafusion-federation` | **=0.5.5** (`sql` feature) | Apache-2.0 |
| Add | `duckdb` | **=1.10505.0** (`bundled`, `parquet`, `appender-arrow`) | MIT |
| Add | `rusqlite` | **0.40** (`bundled`, `column_decltype`) | MIT |
| Remove | `datafusion-datasource/-common/-execution/-physical-plan` direct deps, `dashmap`, `memmap2` | — | — |

`datafusion-table-providers` was evaluated and **not** used: it brings its own connection pools (a second path to each engine besides `QueryEngine`) and pins an older `rusqlite`. Licenses are checked by `cargo deny check licenses` (`deny.toml`).

### 11.2 Code to Delete (v2 residue)

- `agoradb-core/src/stage.rs`, `operator.rs` — self-built execution plan types; nothing in v3 consumes them.
- `agoradb-catalog/src/datafusion_bridge.rs` (`IcebergTableProvider`, `AgoraCatalogProvider`), `datafusion_sink.rs`, `parquet_util.rs` (morsel reader), `scan_provider.rs` — replaced by engine providers.
- `agoradb-query/src/session.rs` — replaced by `agoradb-federation` + `agoradb-semantic`.

Roughly 1,100 of the current ~4,300 lines are engine-bridge code that goes away; VFS, storage write path, Iceberg catalog core and `SpaceUri` (~2,600 lines) carry over.

---

## 12. Roadmap (v3)

| Phase | Goal | Exit criterion |
|-------|------|----------------|
| **3.0-A Engines** ✅ | `agoradb-engine` trait; DuckDB + SQLite engines; Location/Space split in catalog; `CREATE SPACE ... KIND`; single-Space queries pushed entirely to the engine | TPC-H Q1–Q5 on an analytical Space via DuckDB; an OLTP smoke test (BEGIN/INSERT/COMMIT/SELECT) on a transactional Space |
| **3.0-B Federation** ✅ | DataFusion coordinator over `datafusion-federation` + `EngineSqlExecutor`; cross-Space join (Parquet ⋈ SQLite) locally; snapshot pinning | Two-Space join returns correct results; EXPLAIN shows two pushed-down SQL leaves |
| **3.0-C Semantic** ✅ | Views, column grants and row policies as SQL rewrites; principals read-only and deny-by-default; enumeration resistance | Policy test suite: a restricted DID sees rewritten `*` and injected predicates inside the engine SQL |
| **3.0-D Publish** ✅ | `PUBLISH SPACE` → `orders_published` analytical read-only Space; scheduled publisher | Publish → query via DuckDB matches SQLite source |
| **3.1 Network** | DID/UCAN, Arrow Flight `RemoteTableProvider`, libp2p discovery, snapshot subscription | Two Docker nodes: UCAN-gated federated query; revocation cuts access |
| **3.2 Browser** | WASM core + `HostEngine` bridge to duckdb-wasm / wa-sqlite | Browser node runs a local query and a federated query via a Full Node |
| **3.3 Multimodal** | Load `vss`/`fts`/DuckPGQ, FTS5/`sqlite-vec`; `Mode` capability checks | Vector + FTS query on each engine kind |

No phase skipping (AGENT.md §4.1).

---

## 13. Risks

| Risk | Mitigation |
|------|------------|
| `datafusion-federation` lags DataFusion releases (0.5.5 is the last for DF 54 / Arrow 58) | Pin `datafusion` / `datafusion-federation` / `duckdb` / `iceberg` together in `[workspace.dependencies]`; upgrade as a unit. Our adapter (`EngineSqlExecutor` + `AgoraRemoteTable`) is ~250 lines and can be forked if blocked. |
| DataFusion 54 unparser quirks in pushed-down SQL | Known: (1) a filter pushed below a table alias is qualified by the base name (`FROM t AS o WHERE t.x`) — fixed by the `AgoraRemoteTable` AST analyzer; (2) joining two same-named tables from different Spaces without aliases (`a.t JOIN b.t`) produces ambiguous `"t"` qualifiers — users must alias them. Both covered by tests. |
| Filters DataFusion pushes into `VirtualExecutionPlan` at execution time | `datafusion-federation` 0.5.5 reports them as pushed without adding them to the SQL; `EngineSqlExecutor::execute` evaluates them on each returned batch so results stay correct. |
| DuckDB `bundled` build time / binary size | Feature-gated; CI caches `libduckdb-sys`. Not shipped to browser at all. |
| Pushdown misses (residual operators execute in DataFusion over large Arrow streams) | `EXPLAIN` surfaces pushed vs residual; planner emits a warning when a leaf returns > N batches without pushdown. |
| Cross-engine type mapping (SQLite dynamic typing → Arrow) | Catalog stores declared Arrow schema per transactional table; `SqliteEngine` casts on the way out. |
| Two Spaces over one Location with divergent policies leak via timing/side-channels | Policies are applied per Space before pushdown; engines see only the rewritten SQL. Documented; audited in Phase 3.1 security review. |
| `datafusion-federation` unparser cannot express an engine-specific construct (e.g. `MATCH` in DuckPGQ) | Semantic layer marks such subtrees as *opaque* and pushes the original text verbatim to the single owning engine; opaque subtrees cannot span Spaces. |

---

## 14. Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-09-27 | Stop building computation; delegate to DuckDB / SQLite | Self-built OLAP is out of scope for a small team; differentiation is sovereignty + federation, not operators |
| 2026-09-27 | Keep DataFusion as coordinator only | Needed for cross-engine / cross-node merge; `datafusion-federation` already solves pushdown-as-SQL |
| 2026-09-27 | Engine bound per Space, not per table | Keeps authorization, sync and engine boundaries aligned |
| 2026-09-27 | Split Space (logical) from Location (physical); N:1 with a single writer | Allows the same data to be exposed under different engines/policies without duplicating files |
| 2026-09-27 | Transactional Spaces publish Parquet snapshots; no WAL replication in v3.0 | Keeps the P2P layer to one artefact type (immutable files) |
| 2026-09-27 | Multimodal via engine extensions; drop ART/CSR/HNSW/Cypher | Extensions exist and are maintained; custom indexes were the largest remaining scope |
| 2026-09-27 | Bridge engines with our own `SQLExecutor` instead of `datafusion-table-providers` | One path to each engine (`QueryEngine`), no second connection pool, no pinned old `rusqlite`; the same executor will wrap remote and browser-host engines |
| 2026-09-29 | Run the federation optimizer rule after `push_down_filter` | With the upstream default position, filters above a join stayed in DataFusion instead of reaching the engine SQL |
| 2026-09-30 | Overwrite snapshots produced by `AgoraCatalog` itself (`replace_data_files`) | iceberg-rust 0.10 only exposes fast-append; publishing needs replace semantics and compaction was duplicating rows |
| 2026-09-30 | One private SQLite connection per open transaction | A shared connection let one session's `ROLLBACK` discard other sessions' autocommit writes |
| 2026-09-30 | Grants and policies use PostgreSQL syntax (`GRANT SELECT (cols)`, `CREATE POLICY … USING`); views run with owner rights | Familiar syntax parsed by sqlparser as-is; owner-rights views are what makes a view a publishable semantic interface |

---

*This document is the canonical architecture baseline for AgoraDB v3.0. All engineering implementation proceeds from this specification. v1 and v2 remain for historical context; where they conflict with v3, v3 wins.*
