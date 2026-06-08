# Agora DB — Architecture v2.0

> **Version**: v2.0  
> **Date**: 2026-06-08  
> **Codename**: agoradb  
> **Positioning**: The Multimodal Columnar Data Lake for Web3 — Embedded, Sovereign, and P2P-Native  
> **Core Change**: Execution core migrated to Apache DataFusion for production-grade query optimization and vectorized execution. AgoraDB-specific differentiators (Space sovereignty, DID/UCAN, P2P, multimodal) remain as extensions around the DataFusion core.  
> **Status**: Phase A/B/D complete. Phase C (custom operators) deferred to Phase 3 (multimodal).

---

## v1 → v2 变更摘要

| Dimension | v1 (Self-Built) | v2 (DataFusion Core) |
|-----------|----------------|---------------------|
| SQL Parser | sqlparser directly | DataFusion `SessionContext` + `AgoraSQLParser` hooks |
| Analyzer | Self-built | DataFusion built-in |
| Logical Planner | Self-built | DataFusion built-in |
| Optimizer | ❌ None | ✅ DataFusion RBO + CBO |
| Physical Planner | Self-built | DataFusion built-in |
| Execution Engine | Self-built pipeline | DataFusion vectorized engine |
| Stage/Pipeline Abstraction | StageBuilder + PipelineBuilder | ❌ Removed (DataFusion executes directly) |
| Catalog Interface | Custom trait | `CatalogProvider` trait (bridge to Iceberg) |
| Storage Interface | Custom trait | `TableProvider` trait (bridge to Parquet) |
| Custom Operators | Custom StagePlan variants | **None in Phase 1/2** — all DataFusion built-in. Phase 3 (multimodal) may add VectorScan/GraphTraversal |
| WASM Strategy | Lean (~1-3MB) | Feature-gated (~5MB target for browser) |

**What stays the same**: Space/DID/UCAN, P2P networking, Iceberg Catalog, Parquet storage, VFS layer, multimodal concepts (GRAPH/VECTOR/FTS/BLOB).

---

## 1. Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 5: Interface Layer                                            │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────────────┐  │
│ │ SQL API  │ │Cypher API│ │ MCP API  │ │ JS/TS / HTTP / CLI     │  │
│ └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────────┬─────────────┘  │
├──────┼────────────┼────────────┼──────────────────┼────────────────┤
│ Layer 4: Query Layer (Federation-Aware)                             │
│                                                                     │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │ AgoraSessionContext (wraps DataFusion SessionContext)      │    │
│  │ ┌────────────┐ ┌────────────┐ ┌────────────┐              │    │
│  │ │ AgoraSQL   │ │ Cypher→DF  │ │ Federation │              │    │
│  │ │ Parser     │ │ Adapter    │ │ Planner    │              │    │
│  │ │ (hooks)    │ │ (Phase 3)  │ │            │              │    │
│  │ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘              │    │
│  │       └──────────────┼──────────────┘                      │    │
│  │                      ↓                                      │    │
│  │         DataFusion Logical Plan                             │    │
│  │              ↓ DataFusion Optimizer (RBO+CBO)              │    │
│  │         DataFusion Physical Plan                            │    │
│  │              ↓ DataFusion Execution Plan                    │    │
│  └────────────────────────────────────────────────────────────┘    │
│                      ↓                                              │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │ AgoraCatalogProvider (CatalogProvider trait)               │    │
│  │ ├─ Local Spaces → IcebergCatalog + Parquet TableProvider   │    │
│  │ ├─ Subscribed Spaces → Cached metadata                     │    │
│  │ └─ Remote Spaces → Arrow Flight metadata fetch             │    │
│  └────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 3: Execution Layer (Apache DataFusion)                        │
│ ┌──────────────────────────────────────────────────────────────┐    │
│ │ DataFusion Vectorized Engine (1024-row Arrow batches)        │    │
│ │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │    │
│ │ │LocalScan │ │RemoteScan│ │HashJoin  │ │Aggregate │        │    │
│ │ │(Parquet) │ │(Flight)  │ │(DF)      │ │(DF)      │        │    │
│ │ ├──────────┤ ├──────────┤ ├──────────┤ ├──────────┤        │    │
│ │ │VectorScan│ │GraphTra- │ │MergeJoin │ │Window    │        │    │
│ │ │(custom)  │ │versal    │ │(DF)      │ │(DF)      │        │    │
│ │ │          │ │(custom)  │ │          │ │          │        │    │
│ │ └──────────┘ └──────────┘ └──────────┘ └──────────┘        │    │
│ └──────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 2: Storage Engine (Iceberg + Parquet + Indexes)              │
│ ├─ Iceberg Metadata Layer (Schema Registry, Snapshot Log, Manifest) │
│ ├─ Parquet Column Store (Row Groups, Zone Map, Bloom Filter)       │
│ ├─ Auxiliary Indexes (ART, CSR, HNSW, Inverted)                    │
│ └─ Write Path + Maintenance (Append Buffer, Compaction Scheduler)  │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 1: VFS (Virtual File System)                                  │
│ ├─ LocalDiskVFS (file://)  ├─ S3VFS (s3://)  ├─ OPFSVFS / IDBVFS   │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 0: Security & P2P Networking                                  │
│ ├─ Peer Manager (DID + relationship state machine)                 │
│ ├─ Capability Manager (UCAN issue/verify/revoke/CRL)               │
│ ├─ Arrow Flight RPC (UCAN-authenticated, streaming)                │
│ └─ P2P Discovery (DHT / Relay / mDNS / WebRTC)                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. AgoraSessionContext — The Unified Entry Point

```rust
pub struct AgoraSessionContext {
    /// Underlying DataFusion session
    df_ctx: SessionContext,
    /// AgoraDB-specific catalog bridge
    catalog_provider: Arc<AgoraCatalogProvider>,
    /// Extension planner for custom operators
    extension_planner: Arc<AgoraExtensionPlanner>,
    /// DID / UCAN context for authorization
    auth_ctx: AuthContext,
}

impl AgoraSessionContext {
    /// Parse and execute SQL (standard + Agora extensions)
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        // 1. Detect Agora-specific syntax via AgoraSQLParser hooks
        // 2. Delegate to DataFusion for standard SQL
        // 3. Apply federation planning for remote Spaces
        // 4. Execute via DataFusion
    }
}
```

---

## 3. AgoraSQLParser — Custom Syntax Hooks

Follows DataFusion's "wrap, don't fork" philosophy:

```rust
pub struct AgoraSQLParser<'a> {
    df_parser: DFParser<'a>,
}

impl<'a> AgoraSQLParser<'a> {
    pub fn parse_statement(&mut self) -> Result<AgoraStatement> {
        // Detect Agora-specific syntax
        if self.peek_tokens(&[CREATE, SPACE]) {
            return self.parse_create_space();
        }
        // Delegate everything else to DataFusion
        Ok(AgoraStatement::DFStatement(Box::new(
            self.df_parser.parse_statement()?,
        )))
    }
}
```

**Agora-specific statements**:
- `CREATE SPACE name WITH STORAGE = '...', MODES = (...)`
- `GRANT UCAN ...`
- Future: `SET SPACE ...`, `SHOW PEERS`, etc.

---

## 4. Catalog Bridge — AgoraCatalogProvider

Bridges AgoraDB's Iceberg Catalog to DataFusion's `CatalogProvider` trait:

```rust
pub struct AgoraCatalogProvider {
    /// Local Iceberg catalog
    iceberg_catalog: Arc<AgoraCatalog>,
    /// Cache for subscribed remote Spaces
    remote_cache: DashMap<SpaceUri, Arc<dyn SchemaProvider>>,
    /// UCAN tokens for authorization
    ucan_tokens: DashMap<SpaceUri, UCAN>,
}

impl CatalogProvider for AgoraCatalogProvider {
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // 1. Try local Iceberg catalog
        // 2. Try subscribed remote cache
        // 3. Try fetching from remote via Arrow Flight (if UCAN authorized)
    }
}
```

---

## 5. Storage Bridge — IcebergTableProvider

Bridges Iceberg tables to DataFusion's `TableProvider`:

```rust
pub struct IcebergTableProvider {
    table: Table,
    snapshot_id: i64,
}

impl TableProvider for IcebergTableProvider {
    fn schema(&self) -> SchemaRef { ... }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. Use Iceberg's scan builder with predicate pushdown
        // 2. Apply projection pushdown
        // 3. Return DataFusion ParquetExec or custom ExecutionPlan
    }
}
```

---

## 6. WASM Strategy

DataFusion WASM ~10MB uncompressed. Target optimization:

```toml
[features]
default = ["server"]

# Full node / Edge node: complete feature set
server = [
    "datafusion/full",
    "p2p",
    "remote-exec",
]

# Browser node: lean feature set
browser = [
    "datafusion/sql",
    "datafusion/parquet",
    # Explicitly excluded:
    # - distributed execution
    # - complex aggregate functions (approx_percentile, etc.)
    # - JSON/struct functions (if not needed)
    # - avro/arrow-ipc
]
```

| Target | WASM Size | Features |
|--------|----------|----------|
| Server / Edge | ~10MB | Full DataFusion |
| Browser Node | **Target < 5MB** | SQL + Parquet + basic ops only |

**Long-term**: Contribute feature flags to upstream DataFusion (GitHub #16554) to reduce WASM size for the entire community.

---

## 8. Code Migration Statistics

| Metric | Value |
|--------|-------|
| **Old code deleted** | **~5,702 lines** (Parser, Analyzer, Planner, Executor, StageBuilder, Pipeline) |
| **New code added** | **~600 lines** (AgoraCatalogProvider, IcebergTableProvider, AgoraSessionContext, AgoraSQLParser) |
| **Net reduction** | **~5,100 lines** |
| **Files deleted** | **36 files** (15 source + 7 test + 14 module files) |
| **Tests passing** | **31 tests** (0 failures) |

### Deleted modules
- `crates/agoradb-query/src/logical/plan.rs`, `analyzer.rs`, `mod.rs`
- `crates/agoradb-query/src/physical/plan.rs`, `planner.rs`, `mod.rs`
- `crates/agoradb-query/src/execution_plan.rs`, `parser.rs`, `explain.rs`
- `crates/agoradb-execution/src/scheduler.rs`, `worker_pool.rs`, `pipeline.rs`, `pipeline_builder.rs`, `executor/mod.rs`
- All related test files (planner_test, parser_test, analyzer_test, executor_test, runner_test, local_exchange_pipeline_test, sql_pipeline_test)

---

## 9. Migration Plan

### Phase A: Foundation ✅ COMPLETE
- ~~Add `datafusion` dependency~~ ✅ DataFusion 51 + sqlparser 0.59
- ~~Implement `AgoraCatalogProvider`~~ ✅ (bridge Iceberg → DataFusion)
- ~~Implement `IcebergTableProvider`~~ ✅ (bridge Parquet → DataFusion)
- ~~Verify: DataFusion can read Iceberg tables~~ ✅ Integration test passes

### Phase B: SQL Migration ✅ COMPLETE
- ~~Replace self-built Parser with `AgoraSessionContext`~~ ✅
- ~~Implement `AgoraSQLParser` with `CREATE SPACE` hook~~ ✅
- ~~Implement CREATE SPACE execution~~ ✅ (catalog.create_namespace)
- ~~Fix sqlparser 0.54 → 0.59 compatibility~~ ✅

### Phase C: Custom Operators ⏭️ DEFERRED to Phase 3
- **Decision**: Phase 1/2 uses only DataFusion built-in operators
- RemoteScan handled inside TableProvider::scan (no custom ExecutionPlan needed)
- VectorScan / GraphTraversal deferred until GRAPH/VECTOR modes are implemented

### Phase D: Cleanup + Benchmark ✅ COMPLETE
- ~~Remove deprecated self-built code~~ ✅ **5,702 lines deleted**
- Run performance benchmark vs self-built engine → **TODO** (after write path)
- WASM compilation verification + feature flag tuning → **TODO** (Phase 2b)

### Phase E: Cypher + Advanced (Phase 3)
- Cypher parser → DataFusion Logical Plan adapter
- Full multimodal operator set (GRAPH, VECTOR, FTS)
- **Custom operators (ExtensionPlanner) introduced here only if needed**

### Phase F: Production Hardening
- TPC-H benchmark: Q1-Q5 on SF1
- Write path: Append Buffer → Parquet → Iceberg commit
- Federation: Arrow Flight server + RemoteScan
- Security: DID + UCAN implementation

---

## 10. Next Steps (Immediate)

1. **Write path**: Append Buffer → Parquet flush → Iceberg atomic commit
2. **INSERT SQL**: Parser → LogicalPlan → TableProvider insert
3. **TPC-H validation**: Run Q1-Q5 on SF1 dataset via DataFusion
4. **Arrow Flight Server**: gRPC server for federated queries

---

## 11. Risk Assessment

| Risk | Mitigation |
|------|-----------|
| WASM size exceeds target | Feature flags + upstream contribution + accept Server/Edge only if Browser fails |
| DataFusion design constraints | Use ExtensionPlanner + custom logical nodes; avoid forking |
| Migration breaks existing tests | Maintain both branches until feat/datafusion-core stabilizes |
| Performance regression | Benchmark at Phase D; keep self-built branch as fallback |

---

## 12. Branch Strategy

```
main                    ← Self-built engine (preserved)
  │
  └── feat/datafusion-core  ← DataFusion migration (active development)
       │
       └── (future) merge to main when stable
```

- `main`: Preserves v1 self-built code. Receives critical bug fixes.
- `feat/datafusion-core`: Active DataFusion migration. All new development.
- Merge decision: After Phase D benchmarks prove DataFusion outperforms self-built.

---

*This document is the canonical architecture baseline for AgoraDB v2.0. All engineering implementation proceeds from this specification.*
