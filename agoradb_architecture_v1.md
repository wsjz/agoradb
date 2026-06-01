# Agora DB — Architecture & Module Design Document

> **Version**: v1.0  
> **Codename**: agoradb  
> **Positioning**: The Multimodal Columnar Data Lake for Web3 — Embedded, Sovereign, and P2P-Native  
> **Core Philosophy**: Every node is an autonomous data-sovereignty unit; the Space is the fundamental boundary of data ownership. Structured tables, property graphs, and vector embeddings coexist within a single Space.

---

## 1. Terminology & Core Abstractions

| Term | Definition |
|------|------------|
| **Space** | A sovereignty unit containing a collection of tables, property graphs, and vector collections — all backed by a unified Iceberg catalog. E.g. `space://did:agora:abc123/blog` |
| **Node** | A peer entity in the network running the Agora engine. Three types: Full Node, Edge Node, Browser Node |
| **Catalog** | The local metadata registry on each node, recording all Spaces visible to that node |
| **Snapshot** | An immutable Iceberg snapshot — a complete file manifest of a Space at a point in time |
| **Manifest** | Points to a set of Parquet data files, including partition info and per-column statistics |
| **Field ID** | An immutable numeric column identifier in Iceberg; enables safe schema evolution |
| **UCAN** | A capability token (JWT) for fine-grained authorization, chainable via delegation |
| **DID** | Decentralized identity based on Ed25519; the root of trust for all access control |
| **Mode** | A data modality within a Space: `TABLE` (SQL), `GRAPH` (Cypher), `VECTOR` (ANN search), `FTS` (full-text), `BLOB` (external content via CID) |

---

## 2. Design Principles (Immutable Constraints)

1. **Single Write Master per Space** — The Space creator is the default write master. Other nodes hold read-only replicas. Write delegation is explicit via UCAN. This eliminates distributed consensus overhead for 95%+ of Web3 use cases.
2. **Columnar-Only Storage** — No row-store engine. All modalities (graph, vector, text) are ultimately stored in Parquet files with appropriate auxiliary indexes.
3. **Space Autonomy** — Each Space has an independent Iceberg Catalog. Nodes synchronize via Snapshot exchange.
4. **Browser Nodes Are Lightweight Peers** — They hold full Catalog capability but delegate heavy operations (compaction, large joins) to Full Nodes via Remote Execution.
5. **Security by Design** — No peer relationship = no Catalog visibility = no query possibility. Deny-by-default at every layer.
6. **Format Interoperability** — Parquet + Arrow IPC + Iceberg are native formats. Zero export/import to use DuckDB, Pandas, Spark, or DataFusion.
7. **Multimodal Coexistence** — A single Space can contain SQL tables, property graphs, vector collections, full-text indexes, and external BLOB references. Cross-modality queries (e.g. vector similarity → graph traversal → SQL filter) are first-class.

---

## 3. Overall Layered Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 5: Interface Layer                                            │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────────────────┐  │
│ │ SQL API  │ │Cypher API│ │ MCP API  │ │ JS/TS / HTTP / CLI     │  │
│ └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────────┬─────────────┘  │
├──────┼────────────┼────────────┼──────────────────┼────────────────┤
│ Layer 4: Query Layer (Federation-Aware)                             │
│ ┌────────┐ ┌────────┐ ┌──────────┐ ┌────────────┐ ┌──────────┐     │
│ │ Parser │ │Analyzer│ │ Logical  │ │ Optimizer  │ │ Physical │     │
│ │(SQL/   │ │(Multimodal│ │ Planner  │ │ (RBO +   │ │ Planner  │     │
│ │ Cypher)│ │ Binding) │ │          │ │ Cardinality│ │          │     │
│ └────────┘ └────────┘ └──────────┘ └─────┬──────┘ └────┬─────┘     │
│                                           │              │           │
│  ┌────────────────────────────────────────┘              │           │
│  │ Federation Planner (Post-Optimizer)                  │           │
│  │ - Remote/local Space identification via Catalog      │           │
│  │ - Query fragmentation across nodes                   │           │
│  │ - Pushdown decisions (predicate/projection/agg)      │           │
│  │ - UCAN-constrained optimization                      │           │
│  └────────────────────────────────────────────────────────┘           │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 3: Execution Layer                                            │
│ ┌──────────────────────────────────────────────────────────────┐    │
│ │ Vectorized Engine (1024-row batches, SIMD where available)   │    │
│ │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │    │
│ │ │LocalScan │ │RemoteScan│ │HashJoin  │ │MergeJoin │        │    │
│ │ │(Parquet) │ │(Flight)  │ │          │ │          │        │    │
│ │ ├──────────┤ ├──────────┤ ├──────────┤ ├──────────┤        │    │
│ │ │VectorScan│ │GraphTra- │ │Aggregate│ │Filter/   │        │    │
│ │ │(HNSW)    │ │versal    │ │(SIMD)    │ │Project   │        │    │
│ │ ├──────────┤ ├──────────┤ ├──────────┤ ├──────────┤        │    │
│ │ │FTSScan   │ │Hybrid    │ │RemoteExec│ │Unnest/   │        │    │
│ │ │(Inverted)│ │Scan      │ │(Delegate)│ │JSONPath  │        │    │
│ │ └──────────┘ └──────────┘ └──────────┘ └──────────┘        │    │
│ └──────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 2: Storage Engine Layer                                       │
│ ┌──────────────────────────────────────────────────────────────┐    │
│ │ Iceberg Metadata Layer (per Space)                           │    │
│ │ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────────┐  │    │
│ │ │ Schema   │ │ Snapshot │ │ Manifest │ │ Mode Registry   │  │    │
│ │ │ Registry │ │ Log      │ │ List     │ │ (TABLE/GRAPH/   │  │    │
│ │ │          │ │          │ │          │ │  VECTOR/FTS/    │  │    │
│ │ │          │ │          │ │          │ │  BLOB)          │  │    │
│ │ └──────────┘ └──────────┘ └──────────┘ └─────────────────┘  │    │
│ ├──────────────────────────────────────────────────────────────┤    │
│ │ Parquet Column Store + Auxiliary Indexes                     │    │
│ │ ┌────────────────────────┐  ┌────────────────────────────┐   │    │
│ │ │ Row Groups / Chunks    │  │ Zone Map / Bloom Filter    │   │    │
│ │ │ Dictionary / RLE / ZSTD│  │ ART Primary Key Index      │   │    │
│ │ │ Nested Types (Struct/  │  │ CSR Graph Index            │   │    │
│ │ │  List / Map)           │  │ HNSW Vector Index          │   │    │
│ │ │                        │  │ Inverted FTS Index         │   │    │
│ │ │                        │  │ JSON Path Index            │   │    │
│ │ └────────────────────────┘  └────────────────────────────┘   │    │
│ ├──────────────────────────────────────────────────────────────┤    │
│ │ Write Path + Maintenance                                     │    │
│ │ ┌──────────────────────────┐  ┌──────────────────────────┐   │    │
│ │ │ Append Buffer → CoW      │  │ Compaction Scheduler      │   │    │
│ │ │ → Atomic Snapshot Commit │  │ (Sort-Merge / Index Rebuild│   │    │
│ │ │                          │  │ / Expire Old Snapshots)    │   │    │
│ │ └──────────────────────────┘  └──────────────────────────┘   │    │
│ │                                                              │    │
│ │ Update Support: Iceberg Delete Files (Position / Equality)   │    │
│ └──────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 1: Platform Adaptation Layer                                  │
│ ┌──────────────────────────────────────────────────────────────┐    │
│ │ Unified VFS                                                  │    │
│ │ ┌──────────────┐ ┌──────────────┐ ┌──────────────────────┐   │    │
│ │ │ LocalDiskVFS │ │ S3VFS        │ │ OPFSVFS / IDBVFS     │   │    │
│ │ │ (NAS/Server) │ │ (Object)     │ │ (Browser)            │   │    │
│ │ └──────────────┘ └──────────────┘ └──────────────────────┘   │    │
│ └──────────────────────────────────────────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────┤
│ Layer 0: Security & P2P Networking Layer                            │
│ ┌──────────────────────────────────────────────────────────────┐    │
│ │ Peer Manager (DID + relationship state machine)              │    │
│ │ Capability Manager (UCAN issue/verify/revoke/CRL)            │    │
│ │ Arrow Flight RPC (UCAN-authenticated, streaming)             │    │
│ │ P2P Discovery (DHT / Relay / mDNS / WebRTC)                  │    │
│ └──────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Core Abstractions

### 4.1 Space — The Sovereignty Unit

A Space is the atomic unit of data ownership. It contains one or more **Modes** (data modalities), each backed by the same Iceberg Catalog but with modality-specific indexing.

**Naming**: `space://<owner-did>/<space-name>`  
**Example**: `space://did:agora:7xK9y/blog`

**Storage Strategy** (declared at creation, mutable only by owner):

| Strategy | Physical Residency | Typical Use Case |
|----------|-------------------|-----------------|
| `browser` | OPFS only | Private notes, temporary analysis, public computers |
| `disk` | Local disk / NAS | Home server, Docker, VPS |
| `s3` | S3-compatible only | Public datasets, cloud archival |
| `browser,disk` | OPFS + local disk | Personal blog multi-device sync, offline-first |
| `browser,s3` | OPFS + S3 | No-NAS users: primary + backup, cross-device sync |
| `disk,s3` | Local disk + S3 | Hot data local, historical archive in cloud |
| `browser,disk,s3` | All three | Full multi-device disaster recovery |

**Authorization Boundary**: UCAN tokens grant capabilities at the Space level, with optional refinement to:
- Specific Mode(s) within the Space
- Column-level allowlists
- Row-level predicate constraints (e.g. `WHERE owner = did:agora:...`)

**Write Model**: Single-master by default. The Space creator is the Write Master. Other nodes hold read replicas. Write delegation is explicit: the creator issues a UCAN with `att: [{ "with": "space://...", "can": "space/write" }]` to another DID. This avoids distributed consensus complexity.

### 4.2 Node Types

| Type | Host | Storage | Compute | Role |
|------|------|---------|---------|------|
| **Full Node** | NAS / Server / VPS | Local disk + S3 | Multi-core, 16GB+ RAM | Authoritative storage, compaction service, P2P routing, remote execution delegate |
| **Edge Node** | Desktop / Laptop | Local disk (limited) | Multi-core, 8GB+ RAM | Query serving, P2P direct connect, can delegate heavy ops to Full Node |
| **Browser Node** | Browser (WASM) | OPFS / IndexedDB | Single-threaded WASM, ~2-4GB RAM limit | Lightweight sovereign peer: local catalog, local queries, delegates compaction and large joins to Full Node |

**Key Design**: Browser Nodes hold a **full Iceberg Catalog** for their subscribed Spaces. They can answer queries independently using locally cached data files. For operations exceeding their capacity (large HashJoin, Compaction, HNSW rebuild), they delegate to a designated Full Node via the `RemoteExec` operator — but the delegation is authenticated via UCAN, and the result is streamed back as Arrow batches.

### 4.3 Catalog

Per-node local metadata registry recording:
- **Local Spaces**: Spaces created by this node (this node is the Write Master)
- **Subscribed Spaces**: Spaces replicated via P2P sync (read-only replicas)
- **Authorized Remote Spaces**: Spaces for which this node holds a UCAN but has no local data (federated query targets)

Each Catalog entry includes:
- Space URI and metadata
- Storage strategy and VFS backend mapping
- Latest known Snapshot ID per subscribed Space
- UCAN tokens held for authorization proof
- Peer node IDs that can serve this Space (for RemoteExec delegation)

### 4.4 Mode — The Multimodal Registry

Each Space declares which modalities it contains. The Mode Registry is part of the Iceberg Catalog:

| Mode | Stored As | Primary Index | Query Interface | Use Case |
|------|-----------|---------------|-----------------|----------|
| `TABLE` | Parquet files | Zone Map, Bloom, ART | SQL | Structured analytics, time-series |
| `GRAPH` | Parquet edge files + vertex dictionaries | CSR index | Cypher | Social graphs, knowledge graphs, on-chain transaction graphs |
| `VECTOR` | Parquet vector columns | HNSW index | Vector similarity (cosine, L2, IP) | Embeddings, semantic search, AI RAG |
| `FTS` | Tokenized term-document postings | Inverted index | `MATCH (text) AGAINST (query)` | Content search, log analysis, document retrieval |
| `BLOB` | CID references in Parquet columns | None (external) | Dereference via gateway | IPFS/Arweave content referenced but not stored locally |

**Cross-Modality Queries**: The query planner can push results from one modality into another:
```sql
-- Example: Vector similarity → Graph traversal → SQL filter
SELECT u.name, u.did, similarity(v.embedding, :query_vec) as score
FROM users u
JOIN (SELECT * FROM user_embeddings ORDER BY embedding <=> :query_vec LIMIT 100) v
  ON u.id = v.user_id
JOIN user_graph g ON u.id = g.source_id
WHERE g.relationship = 'follows'
  AND u.created_at > '2025-01-01'
ORDER BY score DESC;
```

---

## 5. Multimodal Storage Engine (Layer 2)

### 5.1 Iceberg Metadata Layer

**Schema Registry**:
- Tracks schema history per Mode within a Space
- Field IDs are immutable and Mode-scoped
- Schema evolution operations: ADD column, DROP column (soft), RENAME column, REORDER columns, WIDEN type (int → bigint, float → double, decimal precision extension)
- **Nested type support**: STRUCT, LIST, MAP for JSON/semi-structured data

**Snapshot Log**:
- Every write or compaction generates a new immutable Snapshot
- Atomic switch via Iceberg metadata commit
- Time-travel queries supported: `SELECT * FROM table TIMESTAMP AS OF '2025-01-01'`
- Snapshot expiration policy: configurable, default retain last 7 days + hourly for last 24h

**Mode Registry**:
- Stored in Iceberg Catalog properties
- Each Mode entry: name, type, schema reference, index status, last compaction timestamp

### 5.2 Parquet Column Store

- **Universal data file format**: All modalities store data as Parquet
- **Row Group**: Default 128MB (configurable per Space)
- **Column Chunk**: Per-column compression (Dictionary / RLE / ZSTD)
- **Statistics**: Min / Max / Null Count / Distinct Count per Column Chunk
- **Sort Key**: Declared via `ORDER BY (col1, col2)` at table creation; compaction enforces sort order to optimize Zone Map pruning for time-range queries
- **Nested types**: Parquet's native struct/list/map support enables JSON documents without separate document store

### 5.3 Modality-Specific Storage Details

#### TABLE Mode (Standard SQL Tables)
Standard Parquet columnar storage with Zone Map, Bloom Filter, and optional ART primary key index. Supports all Iceberg types including nested STRUCT/LIST/MAP.

#### GRAPH Mode (Property Graphs)
- **Edge table**: A Parquet table with required columns `(source_id, target_id, edge_type, properties_json)`
- **Vertex dictionary**: A compressed mapping from vertex ID to internal integer ID (enables CSR construction)
- **CSR Index**: Built during compaction; stored as auxiliary binary files (not Parquet, but rebuildable from edge table). Enables O(degree) outbound traversal.
- **Cypher translation**: `MATCH (a:User)-[:FOLLOWS]->(b:User)` translates to a CSR traversal followed by vertex property lookups.

#### VECTOR Mode (Vector Collections)
- **Vector table**: Parquet table with columns `(id, vector FLOAT[N], metadata_json)`
- **HNSW Index**: Built during compaction; stored as auxiliary binary files. Supports cosine similarity, L2 distance, and inner product.
- **Vector dimension**: Up to 4096 dimensions (configurable)
- **Quantization**: Optional scalar quantization (float32 → int8) for memory-constrained environments

#### FTS Mode (Full-Text Search)
- **Document table**: Parquet table with columns `(doc_id, content TEXT, metadata_json, lang)`
- **Inverted Index**: Built during compaction; term → (doc_id, position_list, tf-idf) postings stored as compressed auxiliary files
- **Tokenization**: Configurable per-column (Unicode word boundaries + language-specific stemmers)
- **Relevance scoring**: BM25 ranking with optional vector reranking (hybrid search)

#### BLOB Mode (External Content References)
- **CID reference column**: Parquet column storing content identifiers (`ipfs://Qm...`, `ar://...`, `https://...`)
- **Metadata columns**: MIME type, size, checksum
- **Dereference**: Lazy loading via configured gateway (local IPFS node, HTTP gateway, or Arweave gateway)
- **Pin tracking**: Optional integration with local IPFS node for automatic pinning of referenced CIDs

### 5.4 Write Path

Append-only + Copy-on-Write for read-heavy analytical workloads:

1. **Append Buffer**: Writes enter an in-memory micro-batch (flush triggered by row count threshold ~10K rows or time threshold ~30s)
2. **Flush**: Generates a small unsorted Parquet file (the Sort Key is not enforced at this stage)
3. **Iceberg Commit**: Atomically appends the new file to the current Snapshot via metadata commit
4. **Return Success**: The new Snapshot is immediately queryable
5. **Background Compaction**: Asynchronously merges small files, enforces Sort Key, rebuilds all auxiliary indexes

**Browser Node Exception**: Browser Nodes do not run local compaction. They buffer writes to OPFS and periodically delegate compaction to a Full Node via `RemoteExec`. The Full Node performs the merge, rebuilds indexes, generates a new Snapshot, and the Browser Node pulls the updated metadata.

### 5.5 Update & Delete Support (Iceberg Delete Files)

No Merge-on-Read row store. Updates and deletes use Iceberg standard Delete Files:

| Mechanism | Format | Use Case |
|-----------|--------|----------|
| **Position Delete** | `(data_file_path, row_number)` | Bulk delete by position (e.g. purge old data) |
| **Equality Delete** | Primary key values or predicates | Row-level update/delete by key |

**Read-time behavior**: Base Parquet files are merged with Delete Files at scan time.  
**Compaction-time behavior**: Delete markers are physically applied; new "clean" files are generated.

### 5.6 Compaction Scheduler

Background tasks triggered by file count threshold, total size threshold, time window, or manual invocation:

| Task | Description | Browser Node |
|------|-------------|-------------|
| **Sort-Merge** | Merge small files, enforce Sort Key, generate larger files | Delegated to Full Node |
| **Statistics Update** | Recompute Zone Maps, column-level statistics | Delegated to Full Node |
| **Index Rebuild** | Rebuild ART, CSR, HNSW, Inverted indexes | Delegated to Full Node |
| **Snapshot Expiration** | Remove unreferenced old Snapshots and data files | Local (metadata only) |
| **S3 Backend Merge** | Download-merge-upload cycle for S3-backed files | N/A |

**Compaction for S3 backends**: Files are downloaded to local temp, merged, and uploaded as new files. Local disk acts as a buffer.

### 5.7 Auxiliary Indexes

All indexes are **non-primary path** — they can be rebuilt from Parquet files and are not required for basic query execution.

| Index | Modality | Build Trigger | Persistence |
|-------|----------|---------------|-------------|
| **Zone Map** | TABLE (auto) | Per flush + compaction | Embedded in Parquet footer |
| **Bloom Filter** | TABLE (optional) | Compaction | Separate binary file per column |
| **ART** | TABLE (optional) | Compaction | Memory-mapped binary file |
| **CSR** | GRAPH (required) | Compaction | Binary adjacency list + property offsets |
| **HNSW** | VECTOR (required) | Compaction | Binary graph layers + vector store |
| **Inverted** | FTS (required) | Compaction | Compressed term postings files |
| **JSON Path** | TABLE (optional) | Compaction | Path → Row Group bitmap index |

### 5.8 Unified VFS (Virtual File System)

Upper layers are agnostic to the physical storage backend:

| Implementation | Backend | Node Types | URI Prefix |
|----------------|---------|------------|------------|
| **LocalDiskVFS** | `std::fs` + `mmap` | Full / Edge | `file://` |
| **S3VFS** | S3-compatible API | Full / Edge | `s3://` |
| **OPFSVFS** | Origin Private File System | Browser | `opfs://` |
| **IDBVFS** | IndexedDB (object store fallback) | Browser | `idb://` |

**Browser S3 Access**: Browser Nodes do not embed S3 SDKs. They access S3 via:
1. **Presigned URLs**: Generated by the owning Full Node and passed via Arrow Flight
2. **Arrow Flight Proxy**: Full Node / Relay Node forwards range requests

---

## 6. Query Layer (Layer 4)

### 6.1 Standard Query Pipeline

| Module | Responsibility |
|--------|---------------|
| **Parser** | SQL and Cypher → AST. SQL is the primary interface; Cypher for GRAPH mode queries |
| **Analyzer** | Bind to Catalog, resolve table/column references, validate UCAN permissions. Unauthorized tables return "does not exist" (enumeration resistance) |
| **Logical Planner** | AST → Logical Plan (operator tree) |
| **Optimizer** | RBO: predicate pushdown, projection pruning, join reordering, constant folding. Cardinality estimates from Manifest statistics (no full CBO yet) |
| **Physical Planner** | Logical Plan → Physical Plan (scan type, join algorithm, index selection) |

### 6.2 Federation Planner

Inserted between Optimizer and Physical Planner:

1. **Remote/Local Identification**: Traverse the Logical Plan, mark tables from remote Spaces using Catalog's `SpaceLocation` metadata
2. **Permission-Constrained Fragmentation**: Split the plan into Fragments. A Fragment containing a remote table can only include operators that the UCAN permits (e.g., if UCAN denies column `email`, no projection or predicate involving `email` can be pushed to the remote)
3. **Pushdown Decisions**:
   - Predicate pushdown: `WHERE rating > 4` → filter at remote
   - Projection pushdown: only request authorized columns
   - Aggregation pushdown: `COUNT(*)` → remote pre-aggregation when possible
4. **Distribution Strategy**:
   - Small remote table → Broadcast to local node for Join
   - Large remote table → Stream via RemoteScan, or push local data to remote if UCAN allows
5. **RemoteExec Operator**: For Browser Nodes, heavy operations (large HashJoin, Compaction, HNSW rebuild) can be wrapped in a `RemoteExec` operator that sends the subplan to a designated Full Node and streams back Arrow batches

### 6.3 Cross-Modality Planning

The planner recognizes queries that cross modalities and generates Hybrid Plans:

```
Example: "Find similar documents to this vector, then search their text"

1. VECTOR phase: HNSW scan for top-K similar vectors
2. BLOB/FTS phase: Dereference CIDs, FTS scan on retrieved documents
3. TABLE phase: SQL filter on metadata columns

The planner executes the VECTOR phase first (most selective),
then feeds doc_ids into the FTS phase,
finally applies SQL predicates.
```

---

## 7. Execution Layer (Layer 3)

### 7.1 Vectorized Engine

Single execution engine, no OLTP row-store branch. Processes data in 1024-row Arrow batches.

| Operator | Description |
|----------|-------------|
| `LocalScan` | Read local Parquet; apply Zone Map / Bloom Filter data skipping |
| `RemoteScan` | Arrow Flight client; sends subplan to remote, streams Arrow RecordBatches |
| `RemoteExec` | Browser Node delegate; sends computation to Full Node, streams results |
| `VectorScan` | HNSW index scan for approximate nearest neighbor queries |
| `GraphTraversal` | CSR-based traversal; source vertex → outgoing edges → target properties |
| `FTSScan` | Inverted index scan with BM25 scoring |
| `HashJoin` / `MergeJoin` | Vectorized join; either side can be Local or Remote |
| `Aggregate` | SIMD aggregation; supports two-phase (remote pre-agg → local merge) |
| `Filter` / `Project` | Vectorized predicate evaluation, column pruning |
| `Unnest` / `JSONPath` | Expand nested Parquet structures (LIST/MAP) into rows |

### 7.2 Browser Node Execution Model

Browser Nodes follow a **capability-based execution** model:

```
Lightweight ops (local):
  - Point lookups via ART index
  - Small range scans with Zone Map pruning
  - Simple filters and projections
  - FTS queries with small result sets
  - Vector similarity search (if HNSW fits in WASM memory)

Delegated ops (RemoteExec):
  - Large HashJoin (build side exceeds WASM memory)
  - Full-table aggregation
  - Compaction and index rebuilds
  - Multi-way graph traversals on large graphs
  - Cross-Space federated joins involving 3+ remote Spaces
```

The Browser Node maintains a **capability profile** — a manifest of what it can execute locally vs. what it delegates. This profile is shared with peers during handshake.

---

## 8. Security & P2P Networking (Layer 0)

### 8.1 DID (Decentralized Identity)

- Node generates Ed25519 keypair on first startup
- DID format: `did:agora:<base58-encoded-public-key>`
- DID Document contains: public key, service endpoints (dynamically updated), supported Space list, capability profile

### 8.2 Peer Manager

Relationship state machine:

```
Unknown → Discovered → Pending → Authorized → Revoked
             ↑                         │
             └─────────────────────────┘ (re-authorization)
```

- **Discovery**: DHT, Relay, mDNS locate candidate peers
- **Subscription Request**: Node A sends request to Node B with DID + public key + desired Space URI
- **Authorization**: B confirms via CLI/Web UI; A receives UCAN for the requested Space
- **Revocation**: B removes A; issued UCANs are added to CRL (Certificate Revocation List) distributed via gossip

### 8.3 UCAN (Capability Token)

JWT format:
- `iss`: Issuer DID
- `aud`: Audience DID
- `att`: Capability array — Space URI, action (`read`/`write`/`admin`), Mode restrictions, column allowlist, row predicate
- `exp`: Expiration timestamp
- `prf`: Proof chain for delegation (A → B → C)

**Arrow Flight Enforcement**: Every RPC carries UCAN in metadata; remote node verifies signature, expiry, and permission scope before executing.

### 8.4 P2P Discovery

| Protocol | Scenario |
|----------|----------|
| **mDNS** | LAN node discovery |
| **DHT** | WAN decentralized discovery |
| **Relay** | NAT traversal fallback |
| **WebRTC** | Browser Node ↔ any node direct connection |

### 8.5 Arrow Flight RPC

- Transport: gRPC over HTTP/2 (WebSocket fallback for Browser Nodes via grpc-web)
- Data: Arrow RecordBatch zero-copy streaming
- Authentication: UCAN exchanged during handshake, verified per request
- Compression: Optional LZ4 on the wire for bandwidth-constrained peers

---

## 9. Interface Layer (Layer 5)

| Interface | Protocol | Target Use Case |
|-----------|----------|-----------------|
| **SQL API** | Arrow Flight SQL / HTTP | Application integration, BI tools, programmatic access |
| **Cypher API** | HTTP / WebSocket | Graph queries on GRAPH mode |
| **MCP API** | JSON-RPC 2.0 / SSE | AI Agent integration (Claude, Cursor, etc.) |
| **HTTP API** | REST / WebSocket | Browser IDE, management dashboard |
| **WASM API** | `wasm-bindgen` | Browser embedding, frontend components |
| **CLI** | Shell | Power users, NAS SSH management, automation |

---

## 10. Web3 Scenario Adaptations

### 10.1 DApp Backend (Structured Data Store)

**Gap addressed**: DApps need structured storage without centralization.

**Agora DB integration**:
- Each DApp deploys a Space per user (or per organization)
- DApp backend interacts via Arrow Flight SQL with UCAN delegation
- Users can revoke DApp access by revoking the UCAN
- Data remains queryable by the user even if the DApp ceases operation

**Implementation note**: DApp developers use `disk` or `s3` storage strategy. Full Node can be co-located with the DApp's existing infrastructure (Docker).

### 10.2 Decentralized Social (PDS 2.0)

**Gap addressed**: Bluesky PDS requires 5TB+ for a Relay; Nostr has no query capability.

**Agora DB differentiation**:
- Browser Node as true PDS: holds user's posts, social graph, and media metadata
- GRAPH mode for social graph (following/followers)
- FTS mode for content search across subscribed feeds
- P2P sync replaces Relay for friend-to-friend content distribution
- `browser,disk` strategy enables multi-device sync without a central relay

**Implementation note**: Social graph stored in GRAPH mode; posts stored in TABLE mode with FTS index on content; media stored as BLOB references (IPFS CID).

### 10.3 On-Chain Analytics (Local Index)

**Gap addressed**: Dune/Indexed.xyz are centralized; Indexed.xyz is static Parquet dumps.

**Agora DB integration**:
- Indexer Full Node converts chain data to Iceberg tables (blocks, transactions, logs, traces)
- Publishes Snapshot updates via P2P
- Users subscribe to subsets (specific contracts, address activity)
- Local SQL analysis with automatic sync — no API keys, no rate limits
- TABLE mode with time partitioning for efficient time-range queries

**Implementation note**: Phase 3a introduces `time_bucket()` for time-series aggregation. Chain-specific schemas (EVM, Solana) provided as Space templates.

### 10.4 DePIN Data Layer

**Gap addressed**: DePIN projects lack a standard data infrastructure layer.

**Agora DB integration**:
- Edge Nodes on devices (sensors, cameras, vehicles) store recent data locally
- `disk,s3` strategy: hot data on device, historical archive to S3
- TABLE mode with time-series optimizations for high-frequency sensor data
- VECTOR mode for embedding-based anomaly detection
- P2P sync between device Edge Nodes and user Full Nodes

### 10.5 AI Agent Data Layer

**Gap addressed**: AI Agents need secure, verifiable, structured data access.

**Agora DB integration**:
- MCP API exposes Spaces as "tools" to AI Agents
- UCAN tokens scope Agent access to specific Spaces, Modes, and row predicates
- Agent actions are auditable: all queries and writes are logged in the Space's Snapshot history
- VECTOR mode for RAG (Retrieval-Augmented Generation) — embedding user documents for semantic search
- FTS mode for keyword-based retrieval
- BLOB mode for referencing external documents (IPFS papers, reports)

### 10.6 Verifiable Credentials & Reputation

**Gap addressed**: No privacy-preserving structured storage for credentials.

**Agora DB integration**:
- User's credentials stored in personal Space (`browser` strategy)
- UCAN enables selective disclosure: verifier can query `age >= 18` predicate without seeing birth_date
- GRAPH mode for trust graph (who attests to whom)
- Federation across credential issuers for cross-domain reputation

### 10.7 Open Data Networks (DeSci / Public Datasets)

**Gap addressed**: Scientific data needs versioning, reproducibility, and distributed access.

**Agora DB integration**:
- Datasets published as Spaces with public UCAN (anyone can read)
- Iceberg Snapshot provides immutable version for reproducibility
- P2P distribution reduces bandwidth costs for data publishers
- TABLE + GRAPH + VECTOR modalities for multi-modal scientific data
- BLOB mode for referencing large raw files (microscopy images, genomic data) via IPFS

---

## 11. Data Flows

### 11.1 Single-Node Write

```
[INSERT / UPDATE / DELETE]
    → Append Buffer (in-memory micro-batch)
    → Flush to small Parquet file
    → Iceberg atomic Snapshot commit
    → Return success (new data immediately visible)
    → Background Compaction:
        Sort-Merge → large sorted files → rebuild indexes (ART, HNSW, CSR, Inverted)
```

### 11.2 Federated Query

```
[SQL query with remote tables]
    → Analyzer: Catalog identifies remote Spaces (must be authorized)
    → Federation Planner: fragment plan, decide pushdown
    → Physical Planner: choose algorithms
    → Execution Engine:
        ├─ LocalScan (local Parquet with data skipping)
        ├─ RemoteScan → Arrow Flight + UCAN → remote execution → stream Arrow batches
        ├─ RemoteExec (Browser Node) → delegate heavy op to Full Node → stream results
        └─ HashJoin / Aggregate (local merge)
    → Return results
```

### 11.3 P2P Synchronization

```
Node A (Full Node, disk)          Node B (Browser Node, OPFS)
├─ Space: blog (Snapshot-3)       ├─ Space: blog (Snapshot-2)
│  [file-1, file-2, file-3]       │  [file-1, file-2]
└──────────────┬──────────────────┘
               │ 1. Exchange latest Manifest hash
               │ 2. A detects B is missing file-3
               │ 3. Arrow Flight / WebRTC transfer file-3
               │ 4. B verifies Blake3 checksum
               │ 5. B commits Snapshot-3 to local Catalog
               ▼
```

**Write Sync** (single master):
```
Master Node (Write)               Replica Node (Read-Only)
├─ New Snapshot-4 committed       ├─ Subscribed to Space
│  (new files: file-4, file-5)    │
└──────────────┬──────────────────┘
               │ 1. Master broadcasts new Manifest CID
               │ 2. Replica fetches Manifest
               │ 3. Replica identifies new files
               │ 4. P2P transfer of missing files
               │ 5. Replica commits Snapshot-4
               ▼
```

### 11.4 Browser Node Remote Execution

```
Browser Node (WASM)               Full Node (Delegated)
├─ Query: large HashJoin          ├─ Receives subplan via RemoteExec
│  (exceeds WASM memory)          │  (UCAN verified)
└──────────────┬──────────────────┘
               │ 1. Browser sends Logical Plan fragment
               │ 2. Full Node compiles to Physical Plan
               │ 3. Full Node executes, produces Arrow batches
               │ 4. Batches streamed back via Arrow Flight
               │ 5. Browser Node receives and delivers to user
               ▼
```

---

## 12. Implementation Roadmap

### Phase 0: Single-Node Storage Kernel (Weeks 1-10)

**Goal**: A standalone columnar database engine with Iceberg catalog, Parquet I/O, and basic SQL query capability running in Docker.

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| Iceberg Metadata Layer | Schema Registry, Snapshot Log, Manifest List, Mode Registry stub | 2 wks |
| Parquet I/O Engine | Row Group read/write, Column Chunk compression (ZSTD), Zone Map statistics | 2 wks |
| VFS Layer | LocalDiskVFS with mmap support | 1 wk |
| Append Buffer + CoW | Micro-batch buffering, atomic Snapshot commit | 1 wk |
| Compaction Scheduler | Sort-Merge compaction, statistics refresh | 2 wks |
| Basic SQL Parser | SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT | 2 wks |

**Milestone 0**: Docker container executes `SELECT SUM(x), COUNT(*) FROM t WHERE y > 100 GROUP BY z` on a 1GB dataset with Compaction running automatically. Data persists across restarts.

**Exit Criteria**:
- [ ] All unit tests pass for Iceberg metadata operations
- [ ] 1GB TPC-H SF1 data can be loaded and queried (SQL subset)
- [ ] Compaction reduces small files to target Row Group size
- [ ] Container restart preserves all data and schema

---

### Phase 1: Query Engine + Federation Framework (Weeks 11-20)

**Goal**: A vectorized SQL execution engine with cross-node federation support.

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| Analyzer + Logical Planner | Table/column resolution, subquery support, CTEs | 2 wks |
| Optimizer (RBO) | Predicate pushdown, projection pruning, join reordering, constant folding | 2 wks |
| Vectorized Execution Engine | 1024-row Arrow batches, LocalScan, HashJoin, MergeJoin, Aggregate, Filter, Project | 3 wks |
| Arrow Flight Server | gRPC server with Arrow RecordBatch streaming | 1 wk |
| Federation Planner | Remote/local fragmentation, pushdown decisions, distribution strategy | 2 wks |
| RemoteScan Operator | Arrow Flight client, subplan serialization | 1 wk |
| Mock Security Layer | Stub UCAN verification (no real crypto yet) | 1 wk |

**Milestone 1**: Two Docker containers can execute a federated JOIN query across Spaces — `SELECT * FROM local.orders o JOIN remote.customers c ON o.customer_id = c.id`. TPC-H queries 1-5 run on single node.

**Exit Criteria**:
- [ ] Cross-container federated query returns correct results
- [ ] TPC-H Q1-Q5 execute with correct results on SF1
- [ ] Query plan shows predicate pushdown to remote node
- [ ] Performance baseline established: Q1 latency < 5s on SF1

**Mid-Phase Evaluation (Week 15)**: If vectorized engine performance is < 50% of DuckDB on equivalent queries, evaluate embedding DuckDB/DataFusion as the execution backend rather than building from scratch.

---

### Phase 2: Security + P2P + Browser Node (Weeks 21-34)

**Goal**: A decentralized network of authenticated peers with Browser Node support.

**Phase 2a: Identity & Authorization (Weeks 21-26)**

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| DID Key Manager | Ed25519 key generation, `did:agora` format, DID Document | 1 wk |
| Peer Manager | Relationship state machine, discovery handshake | 1 wk |
| UCAN Manager | JWT issue/verify, delegation chains, revocation/CRL | 2 wks |
| Arrow Flight + UCAN | UCAN metadata in every RPC, permission enforcement | 1 wk |
| Subscription Protocol | Space subscription request/response flow | 1 wk |

**Milestone 2a**: Two Docker containers establish an authorized peer relationship, exchange Spaces via subscription, and execute a UCAN-gated federated query. Revocation immediately terminates access.

**Exit Criteria**:
- [ ] Peer A can discover Peer B via mDNS
- [ ] Peer B authorizes Peer A for Space X
- [ ] Peer A queries Peer B's Space X via Arrow Flight + UCAN
- [ ] Peer B revokes access; subsequent queries from A are rejected

**Phase 2b: P2P Networking + Browser Node (Weeks 27-34)**

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| P2P Discovery | mDNS (LAN), DHT stub (WAN), Relay fallback | 2 wks |
| P2P Sync Protocol | Manifest comparison, incremental file transfer (Blake3 verified) | 2 wks |
| WASM Build Pipeline | Rust → WASM compilation, wasm-bindgen API | 2 wks |
| OPFSVFS | Origin Private File System integration | 1 wk |
| IDBVFS | IndexedDB fallback for older browsers | 1 wk |
| Browser Query Engine | WASM vectorized execution (subset), RemoteExec delegation | 2 wks |
| WebRTC Transport | Browser Node ↔ Full Node direct connection | 1 wk |

**Milestone 2b**: Chrome browser loads Agora DB WASM, creates a Space in OPFS, subscribes to a Full Node's Space via WebRTC, and executes a federated query combining local browser data with remote Full Node data. Browser Node delegates a large aggregation to Full Node via RemoteExec.

**Exit Criteria**:
- [ ] Browser Node creates and writes to a Space in OPFS
- [ ] Browser Node subscribes to Full Node Space via WebRTC
- [ ] Federated query (local + remote) returns correct results
- [ ] RemoteExec successfully delegates large aggregation
- [ ] Performance: Browser Node can query 10MB local dataset in < 1s

**Mid-Phase Evaluation (Week 30)**: If WASM performance is < 30% of native or memory constraints prevent 50MB+ dataset handling, Browser Node scope is narrowed to: catalog + lightweight scans + mandatory RemoteExec for all joins and aggregations.

---

### Phase 3: Multimodal Extensions (Weeks 35-50)

**Goal**: Full multimodal support — graph, vector, full-text, time-series, and external BLOBs.

**Phase 3a: Graph + Vector + FTS (Weeks 35-44)**

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| GRAPH Mode | Edge/vertex Parquet schema, Cypher parser, CSR index builder | 3 wks |
| Graph Traversal Executor | CSR-based traversal operator, Cypher MATCH execution | 2 wks |
| VECTOR Mode | Vector Parquet schema, HNSW index builder (cosine, L2, IP) | 2 wks |
| Vector Scan Operator | Approximate nearest neighbor search, top-K | 2 wks |
| FTS Mode | Tokenization pipeline, inverted index builder | 2 wks |
| FTSScan Operator | BM25 scoring, ranked retrieval | 2 wks |
| Cross-Modality Planner | Hybrid plan generation: vector → graph → SQL | 2 wks |
| BLOB Mode | CID reference columns, gateway dereference | 1 wk |

**Milestone 3a**: Execute a cross-modality query: vector similarity search on embeddings → graph traversal of social connections → SQL filter on user metadata, all in a single query plan.

**Exit Criteria**:
- [ ] Cypher `MATCH (a)-[:FOLLOWS]->(b) RETURN b.name` executes correctly
- [ ] Vector `SELECT id FROM embeddings ORDER BY vec <=> $query LIMIT 10` returns correct top-10
- [ ] FTS `SELECT * FROM posts WHERE content MATCH 'web3 database'` returns ranked results
- [ ] Cross-modality query produces correct results with proper plan

**Phase 3b: Time-Series + Advanced Features (Weeks 45-50)**

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| Time Partitioning | Automatic time-based partitioning, partition pruning | 1 wk |
| `time_bucket()` | SQL time bucketing function for aggregation | 1 wk |
| Delete Files | Position and Equality delete file support | 1 wk |
| Snapshot Time-Travel | `AS OF TIMESTAMP` queries, snapshot rollback | 1 wk |
| MCP Server | JSON-RPC/SSE server exposing Spaces as AI tools | 2 wks |
| JSON Path Index | Path → Row Group bitmap for semi-structured queries | 1 wk |

**Milestone 3b**: Claude (via MCP) can query an Agora Space with natural language, retrieving data via the SQL interface. Time-series queries with `time_bucket()` execute efficiently on partitioned data.

**Exit Criteria**:
- [ ] MCP Server responds to tool-list and query requests
- [ ] `SELECT time_bucket('1 hour', ts), COUNT(*) FROM events GROUP BY 1` executes correctly
- [ ] Time-travel query returns historical data accurately
- [ ] Delete files are correctly applied at read time and physically removed during compaction

---

### Phase 4: Production Hardening (Weeks 51-60)

**Goal**: Production-ready deployment, observability, and ecosystem integration.

| Work Package | Deliverables | Effort |
|-------------|-------------|--------|
| Observability | Metrics (Prometheus), structured logging, tracing | 2 wks |
| Configuration Management | Space-level config, node-level config, hot reload | 1 wk |
| Performance Tuning | Query profiler, index advisor, slow query log | 1 wk |
| Backup & Restore | Snapshot-based backup, point-in-time recovery | 1 wk |
| Multi-tenant Node | Multiple DIDs per node, namespace isolation | 1 wk |
| Ecosystem Connectors | Python client (DB-API), JS/TS client, dbt adapter | 2 wks |
| Documentation | API docs, deployment guides, Web3 integration tutorials | 2 wks |

**Milestone 4**: Production deployment on 3-node cluster (1 Full + 1 Edge + 1 Browser) with monitoring, backup automation, and documented failover procedures.

---

## 13. Risk Register & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| WASM browser performance insufficient | Medium | High | Mid-phase evaluation (Week 30); fallback to "thin client" Browser Node with mandatory RemoteExec |
| Vectorized engine slower than DuckDB | Medium | High | Mid-phase evaluation (Week 15); embed DataFusion/DuckDB as execution backend |
| HNSW/CSR index size exceeds WASM memory | Medium | High | Quantization (float32→int8) for vectors; graph sampling for CSR; streaming index builds |
| P2P network cold-start (no peers) | High | Medium | Bootstrap with public Full Nodes run by core team; incentive-compatible relay nodes |
| Web3 developer adoption resistance | Medium | High | PostgreSQL-compatible SQL dialect; DuckDB-compatible Parquet output; extensive examples |
| UCAN/DID ecosystem immaturity | Low | Medium | Support WalletConnect/SIWE as transitional identity layer |
| S3 API compatibility gaps | Low | Low | Test against MinIO, R2, Wasabi in CI; abstract S3 operations behind VFS |

---

## 14. Competitive Positioning

| Competitor | Agora DB Differentiator |
|------------|------------------------|
| **DuckDB** | Decentralized (P2P), DID-native, browser-capable, federation |
| **SQLite** | Columnar analytics, Parquet-native, P2P sync, UCAN auth |
| **Ceramic/ComposeDB** | Columnar performance (Parquet/Iceberg), SQL + Cypher, no blockchain dependency |
| **Tableland** | True local-first (not just optimistic), Parquet standard, multi-modal |
| **Bluesky PDS** | Storage-efficient (no relay required), full SQL + graph queries, Browser Node |
| **Indexed.xyz / Dune** | Local execution (sovereign), writable, not just static dumps |
| **Weaviate/Pinecone** | Vectors coexist with SQL tables and graphs, no separate service |

---

## 15. Appendix: Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Core Engine | Rust | Performance, WASM compilation, memory safety |
| Catalog | Apache Iceberg (spec implementation) | Open standard, time-travel, schema evolution, broad ecosystem |
| Storage Format | Apache Parquet | Universal columnar standard, zero-copy with Arrow |
| In-Memory Format | Apache Arrow | Zero-copy interchange, SIMD-friendly |
| Network Transport | Arrow Flight (gRPC) | Native Arrow streaming, UCAN-authenticated |
| P2P Networking | libp2p (Rust) | mDNS, DHT, Relay, WebRTC — battle-tested |
| Cryptography | ed25519-dalek, Blake3 | Fast, secure, Web3-standard |
| WASM Build | wasm-bindgen, wasm-pack | Browser embedding |
| Browser Storage | OPFS (primary), IndexedDB (fallback) | Origin-private, sufficient capacity |

---

*This document serves as the canonical architecture baseline for Agora DB. All engineering implementation proceeds from this specification.*
