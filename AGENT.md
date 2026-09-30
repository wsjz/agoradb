# AGENT.zh.md — Agora DB 开发协作协议

> **目的**: 定义人类开发者与 AI Agent 在 Agora DB 项目中的协作规则。
> **文档语言**: 所有协作文档（本文件、开发计划、Issue、PR 描述）使用 **中文**。
> **代码语言**: 所有源代码、注释和标识符使用 **英文**。
> **许可证**: Apache-2.0（为后续 Apache 基金会捐赠做准备）。

---

## 1. 项目概览

Agora DB 是一个**去中心化的本地联邦查询语义层** —— 嵌入式、主权化、原生 P2P。

AgoraDB 负责"谁能看什么、数据在哪、查询发到哪"，**不负责"怎么算"**：计算交给可插拔的嵌入式引擎（分析型 DuckDB、事务型 SQLite，浏览器端由 JS 宿主提供），DataFusion 只作为联邦协调器合并多个引擎的 Arrow 结果。规范见 `agoradb_architecture_v3.md`。

- **核心语言**: Rust（WASM 可编译、内存安全）
- **计算引擎**: DuckDB（分析型 Space）、SQLite（事务型 Space）
- **联邦协调**: Apache DataFusion + `datafusion-federation`（仅合并，不读文件）
- **目录系统**: Apache Iceberg（分析型 Space）+ `.agora/` 下的 Space/Location 注册表
- **存储格式**: Apache Parquet（分析型、P2P 同步单元）、SQLite 文件（事务型）
- **内存格式**: Apache Arrow
- **网络传输**: Arrow Flight (gRPC) + libp2p（3.1）
- **密码学**: ed25519-dalek + Blake3（3.1）
- **浏览器端**: wasm-bindgen + duckdb-wasm / wa-sqlite（3.2）

### 架构分层（自底向上）

```
L0: 身份与网络层 (DID, UCAN, libp2p, Arrow Flight, 快照同步)
L1: 目录与存储层 (Space/Location 注册表, Iceberg 元数据, Parquet 写路径, VFS)
L2: 引擎抽象层 (QueryEngine trait: DuckDB | SQLite | 宿主引擎)
L3: 联邦层 (DataFusion 协调器: 按引擎下推 SQL, 合并 Arrow 流)
L4: 语义层 (<space>.<table> 命名, 语句分类路由, 视图 / UCAN 策略重写)
L5: 接口层 (SQL API, HTTP, WASM, CLI)
```

### 设计原则（不可违背）

1. **计算外包**：不实现扫描、Join、聚合或索引算法；引擎做不到就换引擎或配置引擎，不写算子。
2. **每个 Space 单写主**；写权限通过 UCAN 显式委托。
3. **Space（逻辑）与 Location（物理）分离**：多个 Space 可绑定同一 Location，至多一个可写。
4. **只有不可变文件跨网络**：P2P 只同步 Iceberg 快照；事务型 Space 通过发布 Parquet 快照对外共享。
5. **默认拒绝**：无对等关系 = 无目录可见性 = 无查询权限。

---

## 2. 沟通规则

### 2.1 语言策略

| 上下文 | 语言 |
|---------|----------|
| 本文件 (AGENT.md) | 中文 |
| 开发计划 / 路线图 | 中文 |
| 代码注释 | 英文 |
| 源代码标识符 | 英文 |
| 提交信息 (Commit messages) | 英文，**只写一行**（如 `feat(semantic): add views and row/column policies`），不写正文、不加任何尾注 |
| Issue / PR 描述 | 英文 |
| API 文档 (docstrings) | 英文 |
| 面向用户的文档 | 英文 |

### 2.2 Agent 启动协议

每次开发会话开始前，Agent **必须**：

1. 读取 `AGENT.md`（本文件）。
2. 读取 `agoradb_architecture_v3.md` 获取架构上下文（v1/v2 仅作历史参考，冲突时以 v3 为准）。
3. 检查当前 git 分支和状态。
4. 审阅本次会话的任何待办任务/计划。

**本地工具链**：`rust-toolchain.toml` 固定 stable；首次构建 `agoradb-engine-duckdb` 会从源码编译 DuckDB（约 7 分钟），之后走增量缓存。

---

## 3. Apache 基金会合规要求

为确保项目具备捐赠给 Apache 软件基金会的资格，所有代码 **必须** 遵守以下规定：

### 3.1 许可证头

每个源文件都必须包含标准的 Apache-2.0 头：

```rust
// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
```

### 3.2 依赖策略

- **仅允许宽松许可证**: Apache-2.0, MIT, BSD-2/3-Clause, ISC
- **禁止**: GPL, LGPL, AGPL, SSPL, 专有软件, "非商业" 许可证
- **优先选择**: 同等功能下优先 Apache-2.0 而非 MIT
- **文档化**: 在 `NOTICE` 文件中记录所有依赖及其归属信息
- **自动检查**: `cargo deny check licenses`（规则见 `deny.toml`）；新增依赖后必须通过

### 3.3 治理规范

- 提交要少：按路线图阶段或完整功能合并提交，不按子步骤拆分
- 不接受匿名贡献；所有作者身份必须可识别
- 禁止从 Stack Overflow 或其他来源复制代码而不验证许可证
- 仓库中禁止包含二进制 blob

### 3.4 行为准则

- 所有沟通中使用包容性和专业语言
- 建设性批评；禁止人身攻击
- 尊重不同的背景和技能水平

---

## 4. 开发工作流

### 4.1 分阶段实现

项目遵循严格的分阶段路线图：

v3 路线图（详见 `agoradb_architecture_v3.md` §12）：

| 阶段 | 目标 | 状态 |
|-------|------|----------|
| 3.0-A 引擎 | `QueryEngine` 抽象、DuckDB/SQLite 引擎、Space/Location 目录、单 Space 查询整条下推 | ✅ |
| 3.0-B 联邦 | DataFusion 协调器 + `datafusion-federation`、跨 Space join、快照 pinning | ✅ |
| 3.0-C 语义层 | 视图、列授权与行策略（SQL 改写）、principal 只读 + 默认拒绝、枚举抵抗 | ✅ |
| 3.0-D 发布 | SQLite → Parquet 快照发布器 | 待开始 |
| 3.1 网络 | DID/UCAN、Arrow Flight 远端表、libp2p 发现、快照订阅 | 待开始 |
| 3.2 浏览器 | WASM 核心 + 宿主引擎桥 (duckdb-wasm / wa-sqlite) | 待开始 |
| 3.3 多模态 | 引擎扩展：DuckDB vss/fts/DuckPGQ、SQLite FTS5/sqlite-vec | 待开始 |

**规则**: 禁止跳阶段。每个阶段的退出标准必须达成后才能进入下一阶段。

### 4.2 任务执行协议

每个任务的执行流程：

1. **读取 AGENT.md**（本文件）。
2. **读取相关架构章节** 于 `agoradb_architecture_v3.md`。
3. **沟通与设计** —— **开发代码前必须与用户充分沟通设计方案，经用户确认后方可进入实现阶段。禁止未经确认直接编码。**
4. **制定计划** —— 如果任务跨多个文件或较为复杂，使用 Plan Mode 制定详细实现计划并获得用户批准。
5. **先写测试** —— TDD 优先，尤其针对存储和查询引擎。
6. **实现** —— 使用地道、简洁的 Rust 代码。
7. **运行测试** —— 单元测试、集成测试和相关基准测试。
8. **验证 Apache 合规** —— 许可证头、依赖检查。
9. **请求代码审阅** —— 合并前必须经过审阅。

### 4.3 Skills 使用规范

Claude Code 提供多种 skills 用于规范开发流程。以下规范按强制执行（MUST）和推荐使用（SHOULD）两级分类。

#### 强制使用（MUST）—— 不满足条件禁止 proceeding

| 触发条件 | 必须调用的 Skill | 说明 |
|---------|----------------|------|
| 创建新功能、组件或模块前 | `superpowers:brainstorming` | 先探索需求、设计约束和可行方案 |
| 多步骤实现任务（跨文件或复杂逻辑） | `superpowers:writing-plans` | 制定详细实现计划，经用户确认后方可执行 |
| 执行已书面化的计划 | `superpowers:executing-plans` | 分阶段执行，含审阅检查点 |
| 功能开发（可能修改多个文件） | `superpowers:using-git-worktrees` | 使用隔离的 git worktree，避免污染主分支 |
| 实现任何功能或 bug 修复 | `superpowers:test-driven-development` | 先写测试，后写实现代码 |
| 遇到 bug、测试失败或意外行为 | `superpowers:systematic-debugging` | 系统化调试，禁止凭直觉猜测修复 |
| 声称任务完成、测试通过或准备合并前 | `superpowers:verification-before-completion` | 运行验证命令并确认输出，证据先于断言 |

#### 推荐使用（SHOULD）—— 根据场景判断

| 场景 | 推荐 Skill |
|------|-----------|
| 研究 Iceberg / Parquet / Arrow 规范等外部技术细节 | `deep-research` |
| 存在 2+ 独立任务可并行处理 | `superpowers:dispatching-parallel-agents` |
| 子代理可独立执行实现子任务 | `superpowers:subagent-driven-development` |
| 完成任务后请求代码审查 | `superpowers:requesting-code-review` |
| 处理代码审查反馈 | `superpowers:receiving-code-review` |
| 开发分支完成后决定如何集成 | `superpowers:finishing-a-development-branch` |
| 审查当前 diff 的正确性和效率问题 | `code-review` |
| 简化代码、提升复用和效率 | `simplify` |
| 运行应用验证变更实际生效 | `run` / `verify` |
| Phase 2+ 涉及安全相关的代码 | `security-review` |

#### 不适用（NOT APPLICABLE）

以下 skills 与 Agora DB 项目无关，**禁止**使用：

- 前端/UI 类：`frontend-design`, `impeccable`, `design-taste-frontend`, `baseline-ui`, `high-end-visual-design`, `minimalist-ui`, `industrial-brutalist-ui`, `ui-ux-pro-max`, `fixing-accessibility`, `fixing-metadata`, `fixing-motion-performance`, `gpt-taste`, `image-to-code`, `imagegen-frontend-web`, `imagegen-frontend-mobile`, `redesign-existing-projects`, `stitch-design-taste`, `brandkit`
- Figma 类：`figma:figma-use`, `figma:figma-generate-design`, `figma:figma-generate-diagram`, `figma:figma-generate-library`, `figma:figma-implement-design`, `figma:figma-code-connect`, `figma:figma-create-design-system-rules`, `figma:figma-use-figjam`
- 其他：`claude-api`, `init`, `keybindings-help`

---

### 4.4 代码质量标准

- **Rust 惯用法**: 遵循 `rustfmt` 和 `clippy`（CI 中禁止警告）。
- **错误处理**: 使用 `thiserror` 或 `snafu` 实现结构化错误；生产代码禁止 `unwrap()`。
- **文档**: 每个公共 API 必须有 rustdoc 注释。
- **Unsafe 代码**: 最小化使用；每个 `unsafe` 块必须有安全注释说明不变量。
- **测试**: 核心模块覆盖率目标 >80%（存储、目录、执行）。
- **基准测试**: 性能关键路径使用 `criterion.rs`。

### 4.4 模块组织

```
agoradb/
├── Cargo.toml                  # 工作区根；所有共享依赖版本在 [workspace.dependencies] 统一锁定
├── Cargo.lock                  # 已提交（duckdb 精确锁版）
├── rust-toolchain.toml         # stable + rustfmt + clippy
├── deny.toml                   # cargo-deny 许可证白名单
├── LICENSE / NOTICE            # Apache-2.0 / 依赖归属
├── AGENT.md                    # 本文件
├── agoradb_architecture_v3.md  # 规范架构（v1/v2 为历史）
├── crates/
│   ├── agoradb-core/           # 错误、SpaceUri、SpaceKind/EngineKind/AccessMode
│   ├── agoradb-vfs/            # OpenDAL VFS（3.1 接入）
│   ├── agoradb-catalog/        # Iceberg Catalog + Space/Location 注册表 + 快照解析
│   ├── agoradb-storage/        # Parquet 写路径、Compaction
│   ├── agoradb-engine/         # QueryEngine trait、TableSource、BlockingWorker
│   ├── agoradb-engine-duckdb/  # 分析型引擎（bundled DuckDB）
│   ├── agoradb-engine-sqlite/  # 事务型引擎（SQLite 行 → Arrow）
│   ├── agoradb-semantic/       # Agora 语句解析、语句分类、视图展开与权限改写
│   ├── agoradb-federation/     # DataFusion 协调器、EngineSqlExecutor
│   └── agoradb-node/           # AgoraSession、EngineRegistry、路由
├── tests/                      # agoradb-tests（TPC-H Q1–Q5）+ tools/data-gen
└── docs/                       # 用户和开发者文档（gitignored）
```

计划中：`agoradb-identity`、`agoradb-network`（3.1），`agoradb-wasm`（3.2），`agoradb-cli`。

---

## 5. 命名规范

### 5.1 代码标识符

| 项目 | 规范 | 示例 |
|------|------------|---------|
| Crate 名称 | `kebab-case`，前缀 `agoradb-` | `agoradb-catalog` |
| 模块名称 | `snake_case` | `iceberg_catalog` |
| 类型名称 | `PascalCase` | `SchemaRegistry` |
| 函数名称 | `snake_case` | `commit_snapshot` |
| 常量 | `SCREAMING_SNAKE_CASE` | `DEFAULT_ROW_GROUP_SIZE` |
| 错误类型 | `PascalCase` + `Error` 后缀 | `CatalogError` |
| Trait 名称 | `PascalCase` | `SpaceResolver` |
| 泛型参数 | 单个大写字母 | `T`, `K`, `V` |

### 5.2 领域专用术语

代码中使用以下精确拼写：

| 概念 | 代码标识符 | 说明 |
|---------|-----------------|---------|
| Space | `Space` | 主权单元（逻辑），SQL 限定名 `<space>.<table>` |
| Location | `Location` | 物理存放位置（Iceberg namespace 或 SQLite 文件），可被多个 Space 绑定 |
| SpaceKind | `SpaceKind` | `Analytical`（Iceberg + Parquet）/ `Transactional`（SQLite） |
| Engine | `QueryEngine` / `EngineKind` | 计算引擎（DuckDB / SQLite / 宿主） |
| Node | `Node` | 对等实体 |
| Catalog | `Catalog` | 本地元数据注册表 |
| Snapshot | `Snapshot` | 不可变的 Iceberg 快照 |
| Manifest | `Manifest` | 文件清单 |
| UCAN | `Ucan` / `UCAN` | 能力令牌 |
| DID | `Did` / `DID` | 去中心化身份 |
| VFS | `Vfs` / `VFS` | 虚拟文件系统 |
| OPFS | `Opfs` / `OPFS` | 源私有文件系统 |
| CID | `Cid` / `CID` | 内容标识符 |
| FTS | `Fts` / `FTS` | 全文搜索 |

---

## 6. 测试策略

### 6.1 测试层级

```
单元测试         → 按模块，快速，Mock 依赖
集成测试         → 跨 crate，真实 I/O（临时文件）
基准测试         → criterion.rs，追踪性能回归
一致性测试       → TPC-H 查询验证 SQL 正确性
端到端测试       → 基于 Docker 的多节点场景
```

### 6.2 测试数据

- 单元测试使用确定性合成数据。
- TPC-H 集成测试使用 SF 0.001，数据由 `cargo run -p test-data-gen -- --scale-factor 0.001 --force` 生成到 `tests/agora-local`（gitignored，不提交）。
- 需要网络的测试（如 DuckDB `sqlite` 扩展）标记 `#[ignore]`，用 `--features sqlite-scanner-tests -- --ignored` 单独运行。
- 测试中禁止使用真实用户数据。

### 6.3 CI 要求

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo doc --no-deps`（确保无断链）
- 许可证头检查（通过 `apache-skywalking-eyes` 或类似工具）
- 依赖许可证审计 (`cargo deny check licenses`)

---

## 7. 安全与密码学规范

### 7.1 密码学使用

- **Ed25519**: 使用 `ed25519-dalek` crate 进行签名/验证。
- **哈希**: 使用 `blake3` 进行内容寻址和校验和。
- **随机数**: 使用 `rand::thread_rng()` 或 `getrandom`；WASM 中严禁 `Math.random()`。
- **密钥存储**: 绝不记录私钥；存储于 OS 密钥链或加密 OPFS。

### 7.2 UCAN 实现

- JWT 格式，标准声明 (`iss`, `aud`, `att`, `exp`, `prf`)。
- 任何能力检查前必须先验证签名。
- 过期时短路处理；已撤销令牌不继续处理。
- CRL（证书撤销列表）通过 P2P 传播。

### 7.3 P2P 安全

- 默认拒绝：无对等关系 = 无目录可见性 = 无查询权限。
- 枚举抵抗：未授权表返回 "不存在"。
- 每次 Arrow Flight RPC 携带 UCAN 元数据。

---

## 8. 文档要求

### 8.1 代码文档

- 每个公共函数、结构体、枚举和 trait 必须有 rustdoc。
- 文档注释中尽可能包含示例。
- 记录 panic 条件和安全不变量。

### 8.2 架构文档

- `agoradb_architecture_v3.md` 是规范原文；v1、v2 仅作历史参考。
- 任何架构偏离必须记录并获得批准。
- 开发实践变更时更新 `AGENT.md`。

### 8.3 用户文档

- API 参考从 rustdoc 生成。
- Docker、NAS 和浏览器嵌入的部署指南。
- Web3 集成教程（DApp 后端、社交 PDS、链上分析）。

---

## 9. 提交与审阅清单

标记任何任务完成前：

- [ ] 代码无警告编译通过 (`cargo build`)
- [ ] 所有测试通过 (`cargo test`)
- [ ] Clippy 无警告 (`cargo clippy`)
- [ ] 格式化通过 (`cargo fmt`)
- [ ] 所有新文件包含许可证头
- [ ] 无新增许可证不兼容的依赖
- [ ] 文档已更新（rustdoc + 如有 API 变更则更新用户文档）
- [ ] 基准测试已运行（如触及性能关键代码）
- [ ] CHANGELOG.md 已更新（如为面向用户的变更）

---

## 10. 决策日志

记录重要的架构或流程决策：

| 日期 | 决策 | 理由 |
|------|----------|-----------|
| 2025-05-30 | 初始 AGENT.md 创建 | 为分阶段实现建立开发协议 |
| 2026-06-08 | 执行内核迁移到 DataFusion（v2） | 放弃自研优化器/执行器 |
| 2026-09-27 | 转型为去中心化本地联邦查询语义层（v3）：计算交给 DuckDB/SQLite，DataFusion 只做联邦合并 | 自研 OLAP 超出团队范围；差异化在主权、联邦与 P2P，而非算子 |

---

*本文档是活文档。实践演变时更新它。*
*所有 Agent 每次开发会话前 **必须** 读取本文件。*
