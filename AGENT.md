# AGENT.zh.md — Oikome 开发协作协议

> **目的**: 定义人类开发者与 AI Agent 在 Oikome 项目中的协作规则。
> **文档语言**: 所有协作文档（本文件、开发计划、Issue、PR 描述）使用 **中文**。
> **代码语言**: 所有源代码、注释和标识符使用 **英文**。
> **许可证**: Apache-2.0（为后续 Apache 基金会捐赠做准备）。

---

## 1. 项目概览

Oikome 是一个 **AI Native 的多模态私域数据中心**，通常使用个人资源，支持受信的数据交换。

- **私域**：数据归所有者（个人、家庭或小团队），存在自己控制的存储上（本地硬盘、NAS、网盘），由数据接入从各个 App 接进来。
- **多模态**：文本、图片、音频、文件、向量和表格一样是一等数据。
- **AI Native**：AI 工具和 Agent 通过 MCP 像其他应用一样接入，只看到授权给它的数据；数据存成 AI 能直接用的形式（文本、向量、检索索引）；视图和命名让 AI 知道数据的含义；向量化、推理等计算也跑在个人资源上。
- **个人资源**：存储和算力来自个人，而不是平台的云：先用自己的机器，不够时通过算力共享用其他个人的机器。
- **受信的数据交换**：数据只在互信的数据中心之间双向流动，授权可撤销。

数据交换和算力共享用同一套信任机制（DID、对等关系、默认拒绝、UCAN 撤销），授权分开（数据：`space/query` / `space/read`；算力：`compute/<runtime>`），互不包含。

底层是一个**去中心化的本地联邦查询语义层** —— 嵌入式、主权化、原生 P2P。

Oikome 负责"谁能看什么、数据从哪来、存在哪、查询和任务发到哪"，**不负责"怎么算"**：计算交给可插拔的嵌入式引擎（分析型 DuckDB、事务型 SQLite，浏览器端由 JS 宿主提供），DataFusion 只作为联邦协调器合并多个引擎的 Arrow 结果。规范见 `oikome_architecture_v3.md`。

- **核心语言**: Rust（WASM 可编译、内存安全）
- **计算引擎**: DuckDB 1.5.6（分析型 Space；Lance Space 经 `lance` 扩展读写和检索、经 `duckpgq` 做图查询，扩展用 `oikome extensions install` 下载）、SQLite（事务型 Space；FTS5 全文检索，sqlite-vec 向量检索）
- **联邦协调**: Apache DataFusion + `datafusion-federation`（仅合并，不读文件）
- **目录系统**: Apache Iceberg（分析型 Space）+ `.oikome/` 下的 Space/Location 注册表
- **存储格式**: Apache Parquet（分析型、P2P 同步单元）、SQLite 文件（事务型）、Lance（向量 / 全文 / 大对象，3.3）
- **内存格式**: Apache Arrow
- **网络传输**: Arrow Flight (gRPC)，TLS 证书绑定节点 DID；libp2p 只用于节点发现（mDNS）；浏览器经 WebRTC 数据通道连 Full Node（str0m，DTLS 指纹由 DID 签名的接入记录担保）
- **密码学**: ed25519-dalek + Blake3（3.1）
- **浏览器端**: `oikome-wasm`（wasm-bindgen）+ duckdb-wasm（OPFS）+ IndexedDB + WebRTC（`web/src/transport.js`）；联邦查询在浏览器里单任务执行（`Coordinator::single_task`）；构建见 `web/scripts/build.sh`（`wasm` profile；需要能编译 wasm32 的 clang，macOS 用 Homebrew LLVM）

### 架构分层（自底向上）

```
L0: 身份与网络层 (DID, UCAN, libp2p, Arrow Flight, 快照同步, 算力共享)
L1: 目录与存储层 (Space/Location 注册表, Iceberg 元数据, Parquet 写路径, VFS: 本地盘 / NAS / S3 / 网盘)
L2: 引擎抽象层 (QueryEngine trait: DuckDB | SQLite | 宿主引擎)
L3: 联邦层 (DataFusion 协调器: 按引擎下推 SQL, 合并 Arrow 流)
L4: 语义层 (<space>.<table> 命名, 语句分类路由, 视图 / UCAN 策略重写)
L5: 接口层 (SQL API, HTTP, WASM, CLI, MCP) + 数据接入 (Connector: App 官方 API、导出文件)
```

### 设计原则（不可违背）

1. **计算外包**：不实现扫描、Join、聚合或索引算法；引擎做不到就换引擎或配置引擎，不写算子。
2. **每个 Space 单写主**；写权限通过 UCAN 显式委托。
3. **Space（逻辑）与 Location（物理）分离**：多个 Space 可绑定同一 Location，至多一个可写。
4. **只有不可变文件跨网络**：P2P 只同步 Iceberg 快照和 Lance 版本；事务型 Space 通过发布 Parquet 快照对外共享。
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
2. 读取 `oikome_architecture_v3.md` 获取架构上下文（v1/v2 仅作历史参考，冲突时以 v3 为准）。
3. 检查当前 git 分支和状态。
4. 审阅本次会话的任何待办任务/计划。

**本地工具链**：`rust-toolchain.toml` 固定 stable；首次构建 `oikome-engine-duckdb` 会从源码编译 DuckDB（约 7 分钟），之后走增量缓存。

---

## 3. Apache 基金会合规要求

为确保项目具备捐赠给 Apache 软件基金会的资格，所有代码 **必须** 遵守以下规定：

### 3.1 许可证头

每个源文件都必须包含标准的 Apache-2.0 头：

```rust
// Copyright 2025 The Oikome Authors
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

v3 路线图（详见 `oikome_architecture_v3.md` §14）：

| 阶段 | 目标 | 状态 |
|-------|------|----------|
| 3.0-A 引擎 | `QueryEngine` 抽象、DuckDB/SQLite 引擎、Space/Location 目录、单 Space 查询整条下推 | ✅ |
| 3.0-B 联邦 | DataFusion 协调器 + `datafusion-federation`、跨 Space join、快照 pinning | ✅ |
| 3.0-C 语义层 | 视图、列授权与行策略（SQL 改写）、principal 只读 + 默认拒绝、枚举抵抗 | ✅ |
| 3.0-D 发布 | `PUBLISH SPACE`：SQLite → Iceberg 快照（覆盖式）、定时发布 | ✅ |
| 3.1 网络 | DID/UCAN、Arrow Flight 远端表、libp2p 发现、快照订阅 | ✅（3.1a 远程查询、3.1b 快照订阅、3.1c 节点发现与加密传输、3.1d 三个 Docker 节点在 CI 跑通） |
| 3.2 浏览器 | WASM 核心 + 宿主引擎桥（duckdb-wasm）；经 Full Node 的联邦查询走 WebRTC 数据通道 | ✅ 3.2a 本地查询；3.2b WebRTC 与联邦查询 |
| 3.3 多模态 | Lance Location（经 DuckDB `lance` 扩展：向量、全文、大对象）、DuckPGQ、SQLite FTS5/sqlite-vec | ✅ 3.3a Lance Space；3.3b SQLite 全文和向量检索；3.3c DuckPGQ（只在 Lance Space 上） |
| 3.4 网盘 | opendal 网盘服务（WebDAV、Google Drive、Dropbox、OneDrive 等）、同步文件夹、客户端加密；SQLite 不放网盘 | 待开始 |
| 3.5 数据接入 | `Connector` 合约、按来源主键 upsert + 游标同事务提交、定时拉取；首批：文件导入、flomo | 待开始 |
| 3.6 算力共享 | `compute/<runtime>` UCAN 能力与限额（与数据授权分开）、任务经 Arrow Flight 路由到对端已注册的运行时、按对端配额 | 待开始 |

用户端路线（与上面的核心阶段并行，互不阻塞；每个里程碑要等前置阶段完成，详见架构 §14.1）：

| 里程碑 | 目标 | 前置 | 状态 |
|-------|------|------|----------|
| M1a 目录拆分 | `web/` 拆到 `packages/node` 和 `e2e/`，根目录 `package.json` 用 npm workspaces；browser-e2e 原样通过 | 3.2 | 待开始 |
| M1b 页面 | `packages/ui`、`packages/screens`、`packages/client`、`apps/web`：9 个页面先用假数据，桌面和手机两套布局 | M1a | 待开始 |
| M1c 能用 | `oikome-api`、`oikome serve --web`、`packages/api`；首次启动与恢复密钥、资料库、分享与收回、手机配对、访问记录、高级 SQL 接上真实数据 | 3.0–3.2、M1b | 待开始 |
| M2 能接 | 接入页与同步状态、首页看板、内置卡片、看板插件 v1（`plugin-sdk`、数据契约、插件授权）、参考插件 | 3.5、M1c | 待开始 |
| M3 能找 | 全文和语义搜索、照片和笔记视图、AI 助手页、`oikome-mcp` | 3.3、M2 | 待开始 |
| M4 能扩 | 借用算力、第三方接入插件、插件目录；网盘存储待定 | 3.4、3.6、M3 | 待开始 |

**规则**: 禁止跳阶段。每个阶段的退出标准必须达成后才能进入下一阶段。

### 4.2 任务执行协议

每个任务的执行流程：

1. **读取 AGENT.md**（本文件）。
2. **读取相关架构章节** 于 `oikome_architecture_v3.md`。
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

以下 skills 与 Oikome 项目无关，**禁止**使用：

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
oikome/
├── Cargo.toml                  # 工作区根；所有共享依赖版本在 [workspace.dependencies] 统一锁定
├── Cargo.lock                  # 已提交（duckdb 精确锁版）
├── rust-toolchain.toml         # stable + rustfmt + clippy
├── deny.toml                   # cargo-deny 许可证白名单
├── LICENSE / NOTICE            # Apache-2.0 / 依赖归属
├── AGENT.md                    # 本文件
├── oikome_architecture_v3.md  # 规范架构（v1/v2 为历史）
├── crates/
│   ├── oikome-core/           # 错误、SpaceUri、SpaceKind/EngineKind/AccessMode
│   ├── oikome-vfs/            # OpenDAL VFS（3.4 接入）
│   ├── oikome-catalog/        # Iceberg Catalog + Space/Location 注册表 + 快照解析
│   ├── oikome-storage/        # Parquet 写路径、Compaction
│   ├── oikome-engine/         # QueryEngine trait、TableSource、BlockingWorker
│   ├── oikome-engine-duckdb/  # 分析型引擎（bundled DuckDB）
│   ├── oikome-engine-sqlite/  # 事务型引擎（SQLite 行 → Arrow）
│   ├── oikome-semantic/       # Oikome 语句解析、语句分类、视图展开与权限改写
│   ├── oikome-federation/     # DataFusion 协调器、EngineSqlExecutor
│   ├── oikome-node/           # OikomeSession、EngineRegistry、路由、订阅副本
│   ├── oikome-identity/       # DID、UCAN 签发 / 委托 / 撤销、请求签名
│   ├── oikome-network/        # 节点间协议、PeerClient、FlightEngine
│   ├── oikome-discovery/      # libp2p 节点发现（mDNS、交换 Flight 端点），写入 Peers 表
│   ├── oikome-protocol/       # 节点间协议类型与请求摘要、WebRTC 接入记录与分帧（原生和浏览器共用）
│   ├── oikome-webrtc/         # 浏览器用的 WebRTC 端点（str0m）：会话、DTLS 证书、接入记录
│   ├── oikome-wasm/           # 浏览器节点：BrowserNode、HostEngine（duckdb-wasm）、RemoteEngine、JS 接口
│   └── oikome-server/         # Arrow Flight 服务、oikome 命令行
├── tests/                      # oikome-tests（TPC-H Q1–Q5）+ tools/data-gen + e2e/（Docker 节点端到端脚本）
├── docker/                     # 节点镜像（oikome.Dockerfile）与三节点 compose
├── web/                        # 浏览器节点的 JS 宿主（duckdb-wasm、IndexedDB、WebRTC）、页面与 Playwright 测试
├── third_party/                # 打了补丁的第三方代码（datafusion-federation，见 README）
├── .github/workflows/          # docker-e2e（三个 Docker 节点）、browser-e2e（无头 Chromium 跑浏览器节点）
└── docs/                       # 用户和开发者文档（gitignored）
```

计划中：`oikome-connect`（3.5），`oikome-cli`。

用户端计划中（目录等对应工作开始时才建，结构见架构 §13.3）：
- crate：`oikome-api`（M1c）、`oikome-mcp`（M3）。
- 根目录：`package.json`（npm workspaces，M1a）、`apps/`（web 在 M1b；desktop、mobile 以后）、`packages/`（node 在 M1a；screens、ui、client 在 M1b；api 在 M1c；plugin-sdk 在 M2）、`plugins/`（M2）、`design/`（以后）、`e2e/`（M1a）。
- M1a 时 `web/` 拆到 `packages/node` 和 `e2e/`，之后不再有 `web/`。

---

## 5. 命名规范

### 5.1 代码标识符

| 项目 | 规范 | 示例 |
|------|------------|---------|
| Crate 名称 | `kebab-case`，前缀 `oikome-` | `oikome-catalog` |
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
| Location | `Location` | 物理存放位置（Iceberg namespace、SQLite 文件或 Lance 数据集），可被多个 Space 绑定 |
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
- TPC-H 集成测试使用 SF 0.001，数据由 `cargo run -p test-data-gen -- --scale-factor 0.001 --force` 生成到 `tests/oikome-local`（gitignored，不提交）。
- 需要网络的测试（如 DuckDB `sqlite` 扩展）标记 `#[ignore]`，用 `--features sqlite-scanner-tests -- --ignored` 单独运行。
- 测试中禁止使用真实用户数据。

### 6.3 CI 要求

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo doc --no-deps`（确保无断链）
- 许可证头检查（通过 `apache-skywalking-eyes` 或类似工具）
- 依赖许可证审计 (`cargo deny check licenses`)
- 浏览器节点：`.github/workflows/browser-e2e.yml`，构建 wasm 和 `oikome` 后用 Playwright 在无头 Chromium 里跑 `web/tests`（其中 `remote.spec.mjs` 自己起 `oikome serve --webrtc-listen`）
- 网络端到端：`.github/workflows/docker-e2e.yml`，三个 Docker 节点跑 `tests/e2e/docker_nodes.sh`（推送 `feat/**`、`ci/**` 时触发）

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

- `oikome_architecture_v3.md` 是规范原文；v1、v2 仅作历史参考。
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
| 2026-10-30 | 定位改为 AI Native 的多模态私域数据中心（通常使用个人资源、支持受信的数据交换）：新增网盘接入（3.4）、数据接入（3.5）、算力共享（3.6），v3 各层不变；数据交换和算力共享同一信任机制、不同授权 | 数据和算力留在个人手里，个人之间互相调用而不交给大平台；架构见 `oikome_architecture_v3.md` §1、§7.5、§11、§12 |
| 2026-10-31 | 3.2a 浏览器节点：duckdb-wasm 作本地引擎（SQLite 以后再做）；`HostEngine` 只存 id、JS 对象放线程本地表、结果用 Arrow IPC；DataFusion 去掉 compression/parquet 并给 datafusion-federation 0.5.5 打补丁；浏览器连 Full Node 定为 WebRTC 数据通道（3.2b） | 不改 `QueryEngine` 和 DataFusion；0.5.7 才修掉强制默认功能但要 DataFusion 55；浏览器无法按 DID 校验证书，WebRTC 能锁定 DID 签名过的指纹。见架构 §10 |
| 2026-11-01 | 3.2b：Full Node 用 str0m（纯 Rust 加密）提供 WebRTC 端点，浏览器 WebRTC-direct 直连（不需要信令），服务端不校验浏览器的 DTLS 证书；请求是带 Flight 调用同样字段的信封，由 `serve_envelope` 走同一套认证、授权和语义层；浏览器里的协调器单任务执行（一个分区，`MergePartitionsExec` 代替会起任务的 `CoalescePartitionsExec`），chrono 开 `wasmbind` | 服务端不引入 C 加密库，也不要信令服务；浏览器靠签名的请求认证，和原生节点一样；DataFusion 54 合并分区会起 Tokio 任务、按 `std` 读时钟，在浏览器里都会 panic。见架构 §10 |
| 2026-11-01 | 3.3c：图查询（DuckPGQ）只在 Lance Space 上；图定义存在目录里（`graphs.json`），DuckDB 里没有时先建图；只给主人、只读一个 Space | DuckPGQ 不支持在视图上建图，而 Iceberg 表在 DuckDB 里是视图；为每个快照复制成表的方案没有采用。见架构 §8 |
| 2026-11-01 | 3.3b：事务型 Space 支持 FTS5 和 sqlite-vec 0.1.9；`CREATE VIRTUAL TABLE` 由 Oikome 自己读（只认 fts5、vec0，括号里原样交给 SQLite）；含 `MATCH` 的语句改用 SQLite 方言解析；检索只给主人、只在一个 Space 里；`PUBLISH` 跳过虚拟表 | sqlparser 读不了模块参数，通用方言没有 `MATCH`，自定义方言会改变其他语句的解析；见架构 §8 |
| 2026-11-01 | 3.3a：DuckDB 升到 1.5.6；Lance Space（`FORMAT = 'lance'`）整体挂到 DuckDB，读写、建索引、三种检索都交给 `lance` 扩展；检索只给主人和订阅副本、只在一个 Space 里；读最新版本，订阅用 `lance-table` 列出一个版本的文件（索引一起过去）；不加 `MODES` 声明，按格式和语句检查能力；扩展只由 `oikome extensions install` 下载；DuckDB 每条语句用新连接（扩展按连接缓存数据集） | 扩展不能按版本读、也不能列出版本文件；检索会绕过列授权和行策略；见架构 §7.6、§8、§9 |
| 2026-10-30 | 3.1d：网络阶段的完成标准在 GitHub Actions 上用三个 Docker 节点验证（其中一个只靠 mDNS 发现） | 本机没有容器环境；CI 每次推送都跑，Docker 桥接网络也能验证本机防火墙挡住的 mDNS |
| 2026-10-30 | 3.1c：节点间默认 TLS，证书由节点 DID 密钥自签并按 DID 校验；明文 http 需 `--allow-http`；libp2p 只做发现，结果写入目录的 Peers 表，`PEER` 可不写 | DID 本来就是请求签名的身份，按它校验证书不需要 CA 和域名；发现结果放目录里，`serve` 和 `sql` 两个进程都能用。见架构 §3.4、§9 |
| 2026-10-30 | 3.1b 快照订阅：只复制对方能看整张表的表，其余标 `restricted` 只能远程查；撤销只停止后续同步，已复制的文件收不回；VFS 接入推迟到 3.4 | 文件无法按列授权和行策略过滤；数据一旦复制就无法收回，写明而不是默认。见架构 §5.3、§9 |
| 2026-10-30 | Lance 作为第三种 Location 格式，用于向量、全文和大对象；分析型 Space 不再用 DuckDB vss/fts | Lance 的索引存在数据集里、随版本同步，订阅方不用重建；表格数据仍用 Iceberg + Parquet。见 §7.6 |
| 2026-11-01 | 用户端：只服务主人的个人私域数据中心，不做运维和开发者页面，成员以后做，界面统一英文（以后 i18n）；根目录新增 `apps/`（各端薄外壳）、`packages/`（共享 TypeScript）、`plugins/`、`design/`、`e2e/`，根目录 `package.json` 用 npm workspaces；目录等对应工作开始时才建；新 crate `oikome-api`（不含传输层，调用者分主人和 principal），HTTP 由 `oikome serve --web` 提供，桌面端进程内调用，以后 `oikome-mcp` 只暴露 principal 能做的操作；桌面和手机以后用 Tauri；存储位置只给本地文件夹和 NAS，网盘做存储位置待定 | 网页、桌面、手机是同一层的端，页面放共享包里各端复用；同一套处理逻辑服务所有端，AI 助手碰不到主人才有的命令；产品原则是数据不出家门，和把数据放在网盘服务商那里冲突，架构 §7.5 和 3.4 先不动，等决定。见架构 §13.3、§13.4、§14.1 |

---

*本文档是活文档。实践演变时更新它。*
*所有 Agent 每次开发会话前 **必须** 读取本文件。*
