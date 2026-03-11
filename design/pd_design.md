# PD 模块详细设计方案

## 一、背景与目标

当前 Curvine 的 master 同时承担两类职责：

- **文件系统元数据面**：inode 树、目录项、block location、mountpoints
- **集群管理/调度面**：worker 注册/心跳、worker 状态/容量、下发删除块命令等

为了解决元数据扩展性问题，独立 MetaNode 模块，引入 PD (Placement Driver) 作为集群"大脑"。MetaNode 承接原 master 的"元数据面"能力，WorkerNode 保持数据面能力不变，但需要向 PD 注册，并接受 PD 分配的 BlockGroup。

---

## 二、详细设计

### 2.1 挂载表

#### 主要数据结构

基本保持和当前结构一致，内存中维护三张索引表：

- `ufs2mountid: HashMap<String, u32>`
- `mountpath2id: HashMap<String, u32>`（key 为 cv_path）
- `mountid2entry: HashMap<u32, MountInfo>`

同时增加版本与变更时间信息：

- `mount_global_version: u64`
- `update_time: u64`
- `mount_change_log`（version -> change，后期可以用于增量同步）

#### 挂载表存储与版本同步

挂载表属于"集群核心元数据"，需要保障强一致，每个请求必须走一遍 Raft：

- mount / umount 必须走 Raft（`PdEntry::UpsertMount/DeleteMount`）
- 每次变更使 `mount_global_version += 1`，暂时不实现变更日志

**RocksDB 建议结构**：

```
mnt:entry:{mount_id} -> MountInfo
mnt:change:{global_version} -> MountChange（upsert/delete，包含 mount_id 与摘要）
```

#### 启动恢复

从 RocksDB 扫描 `mnt:entry:*` 重建三索引表（与现有 restore() 类似）

#### API 接口

**RPC（内部组件交互使用）**：

```
get_mount_table(version?) -> snapshot
get_mount_info(path|mount_id) -> MountInfo?
mount(cv_path, ufs_path, properties, req_id)
umount(mount_id|cv_path, req_id)
```

**HTTP 接口**：

```
GET /api/v1/mount
POST /api/v1/mount（创建挂载表）
DELETE /api/v1/mount
```

---

### 2.2 KV 存储

集群级别的配置信息，集群配置分为如下两种：

- **静态配置**：通过配置文件设置，重启才能生效，通常对集群整体行为影响较大
- **动态配置**：可以实时修改，节点无需重启，比如调度阈值，心跳周期等

KV 存储主要为了支持动态参数配置，满足未来的扩展需求。

#### 数据模型

```rust
ConfigItem {
    key: string,      // 比如 pd.scheduler.max_inflight_moves
    value: bytes,
    version: u64,     // 递增
    mtime: u64,
}
```

**rocksdb 存储**：使用 `cfg:item:key`

#### Key 命名规范

为保证可演进、可观测、可按前缀订阅，建议将 key 做分层命名：

- `pd.*`：PD 自身配置（如调度并发、阈值）
- `worker.*`：下发给 worker 的行为控制（如回收速率、心跳间隔覆盖）
- `metanode.*`：下发给 metanode 的行为控制（如 inode cache、锁超时）
- `quota.*` / `ufs.*` / `security.*`：扩展域

**示例**：

```
pd.scheduler.max_inflight_moves = 64
worker.gc.max_delete_blocks_per_heartbeat = 10000
metanode.lock.expire_time_ms = 30000
```

#### API 接口

**RPC 接口（内部组件使用）**：

```
GetConfig(key)
ListConfig(prefix, limit)
SetConfig(key, value)
```

**HTTP 接口**：

```
GET /api/v1/config/{key}
PUT /api/v1/config/{key}
```

---

### 2.3 节点注册与管理

WorkerNode/MetaNode 等启动后向 PD 发送 register（带静态信息：地址、标签、能力等），之后周期性发送心跳，上报节点相关的信息。

PD 维护 node lease，长期未收到心跳则将节点标记为 Lost/Offline，并根据配置触发相关的调度任务等。

这里节点的注册和心跳上报采用同一个接口，heartbeat 则根据节点角色不同进行不同的实现处理，节点注册数据通过 raft log 保障强一致性，并持久化到 rocksdb 中，heartbeat 则根据节点具体状态来决定是否走 raft log 和进行持久化。

#### 节点信息

```rust
node_id: u64                    // 全局唯一；沿用现有 worker 逻辑，自己生成唯一 id
node_type: enum {Worker, Meta, .......}
addr: {hostname, ip, rpc_port, web_port}
labels: map<string, string>     // 如 az/rack/media/group/...
epoch: u64                      // 节点重新注册递增，pd 来管理
software_version: str           // 二进制版本号，后期兼容性考虑
status: enum {Starting, Live, Decommission, Blacklist, Lost}
last_heartbeat_ms: u64
```

#### 注册请求结构

```rust
// 节点注册请求（公共部分）
pub struct RegisterRequest {
    // ========== 公共字段 ==========
    pub cluster_id: String,                      // 集群 ID（注册时校验）
    pub node_id: u64,                           // 节点 ID（节点自生成）
    pub node_type: NodeType,                    // 节点类型：Worker / Meta
    pub sofeware_version: String,              // 二进制版本号
    pub address: NodeAddress,                   // 节点地址
    pub labels: HashMap<String, String>,        // 标签（az/rack/media 等）
    pub startup_time_ms: u64,                   // 启动时间   

    // ========== 角色特定数据 ==========
    pub payload: RegisterRequestPayload,              // 不同角色的心跳详情
}

/// 节点地址
pub struct NodeAddress {
    pub hostname: String,
    pub ip: String,
    pub rpc_port: u16,
    pub web_port: u16,
}

/// 节点注册 payload
pub enum RegisterRequestPayload {
    Worker(WorkerRegisterPayload),
    Meta(MetaRegisterPayload),
}

```

#### 心跳请求响应结构

```rust
/// 节点心跳请求（公共部分）
pub struct HeartbeatRequest {
    // ========== 公共字段 ==========
    pub cluster_id: String,                      // 集群 ID
    pub node_id: u64,                           // 节点 ID
    pub node_type: NodeType,                    // 节点类型
    pub epoch: u64,                             // 当前 epoch
    pub timestamp_ms: u64,                      // 心跳时间戳
    pub address:NodeAddress
    
    // ========== 角色特定数据 ==========
    pub payload: HeartbeatPayload,              // 不同角色的心跳详情
}

/// 心跳 payload（按角色区分）
pub enum HeartbeatPayload {
    Worker(WorkerHeartbeatPayload),
    Meta(MetaHeartbeatPayload),
    // 未来扩展
}
```

```rust
/// 心跳响应（公共部分）
pub struct HeartbeatResponse {
    // ========== 公共字段 ==========
    pub error: Option<String>,              // 错误信息
    pub config_version: u64,                // 配置版本（用于通知拉取新配置）
    pub mount_version: u64,                 // 挂载表版本（用于通知拉取新挂载信息）
    pub bg_version: u64,                    // BlockGroup 版本（用于通知拉取新 BG 信息）
    pub epoch: u64,                         // 节点 epoch 信息
    
    // ========== 角色特定响应 ==========
    pub payload: HeartbeatResponsePayload,      // 不同角色的响应
}

/// 心跳响应 payload
pub enum HeartbeatResponsePayload {
    Worker(WorkerHeartbeatResponse),
    Meta(MetaHeartbeatResponse),
}
```

#### RPC 接口

```
// 节点注册
Register(RegisterRequest) -> RegisterResponse

// 节点心跳
Heartbeat(HeartbeatRequest) -> HeartbeatResponse
```

提供 RESTFUL API 方便查看当前的节点信息：

```
GET  /api/v1/node/{node_type}                   # 按类型获取节点信息列表
GET  /api/v1/node/{node_id}                     # 查询单个节点详情
POST /api/v1/node/decommission/{node_id}        # 下线节点（管理操作）
```

---

### 2.4 Worker Node 管理

WorkerNode 注册时除基本地址信息外，需要重点携带如下信息：

- **AZ 信息**：例如 az1 / az2（用于副本跨 AZ 分散，考虑支持多 AZ 容灾）
- **磁盘介质信息（media）**：例如 ssd / hdd（用于把 worker 划分到不同 pool，支撑后续冷热分层能力）

PD 在接收心跳时需要做两件事：

1. **一致性校验**：同一 worker_id 在一个 epoch 生命周期内 az/media 不允许随意变化；否则需要重新注册（epoch++）
2. **Pool 归类**：根据 labels["media"] 将 worker 归入对应 Pool，并参与后续 BlockGroup 放置与降冷迁移

这里引入 pool 概念，同一个 pool 内 worker 为同一种介质类型，从而：

- pool 内创建的 BlockGroup 为同一种存储类型
- 后续"降冷"可以通过跨 pool 迁移（例如 ssd_pool -> hdd_pool）实现

worker 节点定期的心跳上报，心跳上报请求中除了基本数据外，需要携带：

- `storage_info: repeated StorageInfo`（容量、可用、fs_used、non_fs_used、reserved、storage_type 等）
- `metrics`: 可选（IOPS、带宽、延迟、load、inflight 等）
- `blockgroups`: 当前持有的 BlockGroup 列表摘要/block数量/使用容量统计等

#### Worker 注册 Payload

```rust
/// Worker 注册详情
pub struct WorkerRegisterPayload {
    pub cpu_cores: u16,                        // CPU 核数
    pub memory: u64,                          // 内存大小

    // 存储信息
    pub storage_info: Vec<StorageInfo>,        // 各磁盘存储信息
}

/// 磁盘存储信息
pub struct StorageInfo {
    pub path: String,                          // 磁盘路径
    pub storage_type: StorageType,             // 存储类型：Mem/Ssd/Hdd
    pub capacity_bytes: u64,                   // 总容量
    pub available_bytes: u64,                  // 可用容量
    pub used_bytes: u64,                       // 已用容量
    pub reserved_bytes: u64,                   // 预留空间
}
```

#### Worker 心跳 Payload

```rust
/// Worker 心跳详情
pub struct WorkerHeartbeatPayload {
    // 存储状态
    pub storage_info: Vec<StorageInfo>,        // 各磁盘存储信息
    
    // 运行指标
    pub metrics: Option<WorkerMetrics>,        // 运行指标（可选）
    
    // BG 状态上报
    pub bg_stats: Vec<BGStats>,             // BG 状态报告    
}

/// Worker 运行指标
pub struct WorkerMetrics {
    // 系统指标
    pub cpu_usage: f32,                        // CPU 使用率
    pub memory_usage: f32,                     // 内存使用率

    // IO 指标
    pub read_iops: u64,                        // 读 IOPS
    pub write_iops: u64,                       // 写 IOPS
}

/// BG 状态报告
pub struct BGStats {
    pub bg_id: u32,                            // BG ID
    pub state: BGState,                        // BG 状态
    pub used_bytes: u64,                       // 已用空间
    pub available_bytes: u64,                  // 可用空间
    pub block_count: u64,                      // Block 数量
    
    // 副本状态（用于恢复判断）
    pub replica_status: Option<Vec<ReplicaStatus>>,
}

/// 副本状态
pub struct ReplicaStatus {
    pub worker_id: u64,                        // 副本所在 Worker
    pub is_available: bool,                    // 是否可用
    pub last_sync_ms: u64,                     // 最后同步时间
}
```

#### Worker 心跳响应 Payload

```rust
/// Worker 心跳响应详情
pub struct WorkerHeartbeatResponse {
    // BG 指令
    pub add_bgs: Vec<BlockGroupInfo>,          // 需要新增的 BG
    pub remove_bgs: Vec<u32>,                  // 需要移除的 BG ID
    pub update_bgs: Vec<BlockGroupInfo>,       // 需要更新的 BG 列表
}
```

---

### 2.5 MetaNode 管理

MetaNode 的核心目标是元数据扩展性。MetaNode 需要 PD 提供可扩展的路由能力，使 client 能在不同部署模式下正确选择 MetaNode（或 MetaRaftGroup）进行访问。

考虑到未来的扩展性，MetaNode 的"服务模式"作为可配置的注册模式：

- **MetaNodeMode::Proxy**：MetaNode 作为 proxy，元数据使用分布式 KV 存储，client 随机选一个 MetaNode 访问即可
- **MetaNodeMode::Shard**：MetaNode 带分片信息，client 必须按路径分片选择对应节点（仅作为扩展，暂不实现）
- **MetaNodeMode::Federation**：MetaNode 以 raft group 形式提供服务（leader RW、follower R），PD 维护路径表（path table）把路径映射到某个 MetaRaftGroup，client 根据路径表选择不同的 group metanode 访问

PD 中配置元数据节点模式，client 根据不同的模式，采取不同的元数据访问策略，metanode 节点的上报复用之前的注册字段，通过 label 字段来进行扩展。

#### MetaNode 注册 Payload

```rust
/// MetaNode 注册详情
pub struct MetaRegisterPayload {
    pub group_id: u64,                         // Raft Group ID
    pub is_leader: bool,                       // 是否为 Leader
    pub group_epoch: u64,                      // Group epoch（可用 raft term）
    pub peers: Vec<NodeAddress>,               // Group 成员地址
    pub rw_policy: RWPolicy,                   // 读写策略
}

/// 读写策略
pub enum RWPolicy {
    LeaderWriteFollowerRead,                   // Leader 写，Follower 可读
    LeaderOnly,                                // 仅 Leader 读写
}


```

#### MetaNode 心跳 Payload

```rust

/// MetaNode 心跳详情
pub struct MetaHeartbeatPayload {    
    pub inodes_metric: InodesMetrics
    // 运行指标
    pub sys_metrics: SystemMetrics,
    
}

/// MetaNode 元数据相关指标
pub struct InodesMetrics {
    pub inode_count: u64,                      // Inode 数量
    pub dir_count: u64,                        // 目录数量
    pub file_count: u64,                       // 文件数量
    pub total_size: u64,                       // 文件总大小
}

/// MetaNode 系统指标
pub struct SystemMetrics {
    pub cpu_usage: f32,
    pub memory_usage: f32,
}
```

#### MetaNode 心跳响应 Payload

```rust
/// MetaNode 心跳响应详情 TODO 
pub struct MetaHeartbeatResponse {
    pub path_route_update: Option<PathRouteUpdate>,
    pub node_group_update: Option<NodeGroupUpdate>,
}

/// 路由更新 TODO 
pub struct PathRouteUpdate {
    pub path_table_version: u64,               // 新版本
    pub added_paths: Vec<String>,              // 新增路径
    pub removed_paths: Vec<String>,            // 移除路径
}

/// metanode 节点更新 TODO 
pub struct NodeGroupUpdate {
    pub added_node_: Vec<String>,              // 新增路径
    pub removed_paths: Vec<String>,            // 移除路径
}

```

#### Proxy 模式（仅扩展，暂不实现）

**特性**：

- MetaNode 直接注册到 PD，PD 维护 live_meta_nodes 列表
- client 访问时随机（或轮询）选择一个 MetaNode
- 失败重试时换一个 MetaNode

#### Shard 模式（仅扩展，暂不实现）

**特性**：

- PD 维护 shard 信息，MetaNode 负责不同的 shard 元数据，比如 CubeFS metanode inode range
- client 需要根据 shard 信息选择对应 MetaNode

#### Federation 模式

基于 Single Raft Group 的元数据节点，meta node 上报需要增加如下信息（记录在 label 中）：

- `group_id: u64` - metanode 来上报，可通过配置文件配置
- `is_leader: bool` - 是否为 raft leader 节点
- `group_epoch: u64` - 成员变更/配置变更递增，可以使用 raft term
- `rw_policy: enum {LeaderWriteFollowerRead}` - 未来支持 follower read 功能

**Federation 路由表**：

对于 Federation 模式，需要管理 PathTable，路径表有如下两种实现方式，为了避免复杂的路由规则（组合模式）以及可能出现的冲突，两种规则互斥，只能配置使用其中一种方式，PD 启动时需要在配置文件指定好。

**PathRule**：

- `match: enum {Static, Hash}` - 当前只考虑静态路径和基于层级的 hash 算法
- `dir_level: u8` - 目录层级，比如指定第二层级

**静态路径映射**：

目录数量相对有限，可以动态增添，需要明确的 namespace 映射，比如 /user/a，/user/b 等。路由算法通过最长匹配原则，PD 中记录并保存该映射信息：

```rust
PathTable {
    paths: String,
    match: enum {Exact, Prefix},  // 若未来要支持"目录前缀路由"
    epoch: u64,
    group_id: u64,
}
```

**基于目录层级的 hash**：

以"第二级目录"为例（level=2），假设路径为 /a/b/c/d：

1. 取 level 对应的目录名作为 shard_key：`shard_key = "b"`
2. 计算 `idx = hash(shard_key) % groups.len()`
3. 目标 group = groups[idx]

使用该方式，需要在 PD 启动时设置，暂不支持动态切换到该模式，meta node 可以在线扩容，扩容后会请求自动路由到不同的 meta node group。

---

### 2.6 Block Group 管理

#### Pool 的核心数据结构

```rust
StoragePool {
    pool_id: u8,                    // Pool ID
    name: String,                   // 如 ssd_pool、hdd_pool
    media: String,                  // 如 ssd/hdd
    workers: set<worker_id>,        // worker 节点列表
    stats: {                        // 聚合统计
        capacity_bytes,
        available_bytes,
        used_bytes,
    },
    epoch: u64,                     // pool 配置变更版本
}
```

BlockGroup 是数据部分的最小管理单元（管理一批 block 数据），由 PD 负责管理和调度，Worker 和 BG 之间是多对多的关系。BG 关联了存储类型和副本数等 policy 策略，上层通过相同 policy 的 BG 组织起来对外提供读写。

#### BG 的核心字段（MVP）

```rust
bg_id: u32
policy: BlockGroupPolicy
replicas: u16                    // 0 表示每个节点一个副本
pool_id: u8                      // 所属的 pool id
placement: BlockGroupPlacement   // 比如必须跨 az
epoch: u64
replica_set: repeated {         
    worker_id,
}
state: enum {                   // BG 状态
    Init,
    Assigned,
    Moving,
    Degraded,
    Deleting,
}
lease_owner: optional {         // 租约持有者
    worker_id,
    lease_expire_ms,
}
stats: {                        // 来自 worker 心跳上报
    used_bytes,
    block_count,
    last_report_ms,
}
```

#### 一致性 Hash 实现

当前 BlockGroup 使用一致性 hash 来管理，同一个 hash 环上 BG 的 policy 是相同的，因此为了满足不同的副本策略需求，会创建多个 hash 环。

hash ring 采用**固定 bucket**的实现方式，bucket 和 BG 一一对应，扩缩容或节点上下线时，重算这张表来调整，而不是像经典的一致性 hash 通过加减虚拟节点。这种方式即可以实现 hash 的均匀性，也可以对节点变更时选择做一些控制。

**固定数量的 bucket**（比如 4096，可进行配置）：bucket0, bucket1, .... bucket4095

**bucket 和 bg 一一对应**：

```
bucket0, bucket1, .... bucket4095  -->  bg0, bg1, .... bg4095
```

**block id 到 bucket**：

```
bucket_id = MurmurHash(key) % bucket_count
```

#### BGTable 结构

```rust
BGTable {
    bucket: list<BlockGroup>,    // bucket 列表
    ring_epoch: u64,              // table 发生变化时，epoch 增加
}
```

BG 和 BGTable 数据走 raft log 保障强一致性，存储到 rocksdb 中。

#### WorkerNode 提供的 BG 管理接口

- `assign_blockgroup(BlockGroupInfo)` - 分配 BlockGroup
- `remove_blockgroup(BlockGroupInfo)` - 移除 BlockGroup
- `sync_blockgroup(BlockGroupInfo, WorkerNode)` - 同步 BlockGroup 中数据，用于 block 复制

#### BG 分配与心跳

BlockGroup 的分配和移除可以通过 WorkerNode heartbeat 来完成，在 response 返回需要添加的 BG 和移除的 BG 信息，workerNode 收到后开始进行相关的处理。

#### BlockGroup 状态流转

- **Init**：初始化状态，刚创建 BlockGroup
- **Assigned**：已分配到 worker 节点，BG 正常
- **Degraded**：worker 节点 lost 或 offline，副本数不足
- **Recovering**：恢复中，增加新的节点，迁移同步数据等
- **Deleting**：BG 删除中，主要用于有容量的 bg 配置

---

## 三、模块实现

本章节详细规划 node、pool、bg、cluster 四个核心模块的实现细节。

### 3.1 模块依赖关系

```
                    ┌─────────────────┐
                    │  PdServer       │
                    └────────┬────────┘
                             │
                    ┌────────▼─────────┐
                    │ ClusterManager   │
                    └────────┬─────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   ┌──────▼──────┐  ┌───────▼────────┐  ┌──────▼──────┐
   │ NodeManager │  │  PoolManager   │  │  BGManager  │
   └──────┬──────┘  └───────┬────────┘  └──────┬──────┘
          │                 │                   │
   ┌──────▼──────┐  ┌───────▼────────┐  ┌──────▼──────┐
   │ NodeIndex   │  │  PoolIndex     │  │  BGTable    │
   │ NodeStore   │  │  PoolStore     │  │  BGStore    │
   └─────────────┘  └────────────────┘  └─────────────┘
                             │
                    ┌────────▼────────┐
                    │   KvStore       │
                    │  (RocksDB)      │
                    └─────────────────┘
```

### 3.2 Node 模块

Node 模块负责节点的注册和心跳管理，维护节点信息，根据节点角色差异化处理。

#### 3.2.1 核心数据结构

**NodeInfo** - 节点信息

```rust
// curvine-common/src/state/node_info.rs
pub struct NodeInfo {
    pub node_id: u64,                           // 全局唯一，Worker 自生成
    pub node_type: NodeType,                    // Worker / Meta
    pub address: NodeAddress,                   // 地址信息
    pub labels: HashMap<String, String>,        // az/rack/media/group 等
    pub stats: HashMap<String, String>,         // load/memory/capacity 等
    pub epoch: u64,                             // 节点重注册时递增
    pub version: String,                        // 二进制版本号
    pub state: NodeState,                       // 节点状态
    pub last_heartbeat_ms: u64,                 // 最后心跳时间
}

pub enum NodeType { Worker, Meta }

pub enum NodeState {
    Starting,       // 启动中
    Live,           // 正常运行
    Decommission,   // 下线中
    Blacklist,      // 黑名单
    Lost,           // 丢失（心跳超时）
}

pub struct NodeAddress {
    pub hostname: String,
    pub ip: String,
    pub rpc_port: u16,
    pub web_port: u16,
}
```

#### 3.2.2 RocksDB 存储设计

| Key | Value | 说明 |
|-----|-------|------|
| `node:info:{node_id:u64_be}` | NodeInfo | 节点完整信息 |

#### 3.2.3 内存索引

```rust
// curvine-server/src/pd/node/index.rs
pub struct NodeIndex {
    nodes: HashMap<u64, NodeInfo>,                    // node_id -> NodeInfo
    by_type: HashMap<NodeType, HashSet<u64>>,         // type -> node_ids
    by_state: HashMap<NodeState, HashSet<u64>>,       // state -> node_ids
}

impl NodeIndex {
    pub fn insert(&mut self, node: NodeInfo) { ... }
    pub fn remove(&mut self, node_id: u64) { ... }
    pub fn get_by_id(&self, node_id: u64) -> Option<&NodeInfo> { ... }
    pub fn get_by_type(&self, node_type: NodeType) -> Vec<&NodeInfo> { ... }
    pub fn get_by_state(&self, state: NodeState) -> Vec<&NodeInfo> { ... }
}
```

#### 3.2.4 心跳处理 Trait

```rust
// curvine-server/src/pd/node/heartbeat.rs
pub trait HeartbeatHandler: Send + Sync {
    /// 处理节点注册
    fn handle_register(&self, req: RegisterRequest) -> FsResult<NodeInfo>;
    
    /// 处理节点心跳
    fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse>;
    
    /// 校验一致性（epoch、labels 等）
    fn validate_consistency(&self, node: &NodeInfo, req: &HeartbeatRequest) -> FsResult<()>;
}
```

**WorkerHeartbeatHandler 实现**：

- 校验 `labels["media"]` 与已注册信息一致
- 校验 `labels["az"]` 与已注册信息一致
- 更新存储容量统计
- 返回 BG 分配/移除指令

**MetaHeartbeatHandler 实现**：

- 解析 `labels["group_id"]`、`labels["is_leader"]`
- 维护 MetaRaftGroup 映射
- 支持目录层级 Hash 路由

#### 3.2.5 NodeManager

```rust
// curvine-server/src/pd/node/manager.rs
pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    raft_client: RaftClient,
    worker_handler: Arc<dyn HeartbeatHandler>,
    meta_handler: Arc<dyn HeartbeatHandler>,
    config_manager: Arc<ConfigManager>,
}

impl NodeManager {
    /// 节点注册（走 Raft）
    pub async fn register_node(&self, req: RegisterRequest) -> FsResult<NodeInfo>;
    
    /// 处理心跳（根据 node_type 路由）
    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse>;
    
    /// 更新节点状态（走 Raft）
    pub async fn update_node_state(&self, node_id: u64, state: NodeState) -> FsResult<()>;
    
    /// Raft apply 回调 - 注册
    pub fn apply_register_node(&self, info: &NodeInfo) -> FsResult<()>;
    
    /// Raft apply 回调 - 状态更新
    pub fn apply_update_state(&self, entry: &NodeStateEntry) -> FsResult<()>;
    
    /// 启动时恢复索引
    pub fn restore(&self) -> FsResult<()>;
    
    /// 心跳超时检测（后台任务）
    pub async fn check_timeout_loop(&self);
}
```

#### 3.2.6 节点状态流转

```
  Starting ──register──> Live
                          │
                          ├──timeout──> Lost
                          ├──admin cmd──> Blacklist
                          └──admin cmd──> Decommission
```

#### 3.2.7 文件结构

```
curvine-server/src/pd/node/
├── mod.rs              # 模块导出
├── store.rs            # NodeStore
├── index.rs            # NodeIndex
├── manager.rs          # NodeManager
├── heartbeat.rs        # HeartbeatHandler trait
├── worker_handler.rs   # WorkerHeartbeatHandler
└── meta_handler.rs     # MetaHeartbeatHandler
```

---

### 3.3 Pool 模块

Pool 模块实现存储池管理，按介质类型划分 Worker 节点。

#### 3.3.1 核心数据结构

```rust
// curvine-common/src/state/pool_info.rs
pub struct PoolInfo {
    pub pool_id: u8,                       // Pool ID
    pub name: String,                      // 如 ssd_pool、hdd_pool
    pub media: StorageType,                // Mem / Ssd / Hdd
    pub workers: HashSet<u64>,             // worker 节点列表
    pub stats: PoolStats,                  // 聚合统计
    pub epoch: u64,                        // pool 配置变更版本
}

pub struct PoolStats {
    pub capacity_bytes: i64,
    pub available_bytes: i64,
    pub used_bytes: i64,
}

pub enum StorageType {
    Mem,    // 内存
    Ssd,    // SSD
    Hdd,    // HDD
}
```

#### 3.3.2 RocksDB 存储设计

| Key | Value | 说明 |
|-----|-------|------|
| `pool:info:{pool_id:u8}` | PoolInfo | Pool 完整信息 |
| `pool:worker:{worker_id:u64_be}` | pool_id: u8 | Worker 到 Pool 的映射 |

#### 3.3.3 内存索引

```rust
// curvine-server/src/pd/pool/index.rs
pub struct PoolIndex {
    pools: HashMap<u8, PoolInfo>,               // pool_id -> PoolInfo
    by_media: HashMap<StorageType, u8>,         // media -> pool_id
    worker_to_pool: HashMap<u64, u8>,           // worker_id -> pool_id
}

impl PoolIndex {
    pub fn get_pool_by_media(&self, media: StorageType) -> Option<&PoolInfo> { ... }
    pub fn get_pool_by_worker(&self, worker_id: u64) -> Option<&PoolInfo> { ... }
    pub fn add_worker_to_pool(&mut self, worker_id: u64, pool_id: u8) { ... }
    pub fn remove_worker(&mut self, worker_id: u64) { ... }
}
```

#### 3.3.4 PoolManager

```rust
// curvine-server/src/pd/pool/manager.rs
pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    store: Arc<PoolStore>,
    node_manager: Arc<NodeManager>,
}

impl PoolManager {
    /// 将 Worker 分配到对应 Pool
    pub fn assign_worker_to_pool(&self, worker_id: u64, media: StorageType) -> FsResult<u8>;
    
    /// 根据 media 获取 Pool
    pub fn get_pool_by_media(&self, media: StorageType) -> FsResult<PoolInfo>;
    
    /// 更新 Pool 统计信息
    pub fn update_pool_stats(&self, pool_id: u8, stats: PoolStats) -> FsResult<()>;
    
    /// 列出所有 Pool
    pub fn list_pools(&self) -> Vec<PoolInfo>;
    
    /// 获取 Pool 中的 Worker 列表
    pub fn get_workers_in_pool(&self, pool_id: u8) -> Vec<u64>;
    
    /// 启动时恢复索引
    pub fn restore(&self) -> FsResult<()>;
}
```

#### 3.3.5 默认 Pool 初始化

PD 启动时自动创建三个默认 Pool：

| pool_id | name | media |
|---------|------|-------|
| 1 | mem_pool | Mem |
| 2 | ssd_pool | Ssd |
| 3 | hdd_pool | Hdd |

#### 3.3.6 文件结构

```
curvine-server/src/pd/pool/
├── mod.rs      # 模块导出
├── store.rs    # PoolStore
├── index.rs    # PoolIndex
└── manager.rs  # PoolManager
```

---

### 3.4 BG 模块

BG 模块实现 BlockGroup 管理，包括一致性 Hash 环、BGTable、状态机等。

#### 3.4.1 核心数据结构

```rust
// curvine-common/src/state/blockgroup_info.rs
pub struct BlockGroupInfo {
    pub bg_id: u32,                        // BG ID（等于 bucket_id）
    pub policy: BlockGroupPolicy,          // 存储策略
    pub replicas: u16,                     // 副本数（按 Policy 配置）
    pub pool_id: u8,                       // 所属 Pool
    pub placement: PlacementPolicy,        // 放置策略
    pub epoch: u64,                        // BG 变更版本
    pub replica_set: Vec<u64>,             // 副本所在的 worker_id 列表
    pub state: BGState,                    // BG 状态
    pub lease_owner: Option<BGLease>,      // 租约持有者
    pub stats: BGStats,                    // 统计信息
}

pub struct BlockGroupPolicy {
    pub storage_type: StorageType,         // 存储类型
}

pub enum PlacementPolicy {
    CrossAZ,       // 跨 AZ 放置
    SameAZ,        // 同 AZ 放置
}

pub enum BGState {
    Init,          // 初始化
    Assigned,      // 已分配
    Moving,        // 迁移中
    Degraded,      // 副本不足
    Recovering,    // 恢复中
    Deleting,      // 删除中
}

pub struct BGLease {
    pub worker_id: u64,
    pub lease_expire_ms: u64,
}

pub struct BGStats {
    pub used_bytes: u64,
    pub block_count: u64,
    pub last_report_ms: u64,
}
```

#### 3.4.2 一致性 Hash 实现

```rust
// curvine-server/src/pd/bg/consistent_hash.rs
pub struct ConsistentHashRing {
    bucket_count: u32,                     // 可配置，默认 4096
    buckets: Vec<BlockGroupInfo>,          // bucket 列表
    epoch: u64,                            // 环变更版本
}

impl ConsistentHashRing {
    pub fn new(bucket_count: u32) -> Self { ... }
    
    /// 根据 key 获取 BG
    pub fn get_bg(&self, key: &[u8]) -> &BlockGroupInfo {
        let bucket_id = murmur3::hash32(key) % self.bucket_count;
        &self.buckets[bucket_id as usize]
    }
    
    /// 重建 Hash 环（节点变更时调用）
    pub fn rebuild(&mut self, workers: &[u64], replicas: u16, placement: PlacementPolicy) {
        // 1. 按 replicas 分配 worker 到每个 bucket
        // 2. 考虑 placement 策略（CrossAZ / SameAZ）
        // 3. 更新 epoch
    }
    
    pub fn get_epoch(&self) -> u64 { self.epoch }
}

**副本选择策略（加权负载均衡）**：

```rust
pub struct ReplicaSelector {
    /// 选择 replicas 个节点作为副本
    pub fn select_workers(
        &self,
        pool_id: u8,
        replicas: u16,
        placement: PlacementPolicy,
        exclude_workers: &[u64],  // 排除列表
    ) -> Vec<u64> {
        let candidates = pool_manager.get_workers_in_pool(pool_id);
        
        // 计算权重
        let mut weighted_candidates: Vec<(u64, f64)> = candidates
            .into_iter()
            .filter(|w| !exclude_workers.contains(w))
            .map(|w| {
                let weight = self.calculate_weight(w);
                (w, weight)
            })
            .collect();
        
        // 按权重排序（负载越轻，权重越高）
        weighted_candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        // 考虑 placement 策略
        match placement {
            PlacementPolicy::CrossAZ => self.select_cross_az(&weighted_candidates, replicas),
            PlacementPolicy::SameAZ => self.select_same_az(&weighted_candidates, replicas),
        }
    }
    
    /// 权重计算因素：
    /// - available_bytes: 可用容量（正向）
    /// - current_bg_count: 当前 BG 数量（反向）
    /// - disk_load: 磁盘负载（反向）
    /// - network_io: 网络 IO（反向）
    fn calculate_weight(&self, worker_id: u64) -> f64 {
        let node = node_manager.get_node(worker_id).unwrap();
        let capacity = node.stats.get("available_bytes").unwrap_or(0);
        let bg_count = bg_manager.get_bg_count_on_worker(worker_id);
        let load = node.stats.get("disk_load").unwrap_or(0);
        
        // 权重公式（可配置）
        let weight = (capacity as f64 * 0.5) 
                   + (1.0 / (bg_count + 1) as f64 * 1000000.0) 
                   - (load as f64 * 0.001);
        weight
    }
}
```

**选择策略说明**：

| 因素 | 权重 | 说明 |
|------|------|------|
| 可用容量 | 50% | 容量越多权重越高 |
| BG 数量 | 30% | 当前 BG 越少权重越高 |
| 磁盘负载 | 20% | 负载越低权重越高 |

**Hash 算法**：

- 使用 MurmurHash3
- `bucket_id = MurmurHash3(key) % bucket_count`

#### 3.4.3 BGTable 结构

```rust
// curvine-server/src/pd/bg/table.rs
pub struct BGTable {
    table_id: u32,                         // Table ID（policy hash）
    policy: BlockGroupPolicy,              // 存储策略
    replicas: u16,                         // 副本数
    pool_id: u8,                           // 所属 Pool
    ring: ConsistentHashRing,              // Hash 环
    bg_index: HashMap<u32, BlockGroupInfo>,// bg_id -> BlockGroupInfo
    epoch: u64,                            // Table 版本
}

impl BGTable {
    /// 分配 BG
    pub fn allocate_bg(&self, key: &[u8]) -> &BlockGroupInfo {
        self.ring.get_bg(key)
    }
    
    /// 获取所有 Degraded 状态的 BG
    pub fn get_degraded_bgs(&self) -> Vec<&BlockGroupInfo> { ... }
    
    /// 更新 BG 状态
    pub fn update_bg_state(&mut self, bg_id: u32, state: BGState) { ... }
}
```

#### 3.4.4 RocksDB 存储设计

| Key | Value | 说明 |
|-----|-------|------|
| `bg:info:{bg_id:u32_be}` | BlockGroupInfo | BG 完整信息 |
| `bg:table:{table_id:u32_be}` | BGTable | 整个 Table（序列化） |

#### 3.4.5 BGManager

```rust
// curvine-server/src/pd/bg/manager.rs
pub struct BGManager {
    tables: Arc<RwLock<HashMap<u32, BGTable>>>,   // table_id -> BGTable
    store: Arc<BGStore>,
    raft_client: RaftClient,
    pool_manager: Arc<PoolManager>,
    node_manager: Arc<NodeManager>,
    config_manager: Arc<ConfigManager>,
}

impl BGManager {
    /// 创建 BGTable
    pub async fn create_table(&self, policy: BlockGroupPolicy, pool_id: u8, replicas: u16) -> FsResult<u32>;
    
    /// 重建 BGTable（节点变更时调用）
    /// 内置重建冷却期：30s 内只重建一次
    pub async fn rebuild_table(&self, table_id: u32) -> FsResult<()>;
    
    /// 分配 BG
    pub fn allocate_bg(&self, table_id: u32, key: &[u8]) -> FsResult<BlockGroupInfo>;
    
    /// 更新 BG 状态（Raft）
    pub async fn update_bg_state(&self, bg_id: u32, state: BGState) -> FsResult<()>;
    
    /// 批量更新 BG 状态（Worker 心跳批量上报，异步持久化）
    pub fn batch_update_bg_stats(&self, updates: Vec<BGStatUpdate>);
    
    /// Raft apply 回调
    pub fn apply_create_bg(&self, info: &BlockGroupInfo) -> FsResult<()>;
    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<()>;
    pub fn apply_delete_bg(&self, bg_id: u32) -> FsResult<()>;
    
    /// 启动时恢复
    pub fn restore(&self) -> FsResult<()>;
    
    /// 扫描 Degraded BG，触发恢复
    pub async fn check_degraded_bgs(&self);
}

/// 重建冷却器
pub struct RebuildCooler {
    last_rebuild_ms: AtomicU64,
    cooldown_ms: u64,  // 默认 30000ms
    pending_workers: Mutex<Vec<u64>>,  // 冷却期内积累的变更
}

impl RebuildCooler {
    /// 触发重建（带冷却期）
    pub async fn trigger_rebuild(&self, worker_id: u64, bg_manager: Arc<BGManager>) {
        let mut pending = self.pending_workers.lock().await;
        pending.push(worker_id);
        
        let now = current_time_ms();
        let last = self.last_rebuild_ms.load(Ordering::SeqCst);
        
        // 检查是否在冷却期内
        if now - last > self.cooldown_ms {
            // 可以重建
            self.last_rebuild_ms.store(now, Ordering::SeqCst);
            let workers = std::mem::take(&mut *pending);
            drop(pending);
            
            // 批量重建
            for table_id in bg_manager.get_all_tables() {
                bg_manager.rebuild_table_internal(table_id, &workers).await;
            }
        }
        // 冷却期内：积累到 pending_workers，等待定时任务处理
    }
    
    /// 定时任务：检查冷却期是否结束，处理 pending 变更
    pub async fn check_and_rebuild(&self, bg_manager: Arc<BGManager>) {
        let now = current_time_ms();
        let last = self.last_rebuild_ms.load(Ordering::SeqCst);
        
        if now - last > self.cooldown_ms {
            let mut pending = self.pending_workers.lock().await;
            if !pending.is_empty() {
                self.last_rebuild_ms.store(now, Ordering::SeqCst);
                let workers = std::mem::take(&mut *pending);
                drop(pending);
                
                for table_id in bg_manager.get_all_tables() {
                    bg_manager.rebuild_table_internal(table_id, &workers).await;
                }
            }
        }
    }
}
```

#### 3.4.6 BG 状态流转

```
Init ──assign──> Assigned
                     │
                     ├──worker lost──> Degraded ──recover──> Recovering ──done──> Assigned
                     │
                     ├──data migration──> Moving ──done──> Assigned
                     │
                     └──delete cmd──> Deleting
```

**状态转换触发条件**：

| 当前状态 | 目标状态 | 触发条件 |
|---------|---------|---------|
| Init | Assigned | 分配到 Worker 成功 |
| Assigned | Degraded | 副本节点 Lost |
| Assigned | Moving | 数据迁移（如负载均衡、介质降冷） |
| Degraded | Recovering | 开始恢复 |
| Recovering | Assigned | 恢复完成 |
| Moving | Assigned | 迁移完成 |
| * | Deleting | 收到删除命令 |

#### 3.4.7 文件结构

```
curvine-server/src/pd/bg/
├── mod.rs               # 模块导出
├── store.rs             # BGStore
├── table.rs             # BGTable
├── manager.rs           # BGManager
├── consistent_hash.rs   # ConsistentHashRing
├── replica_selector.rs  # ReplicaSelector
├── rebuild_cooler.rs    # RebuildCooler
└── state_machine.rs     # BGState 状态机
```

---

### 3.5 消息结构定义

#### RegisterRequest / RegisterResponse

```rust
pub struct RegisterRequest {
    pub node_type: NodeType,
    pub node_id: u64,
    pub address: NodeAddress,
    pub labels: HashMap<String, String>,
    pub stats: HashMap<String, String>,
    pub epoch: u64,
    pub version: String,
}

pub struct RegisterResponse {
    pub node_id: u64,
    pub pool_id: u8,
    pub assigned_bgs: Vec<BlockGroupInfo>,  // 初始分配的 BG
    pub status_code: u32,
    pub error_msg: Option<String>,
}
```

#### HeartbeatRequest / HeartbeatResponse

```rust
pub struct HeartbeatRequest {
    pub node_id: u64,
    pub epoch: u64,
    pub storage_info: Vec<StorageInfo>,     // 各磁盘存储信息
    pub metrics: Option<Metrics>,           // 负载/IO 指标
    pub bg_reports: Vec<BGReport>,          // BG 状态上报
}

pub struct StorageInfo {
    pub path: String,
    pub capacity_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
    pub storage_type: StorageType,
}

pub struct BGReport {
    pub bg_id: u32,
    pub used_bytes: u64,
    pub block_count: u64,
    pub state: BGState,                     // Worker 感知的 BG 状态
    pub replica_status: Vec<ReplicaStatus>, // 各副本状态
}

pub struct ReplicaStatus {
    pub worker_id: u64,
    pub is_available: bool,
    pub last_sync_ms: u64,
}

pub struct HeartbeatResponse {
    pub add_bgs: Vec<BlockGroupInfo>,       // 需要新增的 BG
    pub remove_bgs: Vec<u32>,               // 需要移除的 BG ID
    pub sync_bgs: Vec<BGSyncCommand>,       // 需要同步的 BG
    pub config_version: u64,                // 配置版本（用于通知 Worker 拉新配置）
}

pub struct BGSyncCommand {
    pub bg_id: u32,
    pub source_worker: u64,                 // 从哪个 Worker 同步
    pub priority: u8,                       // 优先级
}
```

#### BG 状态批量更新（异步）

```rust
/// Worker 心跳批量上报的 BG 状态，异步持久化
pub struct BGStatUpdate {
    pub bg_id: u32,
    pub used_bytes: u64,
    pub block_count: u64,
    pub replica_status: Vec<ReplicaStatus>,
    pub report_time_ms: u64,
}

/// BGManager 批量处理
pub struct BGBatchUpdater {
    pending_updates: Vec<BGStatUpdate>,
    last_flush_ms: u64,
    flush_interval_ms: u64,  // 5s
}

impl BGBatchUpdater {
    /// 添加待更新项
    pub fn add(&mut self, update: BGStatUpdate) {
        self.pending_updates.push(update);
        if self.should_flush() {
            self.flush();
        }
    }
    
    /// 触发持久化（批量写入 RocksDB，不走 Raft）
    fn flush(&mut self) {
        for update in &self.pending_updates {
            // 直接写入 RocksDB，更新内存索引
            // 不重要的状态变更不走 Raft
        }
        self.pending_updates.clear();
        self.last_flush_ms = current_time_ms();
    }
}
```

---

### 3.6 Cluster 模块

Cluster 模块整合 node、pool、bg，对外提供统一的 RPC 和 REST API。

#### 3.6.1 ClusterManager

```rust
// curvine-server/src/pd/cluster/manager.rs
pub struct ClusterManager {
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
}

impl ClusterManager {
    /// 处理 Worker 注册
    pub async fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<RegisterResponse> {
        // 1. NodeManager 注册节点（走 Raft）
        let node_info = self.node_manager.register_node(req.clone()).await?;
        
        // 2. PoolManager 分配到 Pool
        let media = StorageType::from_str(
            &req.labels.get("media").unwrap_or(&"ssd".to_string()))?;
        let pool_id = self.pool_manager.assign_worker_to_pool(node_info.node_id, media)?;
        
        // 3. 触发 BGTable 重建（通过 RebuildCooler，带冷却期）
        self.bg_manager.trigger_rebuild(node_info.node_id).await;
        
        // 4. 查询已分配的 BG
        let assigned_bgs = self.bg_manager.get_assigned_bgs(node_info.node_id);
        
        Ok(RegisterResponse { 
            node_id: node_info.node_id, 
            pool_id,
            assigned_bgs,
            status_code: 0,
            error_msg: None,
        })
    }
    
    /// 处理 Worker 心跳
    pub async fn handle_worker_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        // 1. NodeManager 更新心跳时间
        self.node_manager.handle_heartbeat(req.clone())?;
        
        // 2. PoolManager 更新统计（异步）
        for storage in &req.storage_info {
            self.pool_manager.update_stats_async(req.node_id, storage);
        }
        
        // 3. BGManager 批量更新 BG 状态（异步，不走 Raft）
        self.bg_manager.batch_update_bg_stats(req.bg_reports.clone());
        
        // 4. 检查是否需要 BG 指令
        let add_bgs = self.bg_manager.get_pending_add_bgs(req.node_id);
        let remove_bgs = self.bg_manager.get_pending_remove_bgs(req.node_id);
        let sync_bgs = self.bg_manager.get_pending_sync_bgs(req.node_id);
        
        // 5. 检查配置版本
        let config_version = self.config_manager.get_version();
        
        Ok(HeartbeatResponse {
            add_bgs,
            remove_bgs,
            sync_bgs,
            config_version,
        })
    }
    
    /// 处理 MetaNode 注册
    pub async fn handle_meta_register(&self, req: RegisterRequest) -> FsResult<RegisterResponse> {
        // MetaNode 注册流程类似，但不涉及 Pool/BG
        let node_info = self.node_manager.register_node(req).await?;
        Ok(RegisterResponse {
            node_id: node_info.node_id,
            pool_id: 0,  // MetaNode 不需要 Pool
            assigned_bgs: vec![],
            status_code: 0,
            error_msg: None,
        })
    }
    
    /// Worker 上线回调（节点从 Starting/Lost 转为 Live）
    pub async fn on_worker_online(&self, worker_id: u64) {
        // 触发 BGTable 重建
        self.bg_manager.trigger_rebuild(worker_id).await;
    }
    
    /// Worker 下线回调（节点标记为 Lost）
    pub async fn on_worker_offline(&self, worker_id: u64) {
        // 1. PoolManager 移除 Worker
        self.pool_manager.remove_worker(worker_id);
        
        // 2. 触发 BGTable 重建
        self.bg_manager.trigger_rebuild(worker_id).await;
        
        // 3. 标记相关 BG 为 Degraded
        self.bg_manager.mark_degraded_by_worker(worker_id).await;
    }
}
```

#### 3.6.2 Worker 注册流程

```
Worker                         PD (Leader)
  │                                │
  │──── RegisterRequest ──────────>│
  │                                │ 1. NodeManager.register_node()
  │                                │    └─> Raft propose RegisterNode
  │                                │ 2. PoolManager.assign_worker_to_pool()
  │                                │ 3. BGManager.trigger_rebuild() (带冷却期)
  │                                │ 4. 等待 Raft commit
  │                                │
  │<──── RegisterResponse ─────────│
  │                                │
```

#### 3.6.3 Worker 心跳流程

```
Worker                         PD
  │                                │
  │──── HeartbeatRequest ─────────>│
  │                                │ 1. NodeManager.handle_heartbeat()
  │                                │    ├─> 更新内存 last_heartbeat_ms
  │                                │    └─> 检查 epoch 一致性
  │                                │ 2. PoolManager.update_stats() (异步)
  │                                │ 3. BGManager.batch_update_bg_stats() (异步批量)
  │                                │ 4. 获取待处理 BG 指令
  │<──── HeartbeatResponse ────────│    ├─> add_bgs
  │                                │    ├─> remove_bgs
  │                                │    └─> sync_bgs
```

#### 3.6.4 节点 Lost 处理流程

```
PD 内部处理：
  │
  │ 1. NodeManager.check_timeout_loop()
  │    └─> 检测到心跳超时
  │        └─> update_node_state(node_id, Lost) [Raft]
  │
  │ 2. ClusterManager.on_worker_offline(worker_id)
  │    ├─> PoolManager.remove_worker(worker_id)
  │    ├─> BGManager.trigger_rebuild(worker_id)
  │    └─> BGManager.mark_degraded_by_worker(worker_id)
  │        └─> 相关 BG 状态 → Degraded
  │
  │ 3. BGManager.check_degraded_bgs() [定时任务]
  │    └─> Degraded BG → Recovering
  │        └─> ReplicaSelector 选新副本节点
  │            └─> 返回 sync_bgs 指令给 Worker
```

#### 3.6.5 BG 状态变更的两级处理

**关键变更（走 Raft，强一致）**：

- BG 创建/删除
- BG 状态变更：Init → Assigned, Assigned → Degraded, Degraded → Recovering
- 副本集变更

**非关键变更（批量异步，最终一致）**：

- BG 统计信息：used_bytes, block_count
- ReplicaStatus 上报
- 心跳时间更新

```
Worker 心跳上报                    PD 处理
  │                                    │
  ├── BGReport (统计信息) ────────────>├── BGBatchUpdater.add()
  │                                    │   └─> 内存更新 + 批量 RocksDB 写入
  ├── BGReport (状态变更 Degraded) ──>├── Raft propose UpdateBG
  │                                    │   └─> 强一致持久化
  │                                    │
```

#### 3.6.6 文件结构

```
curvine-server/src/pd/cluster/
├── mod.rs      # 模块导出
└── manager.rs  # ClusterManager
```

---

### 3.7 PdEntry 扩展

```rust
// curvine-server/src/pd/cluster/manager.rs
pub struct ClusterManager {
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
}

impl ClusterManager {
    /// 处理 Worker 注册
    pub async fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<RegisterResponse> {
        // 1. NodeManager 注册节点
        let node_info = self.node_manager.register_node(req.clone()).await?;
        
        // 2. PoolManager 分配到 Pool
        let media = StorageType::from_str(&req.labels.get("media").unwrap_or(&"ssd".to_string()))?;
        let pool_id = self.pool_manager.assign_worker_to_pool(node_info.node_id, media)?;
        
        // 3. 触发 BGTable 重建
        self.bg_manager.rebuild_table(/* ... */).await?;
        
        Ok(RegisterResponse { node_id: node_info.node_id, pool_id })
    }
    
    /// 处理 Worker 心跳
    pub fn handle_worker_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        // 1. NodeManager 更新心跳时间
        self.node_manager.handle_heartbeat(req.clone())?;
        
        // 2. PoolManager 更新统计
        // 3. BGManager 检查 BG 状态
        // 4. 返回 BG 指令
        Ok(HeartbeatResponse {
            add_bgs: vec![],
            remove_bgs: vec![],
            sync_bgs: vec![],
        })
    }
    
    /// 处理 MetaNode 注册
    pub async fn handle_meta_register(&self, req: RegisterRequest) -> FsResult<RegisterResponse>;
    
    /// Worker 上线回调
    pub async fn on_worker_online(&self, worker_id: u64);
    
    /// Worker 下线回调
    pub async fn on_worker_offline(&self, worker_id: u64);
}
```

---

### 3.8 PdEntry 扩展

```rust
// curvine-server/src/pd/journal/entry.rs
pub enum PdEntry {
    Noop,
    SetConfig(ConfigEntry),
    Mount(MountEntry),
    Unmount(u32),
    
    // 新增
    RegisterNode(NodeEntry),
    UpdateNodeState(NodeStateEntry),
    CreateBG(BGEntry),
    UpdateBG(BGUpdateEntry),
    DeleteBG(u32),
}

pub struct NodeEntry {
    pub op_ms: u64,
    pub info: NodeInfo,
}

pub struct NodeStateEntry {
    pub op_ms: u64,
    pub node_id: u64,
    pub state: NodeState,
}

pub struct BGEntry {
    pub op_ms: u64,
    pub info: BlockGroupInfo,
}

pub struct BGUpdateEntry {
    pub op_ms: u64,
    pub bg_id: u32,
    pub state: Option<BGState>,
    pub replica_set: Option<Vec<u64>>,
}
```

---

### 3.7 RPC 接口定义

| RPC Code | 名称 | 请求 | 响应 | 说明 |
|----------|------|------|------|------|
| 40 | WorkerRegister | RegisterRequest | RegisterResponse | Worker 注册 |
| 41 | WorkerHeartbeat | HeartbeatRequest | HeartbeatResponse | Worker 心跳 |
| 42 | MetaRegister | RegisterRequest | RegisterResponse | MetaNode 注册 |
| 43 | MetaHeartbeat | HeartbeatRequest | HeartbeatResponse | MetaNode 心跳 |
| 44 | AllocateBG | AllocateBGRequest | AllocateBGResponse | 分配 BG |
| 45 | GetBGTable | GetBGTableRequest | GetBGTableResponse | 获取 BGTable |
| 46 | GetNodeList | GetNodeListRequest | GetNodeListResponse | 获取节点列表 |

---

### 3.8 HTTP API

```
GET  /api/v1/node/{type}         # 查询节点列表（type: worker/meta）
GET  /api/v1/node/{id}           # 查询单个节点
GET  /api/v1/pool                # 查询 Pool 列表
GET  /api/v1/pool/{id}           # 查询单个 Pool
GET  /api/v1/bg/table/{policy}   # 查询 BGTable
POST /api/v1/bg/rebuild          # 手动触发重建
```

---

### 3.9 动态配置项

```toml
# PD 心跳配置
pd.heartbeat.timeout_ms = 60000           # 心跳超时阈值（毫秒）
pd.heartbeat.check_interval_ms = 10000    # 心跳检测间隔

# BG 配置
pd.bg.bucket_count = 4096                 # Hash 环 bucket 数量
pd.bg.rebuild_cooldown_ms = 30000         # 重建冷却期

# MetaNode 配置
pd.metanode.route_mode = "hash"           # 路由模式：static / hash
pd.metanode.hash_level = 2                # Hash 层级
```

---

### 3.10 实现步骤

#### Phase 1: 基础数据结构

1. 添加 `curvine-common/src/state/node_info.rs`
2. 添加 `curvine-common/src/state/pool_info.rs`
3. 添加 `curvine-common/src/state/blockgroup_info.rs`
4. 扩展 PdEntry 枚举
5. 实现 NodeStore、PoolStore、BGStore

#### Phase 2: Node 模块

1. 实现 NodeIndex 内存索引
2. 实现 HeartbeatHandler trait
3. 实现 WorkerHeartbeatHandler
4. 实现 MetaHeartbeatHandler
5. 实现 NodeManager
6. 集成心跳超时检测

#### Phase 3: Pool 模块

1. 实现 PoolIndex 内存索引
2. 实现 PoolManager
3. PD 启动时初始化默认 Pool

#### Phase 4: BG 模块

1. 实现 ConsistentHashRing
2. 实现 BGTable
3. 实现 BGManager
4. 实现 BG 状态机

#### Phase 5: Cluster 集成

1. 实现 ClusterManager
2. 扩展 RPC Handler
3. 扩展 HTTP Handler
4. 扩展 PdAppStorage.apply()
5. 修改 PD 启动流程

#### Phase 6: MetaNode Federation

1. 实现 PathTable 数据结构
2. 实现目录层级 Hash 路由
3. 集成到 MetaHeartbeatHandler

---

### 3.11 故障恢复详细实现

当 Worker 节点 Lost 或 BG 副本不足时，PD 需要触发故障恢复流程，确保数据可靠性。

#### 3.11.1 恢复管理器

```rust
// curvine-server/src/pd/recovery/manager.rs
pub struct RecoveryManager {
    bg_manager: Arc<BGManager>,
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    raft_client: RaftClient,
    
    // 恢复任务队列
    pending_recoveries: Mutex<Vec<RecoveryTask>>,
    in_progress_recoveries: Mutex<HashMap<u32, RecoveryTask>>, // bg_id -> task
    
    // 配置
    config: RecoveryConfig,
}

pub struct RecoveryConfig {
    pub check_interval_ms: u64,         // 检查周期 (默认 30s)
    pub max_concurrent_recoveries: u32, // 最大并发恢复数 (默认 10)
    pub recovery_timeout_ms: u64,       // 恢复超时 (默认 10min)
    pub retry_interval_ms: u64,         // 重试间隔 (默认 60s)
    pub max_retry_count: u32,           // 最大重试次数 (默认 3)
}

pub struct RecoveryTask {
    pub bg_id: u32,
    pub source_workers: Vec<u64>,       // 可用数据源
    pub target_worker: u64,             // 目标恢复节点
    pub state: RecoveryState,
    pub start_time_ms: u64,
    pub retry_count: u32,
}

pub enum RecoveryState {
    Pending,        // 等待调度
    Scheduled,      // 已调度，等待 Worker 确认
    InProgress,     // 同步进行中
    Verifying,      // 验证数据完整性
    Completed,      // 完成
    Failed,         // 失败
}
```

#### 3.11.2 故障检测流程

```rust
impl BGManager {
    /// 定时检查 Degraded BG
    pub async fn check_degraded_bgs(&self) {
        let tables = self.tables.read().await;
        
        for (_, table) in tables.iter() {
            let degraded_bgs = table.get_degraded_bgs();
            
            for bg in degraded_bgs {
                // 检查是否已经在恢复中
                if self.recovery_manager.is_in_progress(bg.bg_id) {
                    continue;
                }
                
                // 检查可用副本
                let available_replicas: Vec<u64> = bg.replica_set
                    .iter()
                    .filter(|&&w| self.is_worker_available(w))
                    .cloned()
                    .collect();
                
                if available_replicas.is_empty() {
                    // 所有副本都不可用，记录严重告警
                    log::error!("BG {} all replicas lost! Manual intervention required.", bg.bg_id);
                    self.alert_manager.send_critical_alert(
                        format!("BG {} all replicas lost", bg.bg_id)
                    ).await;
                    continue;
                }
                
                // 触发恢复
                self.recovery_manager.schedule_recovery(bg.bg_id, available_replicas).await;
            }
        }
    }
    
    /// Worker 可用性检查
    fn is_worker_available(&self, worker_id: u64) -> bool {
        match self.node_manager.get_node_state(worker_id) {
            Some(NodeState::Live) => true,
            _ => false,
        }
    }
}
```

#### 3.11.3 恢复调度算法

```rust
impl RecoveryManager {
    /// 调度恢复任务
    pub async fn schedule_recovery(
        &self, 
        bg_id: u32, 
        source_workers: Vec<u64>
    ) -> FsResult<()> {
        let bg = self.bg_manager.get_bg(bg_id).await?;
        let pool_id = bg.pool_id;
        let required_replicas = bg.replicas as usize;
        
        // 计算需要补充的副本数
        let current_replicas = source_workers.len();
        let needed_replicas = required_replicas - current_replicas;
        
        if needed_replicas == 0 {
            // 副本数已满足，直接标记为 Assigned
            self.bg_manager.update_bg_state(bg_id, BGState::Assigned).await?;
            return Ok(());
        }
        
        // 选择恢复目标节点（排除已有副本的节点）
        let exclude_workers: HashSet<u64> = bg.replica_set.iter().cloned().collect();
        let candidates = self.select_recovery_targets(
            pool_id, 
            needed_replicas, 
            &exclude_workers,
            &source_workers
        ).await?;
        
        if candidates.is_empty() {
            log::warn!("No available worker for BG {} recovery", bg_id);
            return Err(FsError::common("no available recovery target"));
        }
        
        // 创建恢复任务
        for target_worker in candidates {
            let task = RecoveryTask {
                bg_id,
                source_workers: source_workers.clone(),
                target_worker,
                state: RecoveryState::Pending,
                start_time_ms: current_time_ms(),
                retry_count: 0,
            };
            
            self.pending_recoveries.lock().await.push(task);
        }
        
        // 更新 BG 状态为 Recovering
        self.bg_manager.update_bg_state(bg_id, BGState::Recovering).await?;
        
        Ok(())
    }
    
    /// 选择恢复目标节点
    async fn select_recovery_targets(
        &self,
        pool_id: u8,
        count: usize,
        exclude_workers: &HashSet<u64>,
        source_workers: &[u64],
    ) -> FsResult<Vec<u64>> {
        let pool_workers = self.pool_manager.get_workers_in_pool(pool_id);
        
        // 过滤可用节点
        let mut candidates: Vec<(u64, f64)> = pool_workers
            .into_iter()
            .filter(|w| !exclude_workers.contains(w))
            .filter(|w| self.is_worker_available(*w))
            .map(|w| {
                let score = self.calculate_recovery_score(w, source_workers);
                (w, score)
            })
            .collect();
        
        // 按得分排序（越高越好）
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        Ok(candidates.into_iter()
            .take(count)
            .map(|(w, _)| w)
            .collect())
    }
    
    /// 计算节点恢复得分
    /// 考虑因素：
    /// - 可用容量（正向）
    /// - 当前恢复任务数（反向，避免热点）
    /// - 与数据源的网络距离（同 AZ 优先）
    fn calculate_recovery_score(&self, worker_id: u64, source_workers: &[u64]) -> f64 {
        let node = self.node_manager.get_node(worker_id).unwrap();
        
        // 容量得分
        let capacity = node.stats.get("available_bytes").unwrap_or(0);
        let capacity_score = (capacity as f64 / (1024.0 * 1024.0 * 1024.0)).min(100.0); // GB
        
        // 负载得分（当前恢复任务越少越好）
        let current_tasks = self.get_worker_recovery_count(worker_id);
        let load_score = 100.0 / (current_tasks + 1) as f64;
        
        // 网络距离得分（同 AZ 得高分）
        let network_score = if self.is_same_az(worker_id, &source_workers[0]) {
            50.0
        } else {
            0.0
        };
        
        // 综合得分
        capacity_score * 0.4 + load_score * 0.4 + network_score * 0.2
    }
}
```

#### 3.11.4 数据同步流程

```
PD (Leader)                    Target Worker                 Source Worker
  │                                │                              │
  │ 1. 发送 SyncCommand ──────────>│                              │
  │    (bg_id, source_worker)      │                              │
  │                                │                              │
  │                                │ 2. 向 Source Worker 请求数据  │
  │                                │────── GetBlockGroupData ─────>│
  │                                │                              │
  │                                │<───── BlockGroupData ────────│
  │                                │                              │
  │                                │ 3. 本地写入数据              │
  │                                │                              │
  │ 4. 心跳上报进度 ──────────────>│                              │
  │                                │                              │
  │ 5. 同步完成 ──────────────────>│                              │
  │                                │                              │
  │ 6. 更新 BG 状态 (Raft)         │                              │
  │    ReplicaSet 添加新节点       │                              │
  │                                │                              │
```

**详细流程**：

```rust
impl RecoveryManager {
    /// 执行恢复任务
    pub async fn execute_recovery(&self, task: &mut RecoveryTask) -> FsResult<()> {
        task.state = RecoveryState::Scheduled;
        
        // 1. 选择最佳数据源
        let source_worker = self.select_best_source(&task.source_workers).await?;
        
        // 2. 发送同步命令给目标 Worker
        let sync_cmd = BGSyncCommand {
            bg_id: task.bg_id,
            source_worker,
            priority: 5, // 恢复任务优先级
        };
        
        self.send_sync_command(task.target_worker, sync_cmd).await?;
        task.state = RecoveryState::InProgress;
        
        // 3. 等待同步完成（通过心跳上报进度）
        let timeout = Duration::from_millis(self.config.recovery_timeout_ms);
        let result = tokio::time::timeout(timeout, self.wait_for_completion(task.bg_id)).await;
        
        match result {
            Ok(Ok(())) => {
                // 同步成功，验证数据
                task.state = RecoveryState::Verifying;
                self.verify_recovery(task).await?;
                
                // 更新 BG 副本集
                self.add_replica_to_bg(task.bg_id, task.target_worker).await?;
                task.state = RecoveryState::Completed;
                
                log::info!("BG {} recovery completed on worker {}", 
                    task.bg_id, task.target_worker);
            }
            Ok(Err(e)) => {
                log::error!("BG {} recovery failed: {}", task.bg_id, e);
                self.handle_recovery_failure(task).await?;
            }
            Err(_) => {
                log::error!("BG {} recovery timeout", task.bg_id);
                self.handle_recovery_failure(task).await?;
            }
        }
        
        Ok(())
    }
    
    /// 处理恢复失败
    async fn handle_recovery_failure(&self, task: &mut RecoveryTask) -> FsResult<()> {
        task.retry_count += 1;
        
        if task.retry_count >= self.config.max_retry_count {
            task.state = RecoveryState::Failed;
            log::error!("BG {} recovery failed after {} retries", 
                task.bg_id, task.retry_count);
            
            // 发送告警
            self.alert_manager.send_alert(
                format!("BG {} recovery failed", task.bg_id)
            ).await;
        } else {
            // 重新放入待处理队列
            task.state = RecoveryState::Pending;
            self.pending_recoveries.lock().await.push(task.clone());
            
            log::warn!("BG {} recovery will retry ({}/{})", 
                task.bg_id, task.retry_count, self.config.max_retry_count);
        }
        
        Ok(())
    }
}
```

#### 3.11.5 Worker 端恢复处理

```rust
// Worker 端代码
impl WorkerNode {
    /// 处理 PD 下发的同步命令
    async fn handle_sync_command(&self, cmd: BGSyncCommand) -> FsResult<()> {
        log::info!("Starting BG {} sync from worker {}", 
            cmd.bg_id, cmd.source_worker);
        
        // 1. 创建 BG 目录结构
        self.create_bg_directory(cmd.bg_id).await?;
        
        // 2. 从源节点拉取数据
        let mut stream = self.connect_to_worker(cmd.source_worker).await?;
        stream.request_bg_data(cmd.bg_id).await?;
        
        // 3. 流式接收数据并写入
        let mut received_bytes = 0u64;
        let mut block_count = 0u64;
        
        while let Some(chunk) = stream.next().await {
            let block_data = chunk?;
            self.write_block(cmd.bg_id, block_data).await?;
            
            received_bytes += block_data.len() as u64;
            block_count += 1;
            
            // 每 100MB 上报一次进度
            if received_bytes % (100 * 1024 * 1024) == 0 {
                self.report_sync_progress(cmd.bg_id, received_bytes, block_count).await?;
            }
        }
        
        // 4. 数据校验
        self.verify_bg_integrity(cmd.bg_id).await?;
        
        // 5. 标记 BG 为可用
        self.activate_bg(cmd.bg_id).await?;
        
        // 6. 通知 PD 同步完成
        self.notify_sync_complete(cmd.bg_id).await?;
        
        log::info!("BG {} sync completed: {} blocks, {} bytes", 
            cmd.bg_id, block_count, received_bytes);
        
        Ok(())
    }
    
    /// 上报同步进度（通过心跳）
    async fn report_sync_progress(
        &self, 
        bg_id: u32, 
        received_bytes: u64,
        block_count: u64
    ) -> FsResult<()> {
        let report = BGReport {
            bg_id,
            used_bytes: received_bytes,
            block_count,
            state: BGState::Recovering,
            replica_status: vec![],
        };
        
        self.heartbeat_queue.add_bg_report(report);
        Ok(())
    }
}
```

#### 3.11.6 恢复状态流转

```
Pending ──schedule──> Scheduled ──send command──> InProgress 
                                                        │
                          ┌─────────────────────────────┤
                          │                             │
                          ▼                             ▼
                    Completed <──verify── Verifying   Failed (超时/失败)
                          │                             │
                          │                    ┌────────┘
                          │                    │ (retry < max)
                          ▼                    ▼
                    Assigned           Pending (重新调度)
```

#### 3.11.7 故障恢复配置

```toml
# recovery.conf
[recovery]
# 检查周期
check_interval_ms = 30000

# 最大并发恢复任务数
max_concurrent_recoveries = 10

# 单个恢复任务超时
recovery_timeout_ms = 600000  # 10分钟

# 重试配置
retry_interval_ms = 60000     # 1分钟
max_retry_count = 3

# 恢复带宽限制（Mbps）
recovery_bandwidth_limit = 100

# 是否启用自动恢复
auto_recovery_enabled = true

# 全副本丢失时的处理策略: "alert" | "best_effort"
all_replicas_lost_policy = "alert"
```

#### 3.11.8 恢复监控与告警

```rust
pub struct RecoveryMonitor {
    metrics: Arc<RecoveryMetrics>,
    alert_manager: Arc<AlertManager>,
}

impl RecoveryMonitor {
    /// 记录恢复指标
    pub fn record_recovery_metrics(&self, task: &RecoveryTask) {
        match task.state {
            RecoveryState::Completed => {
                self.metrics.recovery_success_count.inc();
                self.metrics.recovery_duration
                    .observe(current_time_ms() - task.start_time_ms);
            }
            RecoveryState::Failed => {
                self.metrics.recovery_failure_count.inc();
                
                // 发送告警
                self.alert_manager.send_alert(Alert {
                    level: AlertLevel::Warning,
                    message: format!("BG {} recovery failed", task.bg_id),
                    timestamp: current_time_ms(),
                });
            }
            _ => {}
        }
    }
    
    /// 获取恢复统计
    pub fn get_recovery_stats(&self) -> RecoveryStats {
        RecoveryStats {
            pending_count: self.get_pending_count(),
            in_progress_count: self.get_in_progress_count(),
            completed_count: self.metrics.recovery_success_count.get(),
            failed_count: self.metrics.recovery_failure_count.get(),
            average_recovery_time_ms: self.metrics.recovery_duration.mean(),
        }
    }
}
```

#### 3.11.9 文件结构

```
curvine-server/src/pd/recovery/
├── mod.rs              # 模块导出
├── manager.rs          # RecoveryManager
├── scheduler.rs        # 恢复调度算法
├── monitor.rs          # 恢复监控
└── metrics.rs          # 恢复指标
```

---

### 3.12 边界情况处理

| 场景 | 处理方式 |
|------|---------|
| Worker 重启后 epoch 未递增 | NodeManager 校验 epoch，拒绝重复注册 |
| Worker 的 media/az 配置变更 | 强制状态 → Lost，要求 epoch++ 后重新注册 |
| BG 副本节点全部 Lost | 标记 Degraded，记录严重告警，等待手动介入 |
| Hash 环重建时有并发请求 | 使用 RwLock，rebuild 获取写锁 |
| RocksDB 写入失败 | Manager 层捕获错误，拒绝 Raft 提议 |
| 恢复任务频繁失败 | 达到最大重试次数后停止，发送告警 |
| 恢复过程中源节点也 Lost | 选择其他可用源节点，或等待新源 |
| 恢复带宽占满网络 | 配置 recovery_bandwidth_limit 限速 |
