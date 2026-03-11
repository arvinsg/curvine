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
// ========== 持久化字段 ==========
node_id: u64                    // 全局唯一；沿用现有 worker 逻辑，自己生成唯一 id
node_type: enum {Worker, Meta, .......}
addr: {hostname, ip, rpc_port, web_port}
labels: map<string, string>     // 如 az/rack/media/group/... 作为扩展使用
epoch: u64                      // 节点重新注册递增，由 PD 生成和管理，首次注册时 PD 分配 epoch=1
software_version: String        // 二进制版本号，后期兼容性考虑
state: enum {Starting, Live, Offline, Decommission, Blacklist, Lost}

// ========== 非持久化字段 ==========
last_heartbeat_ms: u64          // 通过心跳实时更新，PD 重启后由下一次心跳恢复
```

**节点状态说明**：

| 状态 | 说明 |
|-----|------|
| Starting | 启动中，已注册但尚未完成初始化 |
| Live | 正常运行，心跳正常 |
| Offline | 主动下线（优雅关闭），节点主动通知 PD |
| Decommission | 下线中，正在迁移数据 |
| Blacklist | 黑名单，管理员手动标记 |
| Lost | 心跳超时丢失，被动检测 |

**epoch 管理机制**：

- epoch 由 PD 统一生成和管理，节点不传入 epoch
- 首次注册时，PD 分配 epoch = 1
- 节点重启后重新注册，PD 检测到 node_id 已存在且状态为 Lost/Offline，分配新的 epoch = old_epoch + 1
- 节点需要在本地持久化 PD 返回的 epoch，心跳时携带，用于 PD 校验一致性

#### 注册请求结构

```rust
/// 节点注册请求（公共部分）
/// 注意：epoch 不由节点传入，由 PD 生成和管理
pub struct RegisterRequest {
    // ========== 公共字段 ==========
    pub cluster_id: String,                      // 集群 ID（注册时校验）
    pub node_id: u64,                           // 节点 ID（节点自生成）
    pub node_type: NodeType,                    // 节点类型：Worker / Meta
    pub software_version: String,               // 二进制版本号
    pub address: NodeAddress,                   // 节点地址
    pub labels: HashMap<String, String>,        // 标签（az/rack/media 等）
    pub startup_time_ms: u64,                   // 启动时间   

    // ========== 角色特定数据 ==========
    pub payload: RegisterRequestPayload,        // 不同角色的注册详情
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
    pub epoch: u64,                             // 当前 epoch（来自上次 PD 返回）
    pub timestamp_ms: u64,                      // 心跳时间戳
    pub address: NodeAddress,                   // 节点地址
    
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
/// 注意：epoch 由 PD 管理并返回，节点需要持久化用于后续心跳
pub struct HeartbeatResponse {
    // ========== 公共字段 ==========
    pub error: Option<String>,              // 错误信息（非空时表示请求失败）
    pub epoch: u64,                         // 节点 epoch（由 PD 生成，节点需持久化）
    pub config_version: u64,                // 配置版本（用于通知拉取新配置）
    pub mount_version: u64,                 // 挂载表版本（用于通知拉取新挂载信息）
    pub bg_version: u64,                    // BlockGroup 版本（用于通知拉取新 BG 信息）
    
    // ========== 角色特定响应 ==========
    pub payload: HeartbeatResponsePayload,  // 不同角色的响应
}

/// 心跳响应 payload
pub enum HeartbeatResponsePayload {
    Worker(WorkerHeartbeatResponse),
    Meta(MetaHeartbeatResponse),
}
```

#### RPC 接口

```
// 节点注册（响应复用 HeartbeatResponse）
Register(RegisterRequest) -> HeartbeatResponse

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

1. **一致性校验**：同一 worker_id 在一个 epoch 生命周期内 az 和存储设备配置不允许随意变化；否则需要重新注册（epoch++）
2. **Pool 归类**：根据 `WorkerRegisterPayload.storage_info` 中的存储介质类型，将 Worker 归入对应的多个 Pool

这里引入 pool 概念，同一个 pool 内 worker 为同一种介质类型，从而：

- pool 内创建的 BlockGroup 为同一种存储类型
- 后续"降冷"可以通过跨 pool 迁移（例如 ssd_pool -> hdd_pool）实现

**注意**：一个 Worker 可以同时拥有多种存储介质，因此可以同时属于多个 Pool。

worker 节点定期的心跳上报，心跳上报请求中除了基本数据外，需要携带：

- `storage_info: repeated StorageInfo`（容量、可用、fs_used、non_fs_used、reserved、storage_type 等）
- `metrics`: 可选（IOPS、带宽、延迟、load、inflight 等）
- `blockgroups`: 当前持有的 BlockGroup 列表摘要/block数量/使用容量统计等

#### Worker 注册 Payload

```rust
/// Worker 注册详情
/// 一个 Worker 可以拥有多种存储介质，同时属于多个 Pool
pub struct WorkerRegisterPayload {
    // ========== 拓扑信息 ==========
    pub az: Option<String>,                    // 可用区（可选，用于跨 AZ 副本放置）
    pub rack: Option<String>,                  // 机架（可选，用于跨机架副本放置）
    
    // ========== 硬件信息 ==========
    pub cpu_cores: u16,                        // CPU 核数
    pub memory_bytes: u64,                     // 内存大小（字节）

    // ========== 存储信息（支持多种介质）==========
    pub storage_info: Vec<StorageInfo>,        // 各磁盘存储信息（必填，至少一个）
}

/// 存储介质类型
pub enum StorageType {
    Mem = 0,    // 内存，pool_id = 1
    Ssd = 1,    // SSD，pool_id = 2
    Hdd = 2,    // HDD，pool_id = 3
}

/// 磁盘存储信息
pub struct StorageInfo {
    pub path: String,                          // 磁盘路径（如 /data/ssd1）
    pub storage_type: StorageType,             // 存储类型：Mem/Ssd/Hdd
    pub capacity_bytes: u64,                   // 总容量
    pub available_bytes: u64,                  // 可用容量
    pub used_bytes: u64,                       // 已用容量
    pub reserved_bytes: u64,                   // 预留空间
}
```

**字段说明**：

| 字段 | 是否必填 | 用途 |
|-----|---------|-----|
| `storage_info` | 必填 | Worker 的存储设备列表，根据其中的 storage_type 决定 Worker 归属的多个 Pool |
| `az` | 可选 | 跨 AZ 副本放置策略使用，如 `az1`、`az2` |
| `rack` | 可选 | 跨机架副本放置策略使用 |

**多 Pool 归属**：

一个 Worker 可以同时拥有多种存储介质（如同时有 SSD 和 HDD），因此可以同时属于多个 Pool。例如：

- Worker A 有 2 块 SSD + 2 块 HDD → 同时属于 ssd_pool 和 hdd_pool
- Worker B 只有 SSD → 只属于 ssd_pool

#### Worker 心跳 Payload

```rust
/// Worker 心跳详情
pub struct WorkerHeartbeatPayload {
    // 存储状态
    pub storage_info: Vec<StorageInfo>,        // 各磁盘存储信息
    
    // 运行指标
    pub metrics: Option<WorkerMetrics>,        // 运行指标（可选）
    
    // BG 状态上报
    pub bg_reports: Vec<BGStatusReport>,       // BG 状态报告    
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

/// BG 状态报告（Worker 心跳上报使用）
pub struct BGStatusReport {
    pub bg_id: u32,                            // BG ID
    pub state: BGState,                        // BG 状态
    pub replica_state: ReplicaState,           // 本 Worker 上的副本状态
    pub used_bytes: u64,                       // 已用空间
    pub available_bytes: u64,                  // 可用空间
    pub block_count: u64,                      // Block 数量
    pub sync_progress: Option<SyncProgress>,   // 同步进度（可选，恢复时使用）
}

/// 副本状态（本 Worker 上的 BG 副本状态）
pub enum ReplicaState {
    Syncing,       // 数据同步中
    Ready,         // 就绪，可服务
    Failed,        // 同步失败
}

/// 同步进度（可选）
pub struct SyncProgress {
    pub total_bytes: u64,                      // 总字节数
    pub synced_bytes: u64,                     // 已同步字节数
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
    pub group_id: u64,                         // Raft Group ID（通过配置文件指定）
    pub peers: Vec<PeerInfo>,                  // Group 成员信息（包含 node_id）
    pub rw_policy: RwPolicy,                   // 读写策略
    pub isleader: bool,
}

/// 读写策略
pub enum RwPolicy {
    LeaderWriteFollowerRead,                   // Leader 写，Follower 可读
    LeaderOnly,                                // 仅 Leader 读写
}

/// Peer 信息（统一定义，用于 MetaNode Group 相关场景）
pub struct PeerInfo {
    pub node_id: u64,                          // 节点 ID
    pub address: NodeAddress,                  // 节点地址
    pub is_leader: Option<bool>,               // 是否为 Leader（注册时可不传，心跳响应中由 PD 填充）
}
```

#### MetaNode 心跳 Payload

```rust
/// MetaNode 心跳详情
pub struct MetaHeartbeatPayload {    
    pub is_leader: bool,                        // 当前是否为 Leader（动态上报）
    pub group_epoch: u64,                       // Group epoch（raft term）
    pub inodes_stats: InodesStats,          // 元数据相关指标
    pub sys_stats: SystemStats,             // 系统运行指标
}

/// MetaNode 元数据相关指标
pub struct InodesStats {
    pub inode_count: u64,                      // Inode 数量
    pub dir_count: u64,                        // 目录数量
    pub file_count: u64,                       // 文件数量
    pub total_size: u64,                       // 文件总大小
}

/// MetaNode 系统指标
pub struct SystemStats {
    pub cpu_usage: f32,
    pub memory_usage: f32,
}
```

#### MetaNode 心跳响应 Payload

```rust
/// MetaNode 心跳响应详情
pub struct MetaHeartbeatResponse {
    // 路由表更新（静态模式专用）
    pub path_route_update: Option<PathRouteUpdate>,
    
    // Group 成员变更
    pub node_group_update: Option<NodeGroupUpdate>,
    
}

/// 路由表更新（静态路径模式）
pub struct PathRouteUpdate {
    pub version: u64,                          // 路由表版本
    pub action: RouteUpdateAction,             // 更新动作
}

/// 路由更新动作
pub enum RouteUpdateAction {
    FullSync {
        routes: Vec<PathRouteEntry>,           // 全量路由表
    },
    Incremental {
        added: Vec<PathRouteEntry>,            // 新增路由
        removed: Vec<String>,                  // 移除的路径
    },
}

/// 路径路由条目（完整定义见"静态路径映射"章节）
/// 用于增量同步时的简化版本
pub struct PathRouteEntry {
    pub path: String,                          // 路径（如 /user/a）
    pub group_id: u64,                         // 目标 Group ID
    pub create_time_ms: u64,                   // 创建时间
    pub update_time_ms: u64,                   // 更新时间
}

/// Group 成员更新
pub struct NodeGroupUpdate {
    pub version: u64,                          // Group 配置版本
    pub action: NodeGroupUpdateAction,
}

/// Group 更新动作
pub enum NodeGroupUpdateAction {
    AddGroup {
        groups: Vec<NodeGroupInfo>,              // 新增的 Group 信息
    },
    RemoveGroup {
        group_ids: Vec<u64>,                     // 移除的 Group ID 列表
    },
}

/// MetaNode Group 信息
pub struct NodeGroupInfo {
    pub group_id: u64,                           // Group ID
    pub peers: Vec<PeerInfo>,                    // Group 成员列表（复用 PeerInfo，is_leader 由 PD 填充）
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

目录数量相对有限，通过 PD 动态配置，需要明确的 namespace 映射，比如 /user/a，/user/b 等。路由算法通过最长匹配原则。PD 中 MetaNode Manager 模块负责管理路由表，提供 API 进行动态增删改查。

```rust
/// 路由表
pub struct PathRouteTable {
    pub version: u64,                          // 路由表版本
    pub routes: Vec<PathRouteEntry>,           // 路由条目列表
    pub last_update_ms: u64,                   // 最后更新时间
}
```

**路由查找算法**：

采用最长前缀匹配原则，路径越长优先级越高：

```rust
impl PathRouteTable {
    /// 根据路径查找目标 Group（最长前缀匹配）
    pub fn lookup(&self, path: &str) -> Option<&PathRouteEntry> {
        let mut best_match: Option<&PathRouteEntry> = None;
        let mut best_len = 0;
        
        for entry in &self.routes {
            // 检查是否为前缀匹配
            if path.starts_with(&entry.path) && entry.path.len() > best_len {
                best_match = Some(entry);
                best_len = entry.path.len();
            }
        }
        best_match
    }
}
```

**基于目录层级的 hash**：

以"第二级目录"为例（level=2），假设路径为 /a/b/c/d：

1. 取 level 对应的目录名作为 shard_key：`shard_key = "b"`
2. 计算 `idx = hash(shard_key) % groups.len()`
3. 目标 group = groups[idx]

使用该方式，需要在 PD 启动时设置，暂不支持动态切换到该模式，meta node 可以在线扩容，扩容后会请求自动路由到不同的 meta node group。

#### PD 配置文件设置

MetaNode 的服务模式由 PD 配置文件决定，全局统一，不支持一个集群内存在多种模式。当前仅实现 Federation 模式。

**PD 配置文件示例**：

```toml
[pd]

# ==================== MetaNode 配置 ====================

# MetaNode 服务模式（当前仅支持 federation）
# - federation: MetaNode 以 Raft Group 形式提供服务
metanode.mode = "federation"

# 路由模式（federation 模式下生效）【静态配置，需重启生效】
# - static: 静态路径映射，通过 MetaNode Manager 动态配置
# - hash: 基于目录层级的 Hash 路由
metanode.route_mode = "hash"

# Hash 路由模式配置（route_mode = "hash" 时生效）【静态配置，需重启生效】
# 目录层级，用于计算 shard_key
# 例如 level = 2，路径 /user/a/file.txt 的 shard_key = "a"
metanode.hash.level = 2

# 路由表自动同步间隔（毫秒）
metanode.route_sync_interval_ms = 5000
```

**配置说明**：

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `metanode.mode` | string | "federation" | MetaNode 服务模式，当前仅支持 federation |
| `metanode.route_mode` | string | "hash" | 路由模式：static / hash（**静态配置**，需重启生效） |
| `metanode.hash.level` | u8 | 2 | Hash 路由的目录层级（**静态配置**，需重启生效） |
| `metanode.route_sync_interval_ms` | u64 | 5000 | 路由表同步间隔 |

**路由模式对比**：

| 特性 | 静态路由 (static) | Hash 路由 (hash) |
|------|------------------|------------------|
| 路径映射 | 通过 API 动态配置 | 自动计算 |
| 扩展性 | 灵活配置 | 自动扩展 |
| 适用场景 | 固定 namespace、精确控制 | 动态 namespace |
| 迁移控制 | 精确控制路由 | 自动分布 |

#### MetaNode Manager 模块

MetaNode Manager 负责 MetaNode 的注册、心跳处理、路由表管理等功能。

**核心职责**：

1. **MetaNode Group 管理**：维护 MetaNode Group 信息（成员、Leader、状态）
2. **路由表管理**：静态模式下动态管理路径路由表
3. **路由计算**：根据路由模式计算路径对应的 Group
4. **心跳处理**：处理 MetaNode 心跳，更新状态

**路由管理接口**：

```rust
/// MetaNode Manager
pub struct MetaNodeManager {
    // Group 管理
    groups: Arc<RwLock<HashMap<u64, MetaNodeGroup>>>,
    
    // 路由表（静态模式）
    route_table: Arc<RwLock<PathRouteTable>>,
    
    // 配置
    config: MetaNodeConfig,
    
    // 存储
    store: Arc<MetaNodeStore>,
}

/// MetaNode Group 信息
pub struct MetaNodeGroup {
    pub group_id: u64,
    pub peers: Vec<PeerInfo>,
    pub leader_id: u64,
    pub epoch: u64,
    pub state: GroupState,
    pub metrics: InodesStats,
}

pub enum GroupState {
    Normal,         // 正常
    Degraded,       // 降级（Leader 不可用）
    Offline,        // 离线
}

/// 路由模式配置
pub struct MetaNodeConfig {
    pub mode: MetaNodeMode,
    pub route_mode: RouteMode,
    pub hash_level: u8,
}
```

**路由表管理方法**：

```rust
impl MetaNodeManager {
    /// 添加路由条目（静态模式）
    pub async fn add_route(&self, entry: PathRouteEntry) -> FsResult<()> {
        // 1. 校验路径格式
        self.validate_path(&entry.path)?;
        
        // 2. 校验 Group 存在
        self.ensure_group_exists(entry.group_id)?;
        
        // 3. 检查冲突
        self.check_conflict(&entry)?;
        
        // 4. 持久化（走 Raft）
        self.raft_propose_route_add(entry.clone()).await?;
        
        // 5. 更新内存路由表
        let mut table = self.route_table.write().await;
        table.routes.push(entry);
        table.version += 1;
        table.last_update_ms = current_time_ms();
        
        Ok(())
    }
    
    /// 删除路由条目
    pub async fn remove_route(&self, path: &str) -> FsResult<()> {
        // 1. 持久化（走 Raft）
        self.raft_propose_route_remove(path).await?;
        
        // 2. 更新内存
        let mut table = self.route_table.write().await;
        table.routes.retain(|r| r.path != path);
        table.version += 1;
        
        Ok(())
    }
    
    /// 更新路由条目
    pub async fn update_route(&self, entry: PathRouteEntry) -> FsResult<()> {
        self.remove_route(&entry.path).await?;
        self.add_route(entry).await
    }
    
    /// 获取完整路由表
    pub async fn get_route_table(&self) -> PathRouteTable {
        self.route_table.read().await.clone()
    }
    
    /// 路由查找
    pub async fn route(&self, path: &str) -> FsResult<u64> {
        match self.config.route_mode {
            RouteMode::Static => {
                let table = self.route_table.read().await;
                table.lookup(path)
                    .map(|e| e.group_id)
                    .ok_or_else(|| FsError::not_found(format!("No route for path: {}", path)))
            }
            RouteMode::Hash => {
                let groups = self.get_active_groups().await;
                if groups.is_empty() {
                    return Err(FsError::common("No available meta group"));
                }
                
                let shard_key = self.extract_shard_key(path)?;
                let idx = murmur3_hash(&shard_key) % groups.len() as u64;
                Ok(groups[idx as usize].group_id)
            }
        }
    }
    
    /// 提取 shard_key（Hash 模式）
    fn extract_shard_key(&self, path: &str) -> FsResult<String> {
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let level = self.config.hash_level as usize;
        
        if parts.len() < level {
            return Err(FsError::invalid_param(
                format!("Path level {} exceeds path depth", level)));
        }
        
        Ok(parts[level - 1].to_string())
    }
}
```

**HTTP API**：

```
# 路由表管理（静态模式）
GET    /api/v1/meta/route                    # 获取路由表
POST   /api/v1/meta/route                    # 添加路由条目
PUT    /api/v1/meta/route                    # 更新路由条目
DELETE /api/v1/meta/route?path=/user/a       # 删除路由条目

# Group 管理
GET    /api/v1/meta/group                    # 获取所有 Group
GET    /api/v1/meta/group/{group_id}         # 获取单个 Group
```

**添加路由请求示例**：

```json
POST /api/v1/meta/route
{
    "path": "/user/a",
    "match_mode": "prefix",
    "group_id": 1
}
```

---

### 2.6 Block Group 管理

#### Pool 的核心数据结构

```rust
/// Pool 信息（部分持久化）
pub struct StoragePool {
    // ========== 持久化字段 ==========
    pub pool_id: u16,                    // Pool ID = StorageType as u8 + 1
    pub name: String,                    // 如 ssd_pool、hdd_pool
    pub media: StorageType,              // Mem / Ssd / Hdd
    pub workers: HashSet<u64>,           // worker 节点列表
    pub epoch: u64,                      // pool 配置变更版本
    
    // ========== 非持久化字段（内存计算）==========
    #[serde(skip)]
    pub stats: PoolStats,                // 聚合统计，通过心跳实时计算
}

/// Pool 统计信息（不持久化）
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
}
```

**Pool 设计说明**：

- **部分持久化**：Pool 基本信息（pool_id、name、media、workers、epoch）持久化到 RocksDB，stats 不持久化
- **pool_id 生成规则**：`pool_id = StorageType as u8 + 1`（Mem=1, Ssd=2, Hdd=3）
- **workers 持久化**：Worker 加入/离开 Pool 时，更新并持久化 workers 集合
- **stats 不持久化**：stats 通过 Worker 心跳实时聚合计算，PD 重启后由下一轮心跳恢复
- **未来扩展**：保留此结构体以支持未来自定义 Pool 的创建（如指定特定节点组成的 Pool）

BlockGroup 是数据部分的最小管理单元（管理一批 block 数据），由 PD 负责管理和调度，Worker 和 BG 之间是多对多的关系。BG 通过 table_id 关联到 BGTable，从而获取存储策略（副本数、放置策略等）。

#### BlockGroupPolicy（存储策略）

```rust
/// 存储策略（决定 BGTable 的行为）
pub struct BlockGroupPolicy {
    pub storage_type: StorageType,             // 存储介质类型
    pub replicas: u16,                         // 副本数（0 表示每个节点一个副本）
    pub placement: PlacementPolicy,            // 放置策略
}

/// 放置策略
pub enum PlacementPolicy {
    CrossAZ,       // 跨 AZ 放置（优先，不足时降级为 SameAZ）
    SameAZ,        // 同 AZ 放置（低延迟场景）
}
```

**PlacementPolicy 降级说明**：

- `CrossAZ`：优先将副本分布在不同 AZ，如果 AZ 数量不足以满足副本数要求，则自动降级为 SameAZ 策略
- `SameAZ`：所有副本放置在同一个 AZ，适用于对延迟敏感的场景

#### BG 的核心字段

```rust
/// BlockGroup 信息（部分持久化）
pub struct BlockGroupInfo {
    // ========== 持久化字段 ==========
    pub bg_id: u32,                            // 全局唯一 ID（从 1 递增）
    pub table_id: u32,                         // 所属 BGTable（通过此字段获取 policy）
    pub epoch: u64,                            // BG 变更版本
    pub replica_set: Vec<u64>,                 // 副本所在的 worker_id 列表
    pub state: BGState,                        // BG 状态
    pub lease_owner: Option<BGLease>,          // 租约持有者
    
    // ========== 非持久化字段（内存计算）==========
    #[serde(skip)]
    pub stats: BGStats,                        // 统计信息，通过心跳实时更新
}

/// BG 状态
pub enum BGState {
    Init,          // 初始化，等待分配副本
    Assigned,      // 已分配，副本就绪
    Degraded,      // 副本不足（部分节点 Lost）
    Recovering,    // 恢复中，正在补充副本
    Moving,        // 迁移中（负载均衡/降冷）
    Deleting,      // 删除中
}

/// 租约信息（持久化）
pub struct BGLease {
    pub worker_id: u64,                        // 租约持有者
    pub expire_time_ms: u64,                   // 过期时间
}

/// BG 统计信息（不持久化）
pub struct BGStats {
    pub used_bytes: u64,                       // 已用空间
    pub block_count: u64,                      // Block 数量
    pub last_report_ms: u64,                   // 最后上报时间
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `bg_id` | u32 | 全局唯一，从 1 递增生成 |
| `table_id` | u32 | 所属 BGTable，通过此字段关联获取 policy |
| `replica_set` | Vec<u64> | 副本所在的 Worker 列表 |
| `state` | enum | BG 整体状态 |
| `lease_owner` | Option | 租约持有者（用于 BG 内数据一致性） |

**lease_owner 说明**：

lease_owner 用于保障 BG 内数据一致性。BG 内的其他 replica Worker 需要向 lease_owner Worker 汇报 block 信息：

- **初始化**：BG 创建时，PD 指定 lease_owner（通常为 replica_set[0]）
- **故障处理**：当 lease_owner Worker 故障或 lease 过期时，PD 重新指定新的 lease_owner
- **更新通知**：lease_owner 变更通过心跳响应通知相关 Worker

#### 一致性 Hash 实现

当前 BlockGroup 使用一致性 hash 来管理，同一个 hash 环上 BG 的 policy 是相同的，即同一个 hash 环上的BG具有相同的副本策略，因此为了满足不同的副本策略需求，会创建多个 hash 环（即多个 BGTable）。

hash ring 采用**固定 bucket**的实现方式，bucket 和 BG 一一对应，扩缩容或节点上下线时，重算这张表来调整，而不是像经典的一致性 hash 通过加减虚拟节点。这种方式即可以实现 hash 的均匀性，也可以对节点变更时选择做一些控制。

**Bucket 本质是数组**：

bucket 就是一个数组，创建 BGTable 时连续创建指定数量的 BG，按顺序放入数组中：

```
buckets[0..4095] = [BG{bg_id=1}, BG{bg_id=2}, ..., BG{bg_id=4096}]
```

注意：bg_id 是全局唯一递增的，与 bucket 索引没有直接映射关系。例如：

- 第一个 BGTable 创建时，分配 bg_id 1~4096，放入 buckets[0~4095]
- 第二个 BGTable 创建时，分配 bg_id 4097~8192，放入 buckets[0~4095]

**Key 到 Bucket 的映射**：

```
bucket_index = MurmurHash(key) % bucket_count
bg = table.buckets[bucket_index]
```

#### BGTable 结构

```rust
/// BGTable 管理（持久化）
pub struct BGTable {
    pub table_id: u32,                         // Table ID = (pool_id << 16) | replicas
    pub policy: BlockGroupPolicy,              // 存储策略
    pub bucket_count: u32,                     // Bucket 数量（默认 4096）
    pub buckets: Vec<u32>,                     // bucket 数组，存储 bg_id
    pub epoch: u64,                            // Table 版本，变更时递增
    pub create_time_ms: u64,                   // 创建时间
    pub last_rebuild_ms: u64,                  // 最后重建时间
}
```

**table_id 计算公式**：

```rust
fn calc_table_id(pool_id: u16, replicas: u16) -> u32 {
    ((pool_id as u32) << 16) | (replicas as u32)
}

// 示例：
// Pool SSD (pool_id=2), 3副本 → table_id = 0x00020003 = 131075
// Pool HDD (pool_id=3), 1副本 → table_id = 0x00030001 = 196609
```

**BGTable 创建时机**：

PD 集群启动后，根据配置为每种有节点的 Pool 创建指定副本数的 BGTable：

1. 遍历所有 Pool 类型（Mem/Ssd/Hdd）
2. 检查 Pool 是否有在线节点，没有则跳过
3. 根据配置创建对应副本数的 BGTable（如：1 副本、3 副本）
4. 创建 BGTable 时，连续创建 bucket_count 个 BG，bg_id 全局递增

```rust
// 创建 BGTable 示例
fn create_bg_table(pool_id: u16, replicas: u16, bucket_count: u32) {
    let table_id = calc_table_id(pool_id, replicas);
    let mut buckets = Vec::with_capacity(bucket_count as usize);
    
    for _ in 0..bucket_count {
        let bg_id = next_bg_id();  // 全局递增
        let bg = create_bg(bg_id, table_id);
        buckets.push(bg_id);
    }
    
    let table = BGTable { table_id, buckets, ... };
    // 持久化到 RocksDB
}
```

**Key 到 BG 的查找**：

```rust
fn lookup_bg(table: &BGTable, key: &[u8]) -> u32 {
    let bucket_index = murmur_hash(key) % table.bucket_count;
    table.buckets[bucket_index as usize]  // 返回 bg_id
}
```

**RocksDB 存储设计**：

| Key | Value | 说明 |
|-----|-------|------|
| `bg:table:{table_id:u32_be}` | BGTable | Table 元信息（包含 buckets 数组） |
| `bg:info:{bg_id:u32_be}` | BlockGroupInfo | BG 详细信息 |
| `bg:next_id` | u32 | 下一个可用的 bg_id |

#### BG 动态分配算法

Worker 加入 Pool 后，BG 采用**动态选择**方式分配副本节点。BG 创建时，需要根据 policy 为其分配 replica_set：

#### BG 状态流转

```
                     ┌─────────────────────────────────────────┐
                     │                                         │
  Init ──assign──> Assigned                                   │
                      │                                       │
                      ├──worker lost──> Degraded ──recover──> Recovering ──sync done──> Assigned
                      │                          │                                   ▲
                      │                          └─────────────────────────────────────┘
                      │
                      ├──load balance──> Moving ──done──> Assigned
                      │
                      └──delete cmd──> Deleting ──purged──> (removed)
```

**状态转换触发条件**：

| 当前状态 | 目标状态 | 触发条件 | 处理逻辑 |
|---------|---------|---------|---------|
| Init | Assigned | 首次分配副本完成 | 所有副本进入 Ready 状态 |
| Assigned | Degraded | 副本节点 Lost | 副本数 < 预期值 |
| Degraded | Recovering | 开始恢复 | 选择新副本节点 |
| Recovering | Assigned | 恢复完成 | 新副本 Ready |
| Assigned | Moving | 触发迁移 | 负载均衡/降冷 |
| Moving | Assigned | 迁移完成 | 原副本删除 |
| * | Deleting | 删除命令 | 清理所有副本 |

**副本状态流转**：

```
  (分配) ──> Syncing ──sync done──> Ready
                 │                      │
                 └──sync failed──> Failed ──retry──> Syncing
                                          │
                                          └──remove──> (removed)
```

#### WorkerNode 提供的 BG 管理接口

Worker 节点需要实现以下接口，用于 PD 调用：

```rust
/// Worker 端 BG 管理接口
pub trait BGManagerService {
    /// 分配 BlockGroup
    /// Worker 创建 BG 目录结构，初始化元数据
    fn assign_blockgroup(&mut self, bg: BlockGroupInfo) -> FsResult<()>;
    
    /// 移除 BlockGroup
    /// Worker 删除 BG 数据和元数据
    fn remove_blockgroup(&mut self, bg_id: u32) -> FsResult<()>;
    
    /// 获取 BlockGroup 状态
    fn get_blockgroup_status(&self, bg_id: u32) -> FsResult<BGStats>;
}
```

#### BG 分配与心跳

BlockGroup 的分配和移除通过 WorkerNode 心跳来完成：

**心跳响应中的 BG 指令**：

```rust
/// Worker 心跳响应中的 BG 指令
pub struct WorkerHeartbeatResponse {
    // BG 指令
    pub add_bgs: Vec<BlockGroupInfo>,          // 需要新增的 BG（包括恢复场景）
    pub remove_bgs: Vec<u32>,                  // 需要移除的 BG ID
    pub update_bgs: Vec<BlockGroupInfo>,       // 需要更新的 BG 列表
}
```

**说明**：恢复场景也使用 `add_bgs`，Worker 收到后自动判断是否需要从其他副本同步数据。

**处理流程**：

```
Worker                              PD
  │                                  │
  │── HeartbeatRequest ─────────────>│
  │                                  │ 1. 检查 Worker 的 Pool 归属
  │                                  │ 2. 检查 BGTable 是否需要调整
  │                                  │ 3. 计算需要新增/移除的 BG
  │<── HeartbeatResponse ────────────│
  │    (add_bgs, remove_bgs, ...)    │
  │                                  │
  │── 执行 BG 操作 ─────────────────>│
  │    (add: 自动从其他副本同步)      │
  │    (remove: 清理本地数据)         │
  │                                  │
  │── 下次心跳上报 BG 状态 ─────────>│
  │    (replica_state: Syncing/Ready) │
  │                                  │
```

---

## 三、模块实现

本章节详细规划 node、pool、bg、cluster 四个核心模块的实现细节。

### 持久化策略

PD 中的数据分为**持久化数据**和**非持久化数据**两类：

**持久化到 RocksDB**：

| 模块 | 持久化内容 | 说明 |
|------|-----------|------|
| Node | NodeInfo（不含 last_heartbeat_ms）| 节点基本信息、状态、epoch 等 |
| Pool | PoolInfo（不含 stats）| Pool 基本信息、workers 集合 |
| BG | BlockGroupInfo（不含 stats）| BG 基本信息、replica_set、state、lease_owner |
| BGTable | 完整 BGTable | table_id、buckets 数组、epoch 等 |

**不持久化（内存计算）**：

| 数据 | 恢复方式 | 说明 |
|------|---------|------|
| NodeInfo.last_heartbeat_ms | 心跳时更新 | 最后心跳时间 |
| PoolStats | 聚合计算 | 通过 Worker 心跳聚合 |
| BGStats | 心跳上报 | 通过 Worker 心跳上报 |

**设计原因**：

- stats 类数据是实时统计，每次心跳都会更新，持久化增加 IO 开销但价值不大
- PD 重启后，通过下一轮心跳即可恢复所有 stats 数据
- 核心元数据（节点信息、Pool 成员、BG 副本分布）必须持久化，保证重启后快速恢复

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
          │          ┌──────┘                   │
          │          │ 依赖                      │ 依赖
          │          ▼                          ▼
          │   ┌─────────────┐           ┌─────────────┐
          │   │ NodeManager │           │ PoolManager │
          │   └─────────────┘           └─────────────┘
          │
   ┌──────▼──────┐  ┌───────────────┐   ┌─────────────┐
   │ NodeIndex   │  │  PoolIndex    │   │  BGTable    │
   │ NodeStore   │  │  PoolStore    │   │  BGStore    │
   └─────────────┘  └───────────────┘   └─────────────┘
                             │
                    ┌────────▼────────┐
                    │   KvStore       │
                    │  (RocksDB)      │
                    └─────────────────┘
```

**模块依赖关系**：

```
BGManager ──依赖──> PoolManager ──依赖──> NodeManager
```

- **BGManager**：只依赖 PoolManager，通过 Pool 获取节点选择服务
- **PoolManager**：依赖 NodeManager，获取节点详细信息
- **NodeManager**：独立管理节点注册和心跳

这种分层设计的好处：

1. BG 模块不需要了解节点细节，职责更加单一
2. 节点信息变化时只影响 Pool 层，不影响 BG 层
3. Pool 作为节点信息的门面，对外提供简化的接口

### 3.2 Node 模块

Node 模块负责节点的注册和心跳管理，维护节点信息，根据节点角色差异化处理。

#### 3.2.1 核心数据结构

**NodeInfo** - 节点信息（部分持久化）

```rust
// curvine-common/src/state/node_info.rs
pub struct NodeInfo {
    // ========== 持久化字段 ==========
    pub node_id: u64,                           // 全局唯一，Worker 自生成
    pub node_type: NodeType,                    // Worker / Meta
    pub address: NodeAddress,                   // 地址信息
    pub labels: HashMap<String, String>,        // 自定义标签（扩展用途）
    pub software_version: String,               // 二进制版本号
    pub epoch: u64,                             // 节点重注册时递增，由 PD 管理
    pub state: NodeState,                       // 节点状态
    pub startup_time_ms: u64,                   // 启动时间
    
    // ========== Worker 专用字段（持久化）==========
    pub storage_types: Option<HashSet<StorageType>>,  // 拥有的存储介质类型（Worker 必填）
    pub az: Option<String>,                     // 可用区
    pub rack: Option<String>,                   // 机架
    
    // ========== 非持久化字段 ==========
    #[serde(skip)]
    pub last_heartbeat_ms: u64,                 // 最后心跳时间，通过心跳实时更新
}

pub enum NodeType { Worker, Meta }

/// 节点状态
pub enum NodeState {
    Starting,       // 启动中，已注册但尚未完成初始化
    Live,           // 正常运行，心跳正常
    Offline,        // 主动下线（优雅关闭）
    Decommission,   // 下线中，正在迁移数据
    Blacklist,      // 黑名单，管理员手动标记
    Lost,           // 心跳超时丢失
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
    /// 返回此 Handler 支持的节点类型
    fn supported_node_type(&self) -> NodeType;
    
    /// 处理节点注册
    fn handle_register(&self, req: RegisterRequest) -> FsResult<NodeInfo>;
    
    /// 处理节点心跳
    fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse>;
    
    /// 校验一致性（epoch、labels 等）
    fn validate_consistency(&self, node: &NodeInfo, req: &HeartbeatRequest) -> FsResult<()>;
}
```

**WorkerHeartbeatHandler 实现**：

```rust
impl HeartbeatHandler for WorkerHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Worker
    }
    // ... 其他方法实现
}
```

- 校验 `storage_info` 中的介质类型与已注册信息一致（同一 epoch 内不可变更）
- 校验 `az` 与已注册信息一致（同一 epoch 内不可变更）
- 更新各存储介质的容量统计
- 返回 BG 分配/移除/更新指令

**MetaHeartbeatHandler 实现**：

```rust
impl HeartbeatHandler for MetaHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Meta
    }
    // ... 其他方法实现
}
```

- 解析 `labels["group_id"]`、`labels["is_leader"]`
- 维护 MetaRaftGroup 映射
- 支持目录层级 Hash 路由

#### 3.2.5 Handler 注册表

使用注册表模式管理不同节点类型的 Handler，便于扩展新的节点角色：

```rust
// curvine-server/src/pd/node/registry.rs
pub struct HandlerRegistry {
    handlers: HashMap<NodeType, Arc<dyn HeartbeatHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self { handlers: HashMap::new() }
    }
    
    /// 注册 Handler（自动从 Handler 获取 NodeType）
    pub fn register(&mut self, handler: Arc<dyn HeartbeatHandler>) {
        let node_type = handler.supported_node_type();
        self.handlers.insert(node_type, handler);
    }
    
    /// 获取指定类型的 Handler
    pub fn get(&self, node_type: NodeType) -> Option<Arc<dyn HeartbeatHandler>> {
        self.handlers.get(&node_type).cloned()
    }
    
    /// 检查是否支持某种节点类型
    pub fn supports(&self, node_type: NodeType) -> bool {
        self.handlers.contains_key(&node_type)
    }
}
```

**扩展新节点角色**：

新增节点角色（如 Scheduler、TaskNode）只需：

1. 在 `NodeType` 枚举中添加新类型
2. 实现 `HeartbeatHandler` trait
3. 在初始化时注册到 `HandlerRegistry`

```rust
// 示例：新增 Scheduler 角色
pub struct SchedulerHeartbeatHandler { ... }

impl HeartbeatHandler for SchedulerHeartbeatHandler {
    fn supported_node_type(&self) -> NodeType {
        NodeType::Scheduler
    }
    // ... 实现其他方法
}

// 初始化时注册
registry.register(Arc::new(SchedulerHeartbeatHandler::new()));
```

无需修改 `NodeManager` 代码。

#### 3.2.6 NodeManager

```rust
// curvine-server/src/pd/node/manager.rs
pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    raft_client: RaftClient,
    handler_registry: HandlerRegistry,     // 使用注册表替代具体的 Handler
    config_manager: Arc<ConfigManager>,
}

impl NodeManager {
    /// 创建 NodeManager
    pub fn new(
        store: Arc<NodeStore>,
        raft_client: RaftClient,
        config_manager: Arc<ConfigManager>,
    ) -> Self {
        let mut registry = HandlerRegistry::new();
        
        // 注册默认的 Handler
        registry.register(Arc::new(WorkerHeartbeatHandler::new()));
        registry.register(Arc::new(MetaHeartbeatHandler::new()));
        
        Self {
            index: Arc::new(RwLock::new(NodeIndex::new())),
            store,
            raft_client,
            handler_registry: registry,
            config_manager,
        }
    }
    
    /// 注册额外的 Handler（用于扩展）
    pub fn register_handler(&mut self, handler: Arc<dyn HeartbeatHandler>) {
        self.handler_registry.register(handler);
    }
    
    /// 节点注册（走 Raft）
    pub async fn register_node(&self, req: RegisterRequest) -> FsResult<NodeInfo> {
        // 从注册表获取对应的 Handler
        let handler = self.handler_registry.get(req.node_type)
            .ok_or_else(|| FsError::invalid_param(
                format!("unsupported node type: {:?}", req.node_type)
            ))?;
        
        handler.handle_register(req)
    }
    
    /// 处理心跳（根据 node_type 路由到对应 Handler）
    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let handler = self.handler_registry.get(req.node_type)
            .ok_or_else(|| FsError::invalid_param(
                format!("unsupported node type: {:?}", req.node_type)
            ))?;
        
        handler.handle_heartbeat(req)
    }
    
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

#### 3.2.7 节点状态流转

```
                     ┌─────────────────────────────────────────────┐
                     │                                             │
  Starting ──register──> Live                                      │
                          │                                        │
                          ├──heartbeat timeout──> Lost ──re-register──┘
                          │                         │
                          ├──graceful shutdown──> Offline ──re-register──┐
                          │                                              │
                          ├──admin cmd──> Blacklist                      │
                          │                                              │
                          └──admin cmd──> Decommission ──migrate done──> │
                                                                         │
                     ┌───────────────────────────────────────────────────┘
                     │
                     └──> Starting (epoch++)
```

**状态转换说明**：

| 转换 | 触发条件 | 说明 |
|-----|---------|------|
| Starting → Live | 节点完成初始化，首次心跳成功 | PD 分配 epoch |
| Live → Lost | 心跳超时（默认 60s） | 被动检测，触发告警 |
| Live → Offline | 节点主动发送下线通知 | 优雅关闭，不触发告警 |
| Live → Blacklist | 管理员手动标记 | 节点被禁用 |
| Live → Decommission | 管理员触发下线 | 开始数据迁移 |
| Lost/Offline → Starting | 节点重新注册 | epoch++ |

#### 3.2.8 文件结构

```
curvine-server/src/pd/node/
├── mod.rs              # 模块导出
├── store.rs            # NodeStore
├── index.rs            # NodeIndex
├── manager.rs          # NodeManager
├── registry.rs         # HandlerRegistry（Handler 注册表）
├── heartbeat.rs        # HeartbeatHandler trait
├── worker_handler.rs   # WorkerHeartbeatHandler
└── meta_handler.rs     # MetaHeartbeatHandler
```

**扩展说明**：

新增节点角色时，只需添加对应的 `xxx_handler.rs` 文件并实现 `HeartbeatHandler` trait，然后在初始化时注册即可。

---

### 3.3 Pool 模块

Pool 模块实现存储池管理，按介质类型划分 Worker 节点。Pool 基本信息持久化，stats 不持久化。

#### 3.3.1 核心数据结构

```rust
// curvine-common/src/state/pool_info.rs

/// Pool 信息（部分持久化）
pub struct PoolInfo {
    // ========== 持久化字段 ==========
    pub pool_id: u16,                      // Pool ID = StorageType as u8 + 1
    pub name: String,                      // 如 ssd_pool、hdd_pool
    pub media: StorageType,                // Mem / Ssd / Hdd
    pub workers: HashSet<u64>,             // worker 节点列表
    pub epoch: u64,                        // pool 配置变更版本
    
    // ========== 非持久化字段 ==========
    #[serde(skip)]
    pub stats: PoolStats,                  // 聚合统计（通过心跳实时计算）
}

/// Pool 统计信息（不持久化）
pub struct PoolStats {
    pub capacity_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
}

pub enum StorageType {
    Mem = 0,    // 内存，pool_id = 1
    Ssd = 1,    // SSD，pool_id = 2
    Hdd = 2,    // HDD，pool_id = 3
}
```

#### 3.3.2 RocksDB 存储设计

| Key | Value | 说明 |
|-----|-------|------|
| `pool:info:{pool_id:u16_be}` | PoolInfo（不含 stats）| Pool 基本信息 |
| `pool:worker:{worker_id:u64_be}` | Vec<u16> | Worker 到多个 Pool 的映射 |

**持久化策略**：

- Pool 基本信息（pool_id、name、media、workers、epoch）持久化
- stats 字段不持久化，启动时初始化为空，通过心跳恢复

#### 3.3.3 内存索引

```rust
// curvine-server/src/pd/pool/index.rs
pub struct PoolIndex {
    pools: HashMap<u16, PoolInfo>,              // pool_id -> PoolInfo
    by_media: HashMap<StorageType, u16>,        // media -> pool_id
    worker_to_pools: HashMap<u64, HashSet<u16>>,// worker_id -> 多个 pool_id
}

impl PoolIndex {
    pub fn get_pool_by_media(&self, media: StorageType) -> Option<&PoolInfo>;
    
    /// 获取 Worker 所属的所有 Pool
    pub fn get_pools_by_worker(&self, worker_id: u64) -> Option<&HashSet<u16>>;
    
    /// 将 Worker 加入指定 Pool
    pub fn add_worker_to_pool(&mut self, worker_id: u64, pool_id: u16);
    
    /// 从所有 Pool 移除 Worker
    pub fn remove_worker(&mut self, worker_id: u64);
    
    /// 检查 Worker 是否拥有指定类型的存储
    pub fn worker_has_storage(&self, worker_id: u64, storage_type: StorageType) -> bool;
    
    pub fn update_pool_stats(&mut self, pool_id: u16, worker_stats: &WorkerStats);
}
```

#### 3.3.4 PoolManager

```rust
// curvine-server/src/pd/pool/manager.rs
pub struct PoolManager {
    index: Arc<RwLock<PoolIndex>>,
    store: Arc<PoolStore>,                 // 持久化存储
    node_manager: Arc<NodeManager>,        // 依赖 NodeManager 获取节点信息
}

impl PoolManager {
    // ========== Pool 管理接口 ==========
    
    /// 将 Worker 分配到多个 Pool（Worker 注册时调用）
    /// 根据 storage_info 中的介质类型，自动加入对应的 Pool
    pub async fn assign_worker_to_pools(
        &self, 
        worker_id: u64, 
        storage_info: &[StorageInfo]
    ) -> FsResult<Vec<u16>> {
        let mut pool_ids = HashSet::new();
        
        // 根据 storage_info 中的介质类型，加入对应 Pool
        for info in storage_info {
            let pool_id = self.get_pool_id_by_media(info.storage_type);
            self.add_worker_to_pool(pool_id, worker_id).await?;
            pool_ids.insert(pool_id);
        }
        
        Ok(pool_ids.into_iter().collect())
    }
    
    /// Worker 下线时从所有 Pool 移除
    pub async fn remove_worker_from_pools(&self, worker_id: u64) -> FsResult<()>;
    
    /// 根据 media 获取 Pool
    pub fn get_pool_by_media(&self, media: StorageType) -> FsResult<PoolInfo>;
    
    /// 列出所有有节点的 Pool
    pub fn list_active_pools(&self) -> Vec<PoolInfo>;
    
    /// 获取 Pool 中的在线 Worker 列表
    pub fn get_workers_in_pool(&self, pool_id: u16) -> Vec<u64>;
    
    /// 启动时从 RocksDB 恢复 Pool 数据
    pub fn restore(&self) -> FsResult<()>;
    
    /// 更新 Pool 统计信息（心跳时调用，不持久化）
    pub fn update_pool_stats(&self, pool_id: u16);
    
    // ========== 供 BG 模块使用的接口（门面模式）==========
    
    /// 为 BG 选择副本节点（核心接口）
    /// 只从拥有对应存储介质的 Worker 中选择
    pub fn select_workers_for_bg(
        &self,
        pool_id: u16,
        replicas: u16,
        placement: PlacementPolicy,
        exclude_workers: &[u64],
    ) -> FsResult<Vec<u64>> {
        let pool = self.get_pool(pool_id)?;
        let storage_type = pool.media;
        
        // 只选择拥有对应介质且状态为 Live 的 Worker
        let candidates: Vec<u64> = pool.workers.iter()
            .filter(|w| self.is_worker_available(**w))
            .filter(|w| !exclude_workers.contains(w))
            .cloned()
            .collect();
        
        // 后续进行权重计算、AZ 分布选择等
        self.select_by_policy(candidates, storage_type, replicas, placement)
    }
    
    /// 检查 Worker 是否可用（Live 状态）
    pub fn is_worker_available(&self, worker_id: u64) -> bool;
    
    /// 获取 Worker 的 AZ 信息（用于跨 AZ 放置）
    pub fn get_worker_az(&self, worker_id: u64) -> Option<String>;
    
    /// 获取 Worker 指定存储类型的信息摘要
    pub fn get_worker_summary(
        &self, 
        worker_id: u64, 
        storage_type: StorageType
    ) -> Option<WorkerSummary>;
}

/// Worker 信息摘要（供 BG 模块使用，按存储类型）
pub struct WorkerSummary {
    pub worker_id: u64,
    pub storage_type: StorageType,           // 查询的存储类型
    pub az: Option<String>,
    pub available_bytes: u64,                // 该类型存储的可用空间
    pub capacity_bytes: u64,                 // 该类型存储的总容量
    pub bg_count: u64,                       // 该类型存储上的 BG 数量
    pub state: NodeState,
}
```

**设计说明**：

PoolManager 作为 NodeManager 的门面，对外提供简化的接口：

- BG 模块只需调用 `select_workers_for_bg()` 获取副本节点
- 选择时只从拥有对应存储介质的 Worker 中选择
- 节点选择逻辑（权重计算、AZ 分布）封装在 PoolManager 内部
- BG 模块不需要了解 NodeInfo 的完整结构

**多 Pool 归属的影响**：

- Worker 可同时属于多个 Pool，但选择副本时只从目标 Pool 选择
- 权重计算时使用对应存储类型的容量信息

#### 3.3.5 默认 Pool 初始化

PD 首次启动时创建三个默认 Pool（持久化）：

| pool_id | name | media | 说明 |
|---------|------|-------|------|
| 1 | mem_pool | Mem | 内存池 |
| 2 | ssd_pool | Ssd | SSD 池 |
| 3 | hdd_pool | Hdd | HDD 池 |

**启动流程**：

1. 从 RocksDB 恢复已有 Pool 数据
2. 如果是首次启动，创建三个默认 Pool 并持久化
3. 初始化内存索引
4. stats 字段初始化为空，等待心跳恢复

#### 3.3.6 文件结构

```
curvine-server/src/pd/pool/
├── mod.rs      # 模块导出
├── store.rs    # PoolStore（持久化）
├── index.rs    # PoolIndex（内存索引）
└── manager.rs  # PoolManager
```

---

### 3.4 BG 模块

BG 模块实现 BlockGroup 管理，包括一致性 Hash 环、BGTable、状态机等。

#### 3.4.1 核心数据结构

```rust
// curvine-common/src/state/blockgroup_info.rs

/// BlockGroup 信息（部分持久化）
pub struct BlockGroupInfo {
    // ========== 持久化字段 ==========
    pub bg_id: u32,                        // 全局唯一 ID（从 1 递增）
    pub table_id: u32,                     // 所属 BGTable（通过此字段获取 policy）
    pub epoch: u64,                        // BG 变更版本
    pub replica_set: Vec<u64>,             // 副本所在的 worker_id 列表
    pub state: BGState,                    // BG 状态
    pub lease_owner: Option<BGLease>,      // 租约持有者
    
    // ========== 非持久化字段 ==========
    #[serde(skip)]
    pub stats: BGStats,                    // 统计信息，通过心跳实时更新
}

/// 存储策略（持久化）
pub struct BlockGroupPolicy {
    pub storage_type: StorageType,         // 存储类型
    pub replicas: u16,                     // 副本数
    pub placement: PlacementPolicy,        // 放置策略
}

/// 放置策略
pub enum PlacementPolicy {
    CrossAZ,       // 跨 AZ 放置（优先，不足时降级为 SameAZ）
    SameAZ,        // 同 AZ 放置（低延迟场景）
}

pub enum BGState {
    Init,          // 初始化
    Assigned,      // 已分配
    Moving,        // 迁移中
    Degraded,      // 副本不足
    Recovering,    // 恢复中
    Deleting,      // 删除中
}

/// 租约信息（持久化）
pub struct BGLease {
    pub worker_id: u64,                    // 租约持有者
    pub expire_time_ms: u64,               // 过期时间
}

/// BG 统计信息（不持久化）
pub struct BGStats {
    pub used_bytes: u64,                   // 已用空间
    pub block_count: u64,                  // Block 数量
    pub last_report_ms: u64,               // 最后上报时间
}
```

#### 3.4.2 BGTable 结构

BGTable 管理一致性 Hash 环，每个 BGTable 对应一种存储策略：

```rust
// curvine-server/src/pd/bg/table.rs
pub struct BGTable {
    pub table_id: u32,                     // Table ID = (pool_id << 16) | replicas
    pub policy: BlockGroupPolicy,          // 存储策略
    pub bucket_count: u32,                 // Bucket 数量（默认 4096）
    pub buckets: Vec<u32>,                 // bucket 数组，存储 bg_id
    pub epoch: u64,                        // Table 版本
    pub create_time_ms: u64,               // 创建时间
    pub last_rebuild_ms: u64,              // 最后重建时间
}

impl BGTable {
    /// 创建 BGTable，同时创建 bucket_count 个 BG
    pub fn create(
        pool_id: u16,
        policy: BlockGroupPolicy,
        bucket_count: u32,
        bg_id_gen: &mut BGIdGenerator,
    ) -> (Self, Vec<BlockGroupInfo>);
    
    /// 根据 key 获取 bg_id
    pub fn lookup(&self, key: &[u8]) -> u32 {
        let bucket_index = murmur3::hash32(key) % self.bucket_count;
        self.buckets[bucket_index as usize]
    }
}
```

#### 3.4.3 副本选择

**设计说明**：

BG 模块不直接实现副本选择逻辑，而是委托给 PoolManager：

```rust
// BGManager 中调用 PoolManager 选择副本
impl BGManager {
    fn select_replicas_for_bg(
        &self,
        pool_id: u16,
        policy: &BlockGroupPolicy,
        exclude_workers: &[u64],
    ) -> FsResult<Vec<u64>> {
        // 直接调用 PoolManager 的接口，不需要了解节点细节
        self.pool_manager.select_workers_for_bg(
            pool_id,
            policy.replicas,
            policy.placement,
            exclude_workers,
        )
    }
}
```

副本选择的具体实现在 PoolManager 内部（见 3.3.4 节），包括：

- 权重计算（容量、负载）
- AZ 分布策略
- 降级处理

**PlacementPolicy 降级策略**：

当 `CrossAZ` 策略无法满足副本数要求时（AZ 数量不足），自动降级为按权重选择，不再强制跨 AZ。

**选择策略说明**：

| 因素 | 权重 | 说明 |
|------|------|------|
| 可用容量 | 50% | 容量越多权重越高 |
| BG 数量 | 30% | 当前 BG 越少权重越高 |
| 磁盘负载 | 20% | 负载越低权重越高 |

**Hash 算法**：

- 使用 MurmurHash3
- `bucket_index = MurmurHash3(key) % bucket_count`

#### 3.4.4 RocksDB 存储设计

| Key | Value | 说明 |
|-----|-------|------|
| `bg:table:{table_id:u32_be}` | BGTable | Table 元信息（包含 buckets 数组） |
| `bg:info:{bg_id:u32_be}` | BlockGroupInfo | BG 详细信息 |
| `bg:next_id` | u32 | 下一个可用的 bg_id |

#### 3.4.5 BGManager

```rust
// curvine-server/src/pd/bg/manager.rs
pub struct BGManager {
    tables: Arc<RwLock<HashMap<u32, BGTable>>>,   // table_id -> BGTable
    bg_index: Arc<RwLock<HashMap<u32, BlockGroupInfo>>>,  // bg_id -> BlockGroupInfo
    store: Arc<BGStore>,
    raft_client: RaftClient,
    pool_manager: Arc<PoolManager>,               // 只依赖 PoolManager，不依赖 NodeManager
    next_bg_id: AtomicU32,                        // bg_id 生成器
}

impl BGManager {
    /// 创建 BGTable（PD 启动时调用）
    /// 同时创建 bucket_count 个 BG
    pub async fn create_table(&self, policy: BlockGroupPolicy) -> FsResult<u32>;
    
    /// 重建 BGTable（节点变更时调用）
    /// 内置重建冷却期：30s 内只重建一次
    pub async fn rebuild_table(&self, table_id: u32) -> FsResult<()>;
    
    /// 根据 key 查找 BG
    pub fn lookup_bg(&self, table_id: u32, key: &[u8]) -> FsResult<&BlockGroupInfo>;
    
    /// 更新 BG 状态（Raft）
    pub async fn update_bg_state(&self, bg_id: u32, state: BGState) -> FsResult<()>;
    
    /// 更新 BG 租约持有者（Raft）
    pub async fn update_lease_owner(&self, bg_id: u32, worker_id: u64) -> FsResult<()>;
    
    /// 批量更新 BG 统计（Worker 心跳上报）
    pub fn batch_update_bg_stats(&self, updates: Vec<BGStatUpdate>);
    
    /// 选择副本节点（委托给 PoolManager）
    fn select_replicas(&self, pool_id: u16, policy: &BlockGroupPolicy) -> FsResult<Vec<u64>> {
        self.pool_manager.select_workers_for_bg(
            pool_id,
            policy.replicas,
            policy.placement,
            &[], // exclude_workers
        )
    }
    
    /// Raft apply 回调
    pub fn apply_create_table(&self, table: &BGTable) -> FsResult<()>;
    pub fn apply_create_bg(&self, info: &BlockGroupInfo) -> FsResult<()>;
    pub fn apply_update_bg(&self, entry: &BGUpdateEntry) -> FsResult<()>;
    pub fn apply_delete_bg(&self, bg_id: u32) -> FsResult<()>;
    
    /// 启动时恢复
    pub fn restore(&self) -> FsResult<()>;
    
    /// 扫描 Degraded BG，触发恢复（通过 PoolManager 检查节点可用性）
    pub async fn check_degraded_bgs(&self);
}
```

**依赖说明**：

BGManager 只依赖 PoolManager，不直接依赖 NodeManager：

- 副本选择通过 `pool_manager.select_workers_for_bg()` 完成
- 节点可用性检查通过 `pool_manager.is_worker_available()` 完成
- 节点 AZ 信息通过 `pool_manager.get_worker_az()` 获取

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
├── store.rs             # BGStore（持久化）
├── table.rs             # BGTable
├── manager.rs           # BGManager（通过 PoolManager 选择副本）
└── state_machine.rs     # BGState 状态机
```

**注意**：副本选择逻辑在 PoolManager 中实现，BGManager 只需调用 `pool_manager.select_workers_for_bg()`。

---

### 3.5 消息结构定义

消息结构与 2.3/2.4/2.5 节定义保持一致，这里汇总便于实现参考。

#### RegisterRequest（与 2.3 节一致）

```rust
/// 节点注册请求
/// 注意：epoch 不由节点传入，由 PD 生成和管理
pub struct RegisterRequest {
    pub cluster_id: String,                      // 集群 ID（注册时校验）
    pub node_id: u64,                           // 节点 ID（节点自生成）
    pub node_type: NodeType,                    // 节点类型：Worker / Meta
    pub software_version: String,               // 二进制版本号
    pub address: NodeAddress,                   // 节点地址
    pub labels: HashMap<String, String>,        // 标签（az/rack/media 等）
    pub startup_time_ms: u64,                   // 启动时间
    pub payload: RegisterRequestPayload,        // 不同角色的注册详情
}
```

#### HeartbeatRequest（与 2.3 节一致）

```rust
/// 节点心跳请求
pub struct HeartbeatRequest {
    pub cluster_id: String,                      // 集群 ID
    pub node_id: u64,                           // 节点 ID
    pub node_type: NodeType,                    // 节点类型
    pub epoch: u64,                             // 当前 epoch（来自上次 PD 返回）
    pub timestamp_ms: u64,                      // 心跳时间戳
    pub address: NodeAddress,                   // 节点地址
    pub payload: HeartbeatPayload,              // 不同角色的心跳详情
}
```

#### HeartbeatResponse（与 2.3 节一致）

```rust
/// 心跳响应
pub struct HeartbeatResponse {
    pub error: Option<String>,              // 错误信息（非空时表示请求失败）
    pub epoch: u64,                         // 节点 epoch（由 PD 生成，节点需持久化）
    pub config_version: u64,                // 配置版本
    pub mount_version: u64,                 // 挂载表版本
    pub bg_version: u64,                    // BlockGroup 版本
    pub payload: HeartbeatResponsePayload,  // 不同角色的响应
}
```

#### Payload 类型定义

```rust
/// 注册 Payload
pub enum RegisterRequestPayload {
    Worker(WorkerRegisterPayload),
    Meta(MetaRegisterPayload),
}

/// 心跳 Payload
pub enum HeartbeatPayload {
    Worker(WorkerHeartbeatPayload),
    Meta(MetaHeartbeatPayload),
}

/// 心跳响应 Payload
pub enum HeartbeatResponsePayload {
    Worker(WorkerHeartbeatResponse),
    Meta(MetaHeartbeatResponse),
}
```

#### 公共结构体

```rust
pub struct NodeAddress {
    pub hostname: String,
    pub ip: String,
    pub rpc_port: u16,
    pub web_port: u16,
}

pub struct StorageInfo {
    pub path: String,                          // 磁盘路径
    pub storage_type: StorageType,             // 存储类型：Mem/Ssd/Hdd
    pub capacity_bytes: u64,                   // 总容量
    pub available_bytes: u64,                  // 可用容量
    pub used_bytes: u64,                       // 已用容量
    pub reserved_bytes: u64,                   // 预留空间
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
    coordinator: Arc<Coordinator>,           // 调度协调器（Schedule 模块核心）
}

impl ClusterManager {
    /// 创建 ClusterManager 并启动后台任务
    pub fn new(
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
    ) -> Self {
        // 创建节点移除事件通道（供 NodeHealthChecker 通知重建）
        let (worker_removed_tx, worker_removed_rx) = tokio::sync::mpsc::unbounded_channel();
        let ctx = Arc::new(CoordinatorContext {
            node_manager: node_manager.clone(),
            pool_manager: pool_manager.clone(),
            bg_manager: bg_manager.clone(),
            config_manager: config_manager.clone(),
            worker_removed_tx,
        });
        let coordinator = Arc::new(Coordinator::new(ctx));
        
        // 启动 Coordinator 后台任务（传入节点移除事件接收端）
        coordinator.clone().run(worker_removed_rx);
        
        Self {
            node_manager,
            pool_manager,
            bg_manager,
            config_manager,
            mount_manager,
            coordinator,
        }
    }
    
    /// 处理 Worker 注册
    pub async fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        // 0. 提取 Worker Payload
        let worker_payload = match &req.payload {
            RegisterRequestPayload::Worker(p) => p,
            _ => return Err(FsError::invalid_param("expected Worker payload")),
        };
        
        // 1. NodeManager 注册节点（走 Raft，PD 分配 epoch）
        let (node_info, new_epoch) = self.node_manager.register_node(req.clone()).await?;

        // 2. PoolManager 分配到多个 Pool（根据 storage_info 中的介质类型）
        let pool_ids = self.pool_manager.assign_worker_to_pools(
            node_info.node_id,
            &worker_payload.storage_info
        ).await?;
        
        // 3. 通知 Coordinator 节点上线，调度 BGTable 重建
        //    （不立即重建，由 RebuildScheduler 根据配置和冷却期决定）
        self.coordinator.on_worker_joined(node_info.node_id, pool_ids.clone());
        
        // 4. 查询已分配的 BG
        let assigned_bgs = self.bg_manager.get_assigned_bgs(node_info.node_id);
        
        // 5. 构建响应（复用 HeartbeatResponse）
        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,  // PD 分配的 epoch，节点需持久化
            config_version: self.config_manager.get_version(),
            mount_version: self.mount_manager.get_version(),
            bg_version: self.bg_manager.get_version(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: assigned_bgs,
                remove_bgs: vec![],
                update_bgs: vec![],
            }),
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

        // 4. OperatorController 分发 Operator（检查进度，返回指令）
        let bg_commands = self.coordinator.dispatch_operators(req.node_id, &req.bg_reports);

        // 5. 获取待处理 BG 指令（合并 Operator 指令和常规指令）
        let mut add_bgs = self.bg_manager.get_pending_add_bgs(req.node_id);
        let mut remove_bgs = self.bg_manager.get_pending_remove_bgs(req.node_id);
        add_bgs.extend(bg_commands.add_bgs);
        remove_bgs.extend(bg_commands.remove_bgs);
        let update_bgs = self.bg_manager.get_pending_update_bgs(req.node_id);

        // 6. 构建响应（与 2.3 节 HeartbeatResponse 一致）
        Ok(HeartbeatResponse {
            error: None,
            epoch: 0,  // 心跳响应不修改 epoch，由节点已有值为准
            config_version: self.config_manager.get_version(),
            mount_version: self.mount_manager.get_version(),
            bg_version: self.bg_manager.get_version(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs,
                remove_bgs,
                update_bgs,
            }),
        })
    }
    
    /// 处理 MetaNode 注册
    pub async fn handle_meta_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        // MetaNode 注册流程类似，但不涉及 Pool/BG
        let (node_info, new_epoch) = self.node_manager.register_node(req).await?;
        
        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            config_version: self.config_manager.get_version(),
            mount_version: self.mount_manager.get_version(),
            bg_version: 0,  // MetaNode 不关心 BG
            payload: HeartbeatResponsePayload::Meta(MetaHeartbeatResponse {
                path_route_update: None,
                node_group_update: None,
            }),
        })
    }
    
    /// 处理节点主动下线请求
    /// TODO 节点下线需要触发 bgtable 的调整， 这里确实相关的逻辑伪代码
    pub async fn handle_offline(&self, req: OfflineRequest) -> FsResult<OfflineResponse> {
        // 1. 校验请求
        self.node_manager.validate_offline_request(&req)?;
        
        // 2. 根据 wait_migration 决定目标状态
        let target_state = if req.wait_migration {
            NodeState::Decommission
        } else {
            NodeState::Offline
        };
        
        // 3. 更新节点状态（走 Raft）
        self.node_manager.update_node_state(req.node_id, target_state).await?;
        
        // 4. 如果是 Worker，处理 BG 相关逻辑
        if req.node_type == NodeType::Worker {
            if req.wait_migration {
                // Decommission：标记 BG 需要迁移，不立即触发恢复
                let pending_bgs = self.bg_manager.get_bgs_on_worker(req.node_id);
                return Ok(OfflineResponse {
                    success: true,
                    error: None,
                    state: NodeState::Decommission,
                    pending_bgs,
                });
            } else {
                // Offline：从 Pool 移除，并通过 Coordinator 触发重建与副本恢复
                self.on_worker_offline(req.node_id).await;
            }
        }
        
        Ok(OfflineResponse {
            success: true,
            error: None,
            state: NodeState::Offline,
            pending_bgs: vec![],
        })
    }
    
    /// Worker 主动下线时调用：从 Pool 移除并通知 Coordinator 触发 BGTable 重建
    pub async fn on_worker_offline(&self, worker_id: u64) {
        let pool_ids = self.pool_manager.get_pools_by_worker(worker_id);
        for pool_id in &pool_ids {
            self.pool_manager.remove_worker(*pool_id, worker_id);
        }
        self.bg_manager.mark_degraded_by_worker(worker_id).await;
        self.coordinator.on_worker_removed(worker_id, pool_ids);
    }
}
```

#### 3.6.2 Worker 注册流程

```
Worker                         PD (Leader)
  │                                │
  │──── RegisterRequest ──────────>│
  │     (无 epoch，由 PD 分配)     │ 1. NodeManager.register_node()
  │                                │    └─> Raft propose RegisterNode (含 new_epoch)
  │                                │ 2. PoolManager.assign_worker_to_pools(storage_info)
  │                                │    └─> 根据介质类型加入多个 Pool
  │                                │ 3. Coordinator.on_worker_joined()（由 RebuildScheduler 按配置与冷却期决定是否重建）
  │                                │ 4. 等待 Raft commit
  │                                │
  │<──── HeartbeatResponse ────────│
  │     (epoch=PD分配的值，需持久化)│
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
  │                                │ 4. Coordinator.dispatch_operators()（分发 Operator 指令）
  │                                │ 5. 合并 BG 指令（add_bgs / remove_bgs / update_bgs）
  │<──── HeartbeatResponse ────────│    └─> 返回 add_bgs（含恢复分配）、remove_bgs、update_bgs
```

#### 3.6.4 节点 Lost/Offline 处理流程

**Lost 处理（心跳超时）**：

```
PD 内部处理（Schedule 模块）：
  │
  │ 1. NodeManager.check_timeout_loop()
  │    └─> 检测到心跳超时（默认 60s）
  │        └─> update_node_state(node_id, Lost) [Raft]
  │
  │ 2. NodeHealthChecker.check() [定期巡检]
  │    └─> 发现 Lost 节点
  │        ├─> 记录 Lost 时间
  │        └─> BGManager.mark_degraded_by_worker(worker_id)
  │            └─> 相关 BG 状态 → Degraded
  │
  │ 3. 节点恢复（状态变回 Live）
  │    └─> NodeHealthChecker 清理记录，无需其他操作
  │
  │ 4. 超过恢复窗口（pd.node.lost_recovery_window_ms）
  │    └─> NodeHealthChecker 处理 Offline
  │        ├─> update_node_state(node_id, Offline) [Raft]
  │        ├─> PoolManager.remove_worker(worker_id)
  │        └─> RebuildScheduler.schedule_rebuild()
  │
  │ 5. ReplicaChecker.check() [定期巡检]
  │    └─> 检测 Degraded BG
  │        └─> 生成 AddReplica + WaitSync Operator
  │            └─> OperatorController 分发到 Worker 心跳响应
```

**Offline 处理（优雅关闭）**：

```
Worker                         PD (Leader)
  │                                │
  │──── OfflineRequest ───────────>│
  │     (wait_migration=false)     │ 1. 校验 cluster_id, node_id, epoch
  │                                │ 2. update_node_state(node_id, Offline) [Raft]
  │                                │ 3. 触发 BG 恢复流程（与 Lost 相同）
  │<──── OfflineResponse ──────────│
  │     (state=Offline)            │
  │                                │
```

```
Worker                         PD (Leader)
  │                                │
  │──── OfflineRequest ───────────>│
  │     (wait_migration=true)      │ 1. 校验 cluster_id, node_id, epoch
  │                                │ 2. update_node_state(node_id, Decommission) [Raft]
  │                                │ 3. 开始数据迁移
  │<──── OfflineResponse ──────────│
  │     (state=Decommission,       │
  │      pending_bgs=[...])        │
  │                                │
  │ ... 数据迁移中，Worker 继续心跳 ...
  │                                │
  │──── 最后一次心跳（bgs 为空）──>│
  │                                │ 4. update_node_state(node_id, Offline) [Raft]
  │<──── HeartbeatResponse ────────│
  │     (state=Offline)            │
  │                                │ Worker 可以安全停止
```

**Lost vs Offline 区别**：

| 场景 | Lost | Offline (wait_migration=false) | Decommission (wait_migration=true) |
|-----|------|-------------------------------|-----------------------------------|
| 触发方式 | 被动检测（心跳超时） | 主动通知 | 主动通知 |
| 告警级别 | 高（需要关注） | 低（预期行为） | 低（预期行为） |
| 数据恢复 | 立即触发 | 立即触发 | 迁移完成后触发 |
| 节点停止时机 | 不可控 | 收到响应后 | BG 迁移完成后 |

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
├── mod.rs          # 模块导出
└── manager.rs      # ClusterManager（依赖 schedule 模块的 Coordinator）
```

---

### 3.7 PdEntry 扩展

```rust
// curvine-server/src/pd/journal/entry.rs
pub enum PdEntry {
    Noop,
    SetConfig(ConfigEntry),
    Mount(MountEntry),
    Unmount(u32),
    
    // 节点管理
    RegisterNode(NodeEntry),
    UpdateNodeState(NodeStateEntry),
    
    // BG 管理
    CreateBG(BGEntry),
    UpdateBG(BGUpdateEntry),
    DeleteBG(u32),
}

/// 节点注册 Entry
pub struct NodeEntry {
    pub op_ms: u64,
    pub info: NodeInfo,
    pub new_epoch: u64,                        // PD 分配的新 epoch
}

/// 节点状态变更 Entry
pub struct NodeStateEntry {
    pub op_ms: u64,
    pub node_id: u64,
    pub old_state: NodeState,                  // 变更前状态（用于校验）
    pub new_state: NodeState,                  // 变更后状态
    pub new_epoch: Option<u64>,                // 重新注册时分配新 epoch
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

### 3.8 RPC 接口定义

| RPC Code | 名称 | 请求 | 响应 | 说明 |
|----------|------|------|------|------|
| 40 | WorkerRegister | RegisterRequest | HeartbeatResponse | Worker 注册 |
| 41 | WorkerHeartbeat | HeartbeatRequest | HeartbeatResponse | Worker 心跳 |
| 42 | MetaRegister | RegisterRequest | HeartbeatResponse | MetaNode 注册 |
| 43 | MetaHeartbeat | HeartbeatRequest | HeartbeatResponse | MetaNode 心跳 |
| 44 | NodeOffline | OfflineRequest | OfflineResponse | 节点主动下线 |
| 45 | AllocateBG | AllocateBGRequest | AllocateBGResponse | 分配 BG |
| 46 | GetBGTable | GetBGTableRequest | GetBGTableResponse | 获取 BGTable |
| 47 | GetNodeList | GetNodeListRequest | GetNodeListResponse | 获取节点列表 |

---

### 3.9 HTTP API

```
GET  /api/v1/node/{type}         # 查询节点列表（type: worker/meta）
GET  /api/v1/node/{id}           # 查询单个节点
GET  /api/v1/pool                # 查询 Pool 列表
GET  /api/v1/pool/{id}           # 查询单个 Pool
GET  /api/v1/bg/table/{policy}   # 查询 BGTable
POST /api/v1/bg/rebuild          # 手动触发重建
```

---

### 3.10 动态配置项

```toml
# ========== 节点管理配置 ==========
pd.node.heartbeat_timeout_ms = 60000          # 心跳超时阈值（毫秒）
pd.node.heartbeat_check_interval_ms = 10000   # 心跳检测间隔
pd.node.lost_recovery_window_ms = 300000      # Lost 状态恢复窗口（5分钟）

# ========== BG 配置 ==========
pd.bg.bucket_count = 4096                     # Hash 环 bucket 数量

# ========== Schedule 模块配置 ==========
# 巡检配置
pd.schedule.patrol_interval_ms = 10000        # BG 巡检间隔（毫秒）
pd.schedule.node_check_interval_ms = 10000    # 节点健康检查间隔

# Operator 控制
pd.schedule.max_waiting_operators = 100       # 最大等待 Operator 数
pd.schedule.operator_timeout_ms = 600000      # Operator 超时（10分钟）

# BGTable 重建配置（RebuildScheduler）
pd.bg.rebuild.auto_enabled = true             # 是否启用自动重建
pd.bg.rebuild.cooldown_ms = 60000             # 重建冷却期（60秒）

# 副本恢复配置（ReplicaChecker）
pd.recovery.max_concurrent = 10               # 最大并发恢复数
```

**说明**：MetaNode 的 `route_mode`、`hash_level` 采用**静态配置**（配置文件启动时加载），不使用动态配置，见 2.5 节 MetaNode 配置说明。

---

### 3.11 实现步骤

#### Phase 1: 基础数据结构

1. 添加 `curvine-common/src/state/node_info.rs`
2. 添加 `curvine-common/src/state/pool_info.rs`
3. 添加 `curvine-common/src/state/blockgroup_info.rs`
4. 扩展 PdEntry 枚举
5. 实现 NodeStore、PoolStore、BGStore

#### Phase 2: Node 模块

1. 实现 NodeIndex 内存索引
2. 实现 HeartbeatHandler trait（含 `supported_node_type()` 方法）
3. 实现 HandlerRegistry（Handler 注册表）
4. 实现 WorkerHeartbeatHandler
5. 实现 MetaHeartbeatHandler
6. 实现 NodeManager（使用 HandlerRegistry）
7. 集成心跳超时检测（标记 Lost 状态）

#### Phase 3: Pool 模块

1. 实现 PoolIndex 内存索引
2. 实现 PoolManager
3. PD 启动时初始化默认 Pool

#### Phase 4: BG 模块

1. 实现 ConsistentHashRing
2. 实现 BGTable
3. 实现 BGManager
4. 实现 BG 状态机

#### Phase 5: Schedule 模块

1. 实现 Operator 基础结构（BGOperator、OpStep、OpStatus、RebuildReason）
2. 实现 OperatorController（Operator 管理、限速、分发、进度与超时检查）
3. 实现 Checker 接口和基础 Checker：
   - NodeHealthChecker（处理 Lost 节点，超时后 Offline 并发送 worker_removed 事件）
   - ReplicaChecker（检查副本数量，生成 AddReplica Operator）
   - LeaseChecker（检查 Lease 有效性，生成 TransferLease Operator）
4. 实现 CheckerController（管理所有 Checker，定期 patrol）
5. 实现 Scheduler 接口和 RebuildScheduler（BGTable 重建调度，冷却期与合并请求）
6. 实现 Coordinator（调度协调器，创建 worker_removed 通道、启动各循环与事件消费）
7. 预留与 ClusterManager 的集成接口（Phase 6 中完成集成）

#### Phase 6: Cluster 集成

1. 实现 ClusterManager（创建 worker_removed channel、CoordinatorContext、Coordinator，并调用 coordinator.run(rx)）
2. 在 handle_worker_register 中调用 coordinator.on_worker_joined；在 handle_worker_heartbeat 中调用 coordinator.dispatch_operators；在 on_worker_offline 中调用 coordinator.on_worker_removed
3. 扩展 RPC Handler（Worker/Meta 注册与心跳、Offline）
4. 扩展 HTTP Handler（配置、状态查询等）
5. 扩展 PdAppStorage.apply()（Node/Pool/BG 等 PdEntry）
6. 修改 PD 启动流程，初始化 ClusterManager 并启动 Coordinator 后台任务

#### Phase 7: MetaNode Federation

1. 实现 PathTable 数据结构
2. 实现目录层级 Hash 路由
3. 集成到 MetaHeartbeatHandler

---

### 3.12 Schedule 模块

Schedule 模块是 PD 的调度核心，负责集群的自动巡检、故障恢复、负载均衡等自动化任务。设计参考了 TiKV PD 的调度模块，引入 Checker（巡检器）、Scheduler（调度器）、Operator（调度操作）等核心概念。

#### 3.12.1 设计理念

**模块化调度架构**：
- **Checker（巡检器）**：定期巡检集群状态，发现异常并生成 Operator
- **Scheduler（调度器）**：响应特定事件，执行复杂的调度策略
- **Operator（操作）**：表示一个 BG 上的调度操作，由多个 OpStep 组成
- **OperatorController**：管理 Operator 生命周期、限速、分发

**增量调度 vs 全量重建**：
- Curvine 使用一致性 Hash，BG 分布由 BGTable（Hash 环）确定
- 节点变化时需要重建 Hash 环（全量操作），而非增量调度
- Operator 主要用于：副本恢复、Lease 迁移、数据同步跟踪

**动态配置**：
- 所有调度参数通过 ConfigManager 管理，运行时可调整
- 支持动态开关自动重建、调整巡检间隔等

#### 3.12.2 核心概念

##### Operator（调度操作）

Operator 表示对单个 BG 的调度操作，由多个 OpStep 组成：

```rust
// curvine-server/src/pd/schedule/operator.rs

/// 调度操作状态
#[derive(Clone, Debug, PartialEq)]
pub enum OpStatus {
    Pending,          // 等待执行
    Running,          // 执行中
    Success,          // 成功完成
    Failed,           // 执行失败
    Timeout,          // 执行超时
    Cancelled,        // 已取消
}

/// 调度操作
pub struct BGOperator {
    pub id: u64,                          // Operator ID
    pub bg_id: u32,                        // 操作的 BG
    pub description: String,               // 描述
    pub steps: Vec<OpStep>,                // 操作步骤
    pub current_step: usize,               // 当前执行步骤
    pub status: OpStatus,                  // 状态
    pub create_time_ms: u64,               // 创建时间
    pub priority: u32,                     // 优先级（数值越大越高）
}

/// 操作步骤
#[derive(Clone, Debug)]
pub enum OpStep {
    /// 添加副本到指定 Worker
    AddReplica {
        worker_id: u64,
    },
    /// 从指定 Worker 移除副本
    RemoveReplica {
        worker_id: u64,
    },
    /// 转移 Lease 到指定 Worker
    TransferLease {
        from_worker: u64,
        to_worker: u64,
    },
    /// 等待副本同步完成
    WaitSync {
        worker_id: u64,
    },
    /// 重建 BGTable（Hash 环变更）
    RebuildTable {
        pool_id: u16,
        reason: RebuildReason,
    },
}

/// 重建原因
#[derive(Clone, Debug)]
pub enum RebuildReason {
    NodeJoined { node_ids: Vec<u64> },     // 节点上线
    NodeRemoved { node_ids: Vec<u64> },    // 节点移除
    Manual,                                 // 手动触发
}
```

##### Checker（巡检器）

Checker 定期巡检集群状态，发现异常并生成 Operator：

```rust
// curvine-server/src/pd/schedule/checker.rs

/// Checker 接口
pub trait Checker: Send + Sync {
    /// Checker 名称
    fn name(&self) -> &str;
    
    /// 执行巡检，返回生成的 Operator
    fn check(&self, ctx: &CheckerContext) -> Vec<BGOperator>;
    
    /// 巡检间隔（毫秒）
    fn interval_ms(&self) -> u64;
}

/// 巡检上下文
pub struct CheckerContext<'a> {
    pub pool_manager: &'a PoolManager,
    pub bg_manager: &'a BGManager,
    pub node_manager: &'a NodeManager,
    pub config_manager: &'a ConfigManager,
}
```

##### Scheduler（调度器）

Scheduler 响应特定事件，执行复杂的调度策略：

```rust
// curvine-server/src/pd/schedule/scheduler.rs

/// Scheduler 接口
pub trait Scheduler: Send + Sync {
    /// Scheduler 名称
    fn name(&self) -> &str;
    
    /// 是否允许执行
    fn is_allowed(&self, ctx: &SchedulerContext) -> bool;
    
    /// 执行调度，返回生成的 Operator
    fn schedule(&self, ctx: &SchedulerContext) -> Vec<BGOperator>;
}

/// 调度上下文
pub struct SchedulerContext<'a> {
    pub pool_manager: &'a PoolManager,
    pub bg_manager: &'a BGManager,
    pub node_manager: &'a NodeManager,
    pub config_manager: &'a ConfigManager,
}
```

#### 3.12.3 核心组件

##### OperatorController

管理 Operator 的生命周期、限速和分发：

```rust
// curvine-server/src/pd/schedule/operator_controller.rs

pub struct OperatorController {
    /// 等待执行的 Operator 队列
    waiting_operators: Mutex<BinaryHeap<PriorityOperator>>,
    
    /// 正在执行的 Operator（key = bg_id）
    running_operators: DashMap<u32, BGOperator>,
    
    /// 配置管理器
    config_manager: Arc<ConfigManager>,
    
    /// 操作 ID 生成器
    next_op_id: AtomicU64,
}

impl OperatorController {
    /// 添加 Operator 到等待队列
    pub fn add_operator(&self, op: BGOperator) -> bool {
        let max_waiting = self.config_manager
            .get_u32("pd.schedule.max_waiting_operators")
            .unwrap_or(100);
        
        let mut queue = self.waiting_operators.lock();
        
        // 检查是否已存在该 BG 的 Operator
        if self.running_operators.contains_key(&op.bg_id) {
            return false;
        }
        
        // 检查队列是否已满
        if queue.len() >= max_waiting as usize {
            return false;
        }
        
        queue.push(PriorityOperator(op));
        true
    }
    
    /// 调度下一批 Operator
    pub fn dispatch_next(&self) -> Vec<BGOperator> {
        let mut queue = self.waiting_operators.lock();
        let mut to_dispatch = Vec::new();
        
        while let Some(PriorityOperator(op)) = queue.pop() {
            // 检查该 BG 是否已有 Operator 在执行
            if self.running_operators.contains_key(&op.bg_id) {
                continue;
            }
            
            self.running_operators.insert(op.bg_id, op.clone());
            to_dispatch.push(op);
        }
        
        to_dispatch
    }
    
    /// 分发 Operator 指令到 Worker（通过心跳响应）
    pub fn dispatch_to_worker(&self, worker_id: u64, bg_reports: &[BGStatusReport]) -> BGCommands {
        let mut add_bgs = Vec::new();
        let mut remove_bgs = Vec::new();
        
        for (_, op) in self.running_operators.iter() {
            if let Some(step) = op.steps.get(op.current_step) {
                match step {
                    OpStep::AddReplica { worker_id: target } if *target == worker_id => {
                        if let Ok(bg_info) = self.get_bg_info(op.bg_id) {
                            add_bgs.push(bg_info);
                        }
                    }
                    OpStep::RemoveReplica { worker_id: target } if *target == worker_id => {
                        remove_bgs.push(op.bg_id);
                    }
                    OpStep::WaitSync { worker_id: target } if *target == worker_id => {
                        // 检查同步进度
                        self.check_sync_progress(op.bg_id, worker_id, bg_reports);
                    }
                    _ => {}
                }
            }
        }
        
        BGCommands { add_bgs, remove_bgs }
    }
    
    /// 检查 Operator 进度和超时
    pub fn check_progress(&self) {
        let timeout = self.config_manager
            .get_u64("pd.schedule.operator_timeout_ms")
            .unwrap_or(600000);
        let now = current_time_ms();
        
        let mut to_remove = Vec::new();
        
        for mut entry in self.running_operators.iter_mut() {
            let op = entry.value_mut();
            
            // 检查超时
            if now - op.create_time_ms > timeout {
                op.status = OpStatus::Timeout;
                to_remove.push(op.bg_id);
                continue;
            }
            
            // 检查是否完成所有步骤
            if op.current_step >= op.steps.len() {
                op.status = OpStatus::Success;
                to_remove.push(op.bg_id);
            }
        }
        
        for bg_id in to_remove {
            self.running_operators.remove(&bg_id);
        }
    }
}

/// BG 指令（通过心跳下发）
pub struct BGCommands {
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
}
```

##### CheckerController

管理所有 Checker，协调巡检任务：

```rust
// curvine-server/src/pd/schedule/checker_controller.rs

pub struct CheckerController {
    checkers: Vec<Box<dyn Checker>>,
    operator_controller: Arc<OperatorController>,
    ctx: Arc<CoordinatorContext>,
}

impl CheckerController {
    pub fn new(
        operator_controller: Arc<OperatorController>,
        ctx: Arc<CoordinatorContext>,
    ) -> Self {
        let mut checkers: Vec<Box<dyn Checker>> = Vec::new();
        
        // 注册默认 Checker
        checkers.push(Box::new(NodeHealthChecker::new(ctx.clone())));
        checkers.push(Box::new(ReplicaChecker::new(ctx.clone())));
        checkers.push(Box::new(LeaseChecker::new(ctx.clone())));
        
        Self {
            checkers,
            operator_controller,
            ctx,
        }
    }
    
    /// 执行所有 Checker 的巡检
    pub async fn patrol(&self) {
        let checker_ctx = CheckerContext {
            pool_manager: &self.ctx.pool_manager,
            bg_manager: &self.ctx.bg_manager,
            node_manager: &self.ctx.node_manager,
            config_manager: &self.ctx.config_manager,
        };
        
        for checker in &self.checkers {
            let operators = checker.check(&checker_ctx);
            for op in operators {
                self.operator_controller.add_operator(op);
            }
        }
    }
}
```

##### Coordinator（调度协调器）

Schedule 模块的入口，协调各组件工作：

```rust
// curvine-server/src/pd/schedule/coordinator.rs

/// 协调器上下文（共享依赖）
pub struct CoordinatorContext {
    pub node_manager: Arc<NodeManager>,
    pub pool_manager: Arc<PoolManager>,
    pub bg_manager: Arc<BGManager>,
    pub config_manager: Arc<ConfigManager>,
    /// 节点移除事件发送端，NodeHealthChecker 在节点 Offline 时发送 (node_id, pool_ids)
    pub worker_removed_tx: tokio::sync::mpsc::UnboundedSender<(u64, Vec<u16>)>,
}

/// 调度协调器
pub struct Coordinator {
    ctx: Arc<CoordinatorContext>,
    checker_controller: CheckerController,
    operator_controller: Arc<OperatorController>,
    rebuild_scheduler: RebuildScheduler,
}

impl Coordinator {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
        ));
        
        let checker_controller = CheckerController::new(
            operator_controller.clone(),
            ctx.clone(),
        );
        
        let rebuild_scheduler = RebuildScheduler::new(ctx.clone());
        
        Self {
            ctx,
            checker_controller,
            operator_controller,
            rebuild_scheduler,
        }
    }
    
    /// 启动后台任务。worker_removed_rx 由 ClusterManager 创建 channel 后传入，用于接收 NodeHealthChecker 的节点移除事件。
    pub fn run(
        self: Arc<Self>,
        mut worker_removed_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, Vec<u16>)>,
    ) {
        // 启动 Checker 巡检循环
        let coord = self.clone();
        tokio::spawn(async move {
            coord.patrol_loop().await;
        });
        
        // 启动 Operator 进度检查循环
        let coord = self.clone();
        tokio::spawn(async move {
            coord.operator_check_loop().await;
        });
        
        // 启动 RebuildScheduler 循环
        let coord = self.clone();
        tokio::spawn(async move {
            coord.rebuild_loop().await;
        });
        
        // 接收节点移除事件并触发重建
        let coord = self.clone();
        tokio::spawn(async move {
            while let Some((node_id, pool_ids)) = worker_removed_rx.recv().await {
                coord.on_worker_removed(node_id, pool_ids);
            }
        });
    }
    
    /// Checker 巡检循环
    async fn patrol_loop(&self) {
        loop {
            let interval = self.ctx.config_manager
                .get_u64("pd.schedule.patrol_interval_ms")
                .unwrap_or(10000);
            
            tokio::time::sleep(Duration::from_millis(interval)).await;
            
            self.checker_controller.patrol().await;
        }
    }
    
    /// Operator 进度检查循环
    async fn operator_check_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            
            self.operator_controller.check_progress();
            self.operator_controller.dispatch_next();
        }
    }
    
    /// RebuildScheduler 循环
    async fn rebuild_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(5000)).await;
            
            self.rebuild_scheduler.check_and_rebuild().await;
        }
    }
    
    /// 分发 Operator 到 Worker（由 ClusterManager 调用）
    pub fn dispatch_operators(&self, worker_id: u64, bg_reports: &[BGStatusReport]) -> BGCommands {
        self.operator_controller.dispatch_to_worker(worker_id, bg_reports)
    }
    
    /// 节点上线通知（由 ClusterManager 调用）
    pub fn on_worker_joined(&self, node_id: u64, pool_ids: Vec<u16>) {
        self.rebuild_scheduler.schedule_rebuild(
            pool_ids,
            RebuildReason::NodeJoined { node_ids: vec![node_id] },
        );
    }
    
    /// 节点移除通知（由 NodeHealthChecker 调用）
    pub fn on_worker_removed(&self, node_id: u64, pool_ids: Vec<u16>) {
        self.rebuild_scheduler.schedule_rebuild(
            pool_ids,
            RebuildReason::NodeRemoved { node_ids: vec![node_id] },
        );
    }
}
```

#### 3.12.4 内置 Checker

##### NodeHealthChecker（节点健康检查器）

监控 Lost 状态节点，处理节点故障：

```rust
// curvine-server/src/pd/schedule/checker/node_health.rs

pub struct NodeHealthChecker {
    ctx: Arc<CoordinatorContext>,
    /// 已处理过的 Lost 节点（防止重复处理）
    processed_lost_nodes: DashMap<u64, u64>,  // node_id -> lost_time_ms
}

impl NodeHealthChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            processed_lost_nodes: DashMap::new(),
        }
    }
}

impl Checker for NodeHealthChecker {
    fn name(&self) -> &str {
        "node-health-checker"
    }
    
    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager
            .get_u64("pd.schedule.node_check_interval_ms")
            .unwrap_or(10000)
    }
    
    fn check(&self, ctx: &CheckerContext) -> Vec<BGOperator> {
        let mut operators = Vec::new();
        let recovery_window = ctx.config_manager
            .get_u64("pd.node.lost_recovery_window_ms")
            .unwrap_or(300000);
        let now = current_time_ms();
        
        // 获取所有 Lost 状态的 Worker 节点
        let lost_workers = ctx.node_manager.get_workers_by_state(NodeState::Lost);
        
        for worker in lost_workers {
            let node_id = worker.node_id;
            
            // 检查是否已处理过
            if let Some(entry) = self.processed_lost_nodes.get(&node_id) {
                let lost_time = *entry.value();
                
                // 检查是否超过恢复窗口
                if now - lost_time > recovery_window {
                    // 超时，标记节点 Offline，移除出 Pool，触发重建
                    self.handle_node_offline(ctx, node_id);
                }
                continue;
            }
            
            // 新发现的 Lost 节点
            self.processed_lost_nodes.insert(node_id, now);
            
            // 标记该节点上所有 BG 为 Degraded
            let affected_bgs = ctx.bg_manager.get_bgs_on_worker(node_id);
            for bg_id in affected_bgs {
                ctx.bg_manager.mark_bg_degraded(bg_id);
            }
            
            log::warn!("Worker {} marked as Lost, {} BGs degraded", 
                node_id, affected_bgs.len());
        }
        
        // 清理已恢复的节点
        self.cleanup_recovered_nodes(ctx);
        
        operators
    }
}

impl NodeHealthChecker {
    /// 处理节点 Offline
    fn handle_node_offline(&self, ctx: &CheckerContext, node_id: u64) {
        log::error!("Worker {} exceeded recovery window, marking Offline", node_id);
        
        // 1. 标记节点 Offline
        ctx.node_manager.update_worker_state(node_id, NodeState::Offline);
        
        // 2. 获取节点所属的 Pool
        let pool_ids = ctx.pool_manager.get_pools_by_worker(node_id);
        
        // 3. 从 Pool 中移除
        for pool_id in &pool_ids {
            ctx.pool_manager.remove_worker(*pool_id, node_id);
        }
        
        // 4. 通过事件通知 Coordinator 触发 BGTable 重建
        let _ = self.ctx.worker_removed_tx.send((node_id, pool_ids));
        
        // 5. 清理记录
        self.processed_lost_nodes.remove(&node_id);
    }
    
    /// 清理已恢复的节点
    fn cleanup_recovered_nodes(&self, ctx: &CheckerContext) {
        let recovered: Vec<u64> = self.processed_lost_nodes.iter()
            .filter(|entry| {
                let node_id = *entry.key();
                // 检查节点是否已恢复（状态不再是 Lost）
                !matches!(
                    ctx.node_manager.get_worker_state(node_id),
                    Some(NodeState::Lost)
                )
            })
            .map(|entry| *entry.key())
            .collect();
        
        for node_id in recovered {
            self.processed_lost_nodes.remove(&node_id);
            log::info!("Worker {} recovered from Lost state", node_id);
        }
    }
}
```

##### ReplicaChecker（副本检查器）

检查 BG 副本数量，为 Degraded BG 生成恢复 Operator：

```rust
// curvine-server/src/pd/schedule/checker/replica.rs

pub struct ReplicaChecker {
    ctx: Arc<CoordinatorContext>,
}

impl Checker for ReplicaChecker {
    fn name(&self) -> &str {
        "replica-checker"
    }
    
    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager
            .get_u64("pd.schedule.patrol_interval_ms")
            .unwrap_or(10000)
    }
    
    fn check(&self, ctx: &CheckerContext) -> Vec<BGOperator> {
        let mut operators = Vec::new();
        let max_concurrent = ctx.config_manager
            .get_u32("pd.recovery.max_concurrent")
            .unwrap_or(10);
        
        // 获取所有 Degraded 状态的 BG
        let degraded_bgs = ctx.bg_manager.get_bgs_by_state(BGState::Degraded);
        
        for bg in degraded_bgs {
            if operators.len() >= max_concurrent as usize {
                break;
            }
            
            // 检查可用副本
            let available_replicas: Vec<u64> = bg.replica_set.iter()
                .filter(|w| ctx.pool_manager.is_worker_available(**w))
                .cloned()
                .collect();
            
            if available_replicas.is_empty() {
                log::error!("BG {} all replicas lost!", bg.bg_id);
                continue;
            }
            
            // 获取 BGTable 策略
            let table = match ctx.bg_manager.get_table(bg.table_id) {
                Ok(t) => t,
                Err(_) => continue,
            };
            
            // 计算需要补充的副本数
            let needed = table.policy.replicas as usize - available_replicas.len();
            if needed == 0 {
                continue;
            }
            
            // 选择新副本节点
            let pool_id = (bg.table_id >> 16) as u16;
            let new_workers = match ctx.pool_manager.select_workers_for_bg(
                pool_id,
                needed as u16,
                table.policy.placement,
                &bg.replica_set,
            ) {
                Ok(workers) => workers,
                Err(_) => continue,
            };
            
            if new_workers.is_empty() {
                log::warn!("No available worker for BG {} recovery", bg.bg_id);
                continue;
            }
            
            // 生成 Operator
            let mut steps = Vec::new();
            for worker_id in &new_workers {
                steps.push(OpStep::AddReplica { worker_id: *worker_id });
                steps.push(OpStep::WaitSync { worker_id: *worker_id });
            }
            
            let op = BGOperator {
                id: generate_operator_id(),
                bg_id: bg.bg_id,
                description: format!("Add {} replicas", new_workers.len()),
                steps,
                current_step: 0,
                status: OpStatus::Pending,
                create_time_ms: current_time_ms(),
                priority: 100,  // 恢复优先级较高
            };
            
            operators.push(op);
        }
        
        operators
    }
}
```

##### LeaseChecker（Lease 检查器）

检查 BG Lease 有效性，处理 Lease 失效：

```rust
// curvine-server/src/pd/schedule/checker/lease.rs

pub struct LeaseChecker {
    ctx: Arc<CoordinatorContext>,
}

impl Checker for LeaseChecker {
    fn name(&self) -> &str {
        "lease-checker"
    }
    
    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager
            .get_u64("pd.schedule.patrol_interval_ms")
            .unwrap_or(10000)
    }
    
    fn check(&self, ctx: &CheckerContext) -> Vec<BGOperator> {
        let mut operators = Vec::new();
        
        // 获取 Lease 即将过期或已过期的 BG
        let expired_lease_bgs = ctx.bg_manager.get_bgs_with_expired_lease();
        
        for bg in expired_lease_bgs {
            // 找一个可用的副本作为新 Lease Owner
            let new_owner = bg.replica_set.iter()
                .find(|w| ctx.pool_manager.is_worker_available(**w))
                .cloned();
            
            if let Some(new_worker) = new_owner {
                let old_worker = bg.lease_owner
                    .map(|l| l.worker_id)
                    .unwrap_or(0);
                
                if new_worker != old_worker {
                    let op = BGOperator {
                        id: generate_operator_id(),
                        bg_id: bg.bg_id,
                        description: format!("Transfer lease to {}", new_worker),
                        steps: vec![OpStep::TransferLease {
                            from_worker: old_worker,
                            to_worker: new_worker,
                        }],
                        current_step: 0,
                        status: OpStatus::Pending,
                        create_time_ms: current_time_ms(),
                        priority: 80,
                    };
                    operators.push(op);
                }
            }
        }
        
        operators
    }
}
```

#### 3.12.5 内置 Scheduler

##### RebuildScheduler（重建调度器）

管理 BGTable 重建，支持冷却期和批量处理：

```rust
// curvine-server/src/pd/schedule/scheduler/rebuild.rs

/// 待重建任务
pub struct RebuildTask {
    pub pool_id: u16,
    pub reason: RebuildReason,
    pub scheduled_time_ms: u64,           // 计划执行时间
}

pub struct RebuildScheduler {
    ctx: Arc<CoordinatorContext>,
    /// 待重建的 Pool（合并多个请求）
    pending_rebuilds: DashMap<u16, RebuildTask>,
}

impl RebuildScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending_rebuilds: DashMap::new(),
        }
    }
    
    /// 调度重建（由 Coordinator 调用）
    pub fn schedule_rebuild(&self, pool_ids: Vec<u16>, reason: RebuildReason) {
        // 检查自动重建是否启用
        let auto_enabled = self.ctx.config_manager
            .get_bool("pd.bg.rebuild.auto_enabled")
            .unwrap_or(true);
        
        if !auto_enabled {
            log::info!("Auto rebuild disabled, skipping rebuild for pools {:?}", pool_ids);
            return;
        }
        
        let cooldown = self.ctx.config_manager
            .get_u64("pd.bg.rebuild.cooldown_ms")
            .unwrap_or(60000);
        let scheduled_time = current_time_ms() + cooldown;
        
        for pool_id in pool_ids {
            // 合并相同 Pool 的重建请求
            self.pending_rebuilds.entry(pool_id)
                .and_modify(|task| {
                    // 合并 reason
                    task.reason = Self::merge_reasons(&task.reason, &reason);
                    // 更新调度时间（取更晚的）
                    task.scheduled_time_ms = task.scheduled_time_ms.max(scheduled_time);
                })
                .or_insert(RebuildTask {
                    pool_id,
                    reason: reason.clone(),
                    scheduled_time_ms: scheduled_time,
                });
        }
    }
    
    /// 检查并执行重建
    pub async fn check_and_rebuild(&self) {
        let now = current_time_ms();
        
        // 收集已到时间的任务
        let ready_tasks: Vec<(u16, RebuildTask)> = self.pending_rebuilds.iter()
            .filter(|entry| entry.value().scheduled_time_ms <= now)
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        
        for (pool_id, task) in ready_tasks {
            // 移除任务
            self.pending_rebuilds.remove(&pool_id);
            
            // 执行重建
            if let Err(e) = self.execute_rebuild(pool_id, &task.reason).await {
                log::error!("Rebuild BGTable for pool {} failed: {}", pool_id, e);
            }
        }
    }
    
    /// 执行重建
    async fn execute_rebuild(&self, pool_id: u16, reason: &RebuildReason) -> FsResult<()> {
        log::info!("Rebuilding BGTable for pool {}, reason: {:?}", pool_id, reason);
        
        // 调用 BGManager 重建
        self.ctx.bg_manager.rebuild_bg_table(pool_id).await?;
        
        log::info!("BGTable for pool {} rebuilt successfully", pool_id);
        Ok(())
    }
    
    /// 合并重建原因
    fn merge_reasons(existing: &RebuildReason, new: &RebuildReason) -> RebuildReason {
        match (existing, new) {
            (RebuildReason::NodeJoined { node_ids: ids1 }, 
             RebuildReason::NodeJoined { node_ids: ids2 }) => {
                let mut merged = ids1.clone();
                merged.extend(ids2);
                RebuildReason::NodeJoined { node_ids: merged }
            }
            (RebuildReason::NodeRemoved { node_ids: ids1 }, 
             RebuildReason::NodeRemoved { node_ids: ids2 }) => {
                let mut merged = ids1.clone();
                merged.extend(ids2);
                RebuildReason::NodeRemoved { node_ids: merged }
            }
            // 混合情况，保留新的 reason
            _ => new.clone(),
        }
    }
}
```

#### 3.12.6 节点故障处理流程

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           节点故障处理流程                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐                                                             │
│  │ Worker 节点  │──心跳超时──> NodeManager 标记 Lost                           │
│  └─────────────┘                    │                                        │
│                                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │              NodeHealthChecker (定期巡检)                         │        │
│  │  ├─ 发现 Lost 节点                                                │        │
│  │  │   └─> 标记该节点上所有 BG 为 Degraded                          │        │
│  │  │   └─> 记录 Lost 时间                                           │        │
│  │  │                                                                │        │
│  │  ├─ 节点恢复（状态变回 Live）                                      │        │
│  │  │   └─> 清理记录                                                 │        │
│  │  │                                                                │        │
│  │  └─ 超过恢复窗口（lost_recovery_window_ms）                        │        │
│  │      └─> 标记节点 Offline                                         │        │
│  │      └─> 从 Pool 中移除                                           │        │
│  │      └─> 触发 RebuildScheduler 重建 BGTable                       │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                                     │                                        │
│                                     ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │               ReplicaChecker (定期巡检)                           │        │
│  │  ├─ 发现 Degraded BG                                              │        │
│  │  │   └─> 选择新副本节点                                           │        │
│  │  │   └─> 生成 AddReplica + WaitSync Operator                      │        │
│  │  │                                                                │        │
│  │  └─> OperatorController 执行 Operator                             │        │
│  │      └─> 通过心跳下发 add_bgs 指令                                │        │
│  │      └─> Worker 从其他副本同步数据                                │        │
│  │      └─> 同步完成后 BG 状态恢复为 Assigned                        │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### 3.12.7 节点上线/扩容流程

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         节点上线/扩容流程                                      │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  方式 1: 批量扩容（推荐）                                                     │
│  ─────────────────────────────────────────                                   │
│                                                                              │
│  1. 管理员设置 pd.bg.rebuild.auto_enabled = false                            │
│  2. 启动所有新 Worker 节点                                                    │
│  3. Worker 注册 → Coordinator.on_worker_joined() → 加入待重建队列            │
│     （因 auto_enabled=false，重建不会执行）                                   │
│  4. 确认所有节点正常后，管理员设置 pd.bg.rebuild.auto_enabled = true          │
│  5. RebuildScheduler 执行一次性重建（合并所有节点的变更）                      │
│                                                                              │
│  方式 2: 单节点扩容（自动）                                                   │
│  ─────────────────────────────────────────                                   │
│                                                                              │
│  1. Worker 启动并注册                                                        │
│  2. ClusterManager.handle_worker_register()                                  │
│     └─> Coordinator.on_worker_joined(node_id, pool_ids)                      │
│  3. RebuildScheduler 记录待重建任务（scheduled_time = now + cooldown）        │
│  4. 冷却期内如有更多节点加入，合并任务                                        │
│  5. 冷却期结束后执行重建                                                      │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────┐        │
│  │                    RebuildScheduler                              │        │
│  │  ┌──────────────────────────────────────────────────────┐       │        │
│  │  │ pending_rebuilds (DashMap<pool_id, RebuildTask>)     │       │        │
│  │  │  ├─ Pool-1: { reason: NodeJoined{W4,W5}, time: T1 }  │       │        │
│  │  │  └─ Pool-2: { reason: NodeJoined{W6}, time: T2 }     │       │        │
│  │  └──────────────────────────────────────────────────────┘       │        │
│  │                          │                                       │        │
│  │                          ▼ (check_and_rebuild 定期执行)          │        │
│  │                    current_time >= T1?                           │        │
│  │                          │                                       │        │
│  │               ┌──────────┴──────────┐                           │        │
│  │               │ Yes                  │ No                        │        │
│  │               ▼                      ▼                           │        │
│  │    execute_rebuild(Pool-1)       等待下次检查                   │        │
│  │               │                                                  │        │
│  │               ▼                                                  │        │
│  │    BGManager.rebuild_bg_table()                                 │        │
│  └─────────────────────────────────────────────────────────────────┘        │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### 3.12.8 Worker 端处理

Worker 收到 `add_bgs` 指令后，自动从其他副本同步数据：

```rust
// Worker 端代码
impl WorkerNode {
    /// 处理 add_bgs 指令（复用现有接口）
    async fn handle_add_bg(&self, bg_info: &BGInfo) -> FsResult<()> {
        // 检查本地是否已有数据
        if self.has_bg_data(bg_info.bg_id) {
            return Ok(()); // 已有数据，无需同步
        }
        
        log::info!("BG {} assigned, starting sync", bg_info.bg_id);
        
        // 从 lease_owner 或其他副本同步
        self.sync_bg_from_replicas(bg_info).await
    }
    
    /// 从其他副本同步 BG 数据
    async fn sync_bg_from_replicas(&self, bg_info: &BGInfo) -> FsResult<()> {
        // 优先从 lease_owner 同步
        let mut sources: Vec<u64> = Vec::new();
        if let Some(lease) = &bg_info.lease_owner {
            sources.push(lease.worker_id);
        }
        
        // 添加其他副本作为备选
        for worker_id in &bg_info.replica_set {
            if *worker_id != self.node_id && !sources.contains(worker_id) {
                sources.push(*worker_id);
            }
        }
        
        // 尝试从每个源同步
        for source_worker in sources {
            match self.sync_bg_from(bg_info.bg_id, source_worker).await {
                Ok(_) => {
                    log::info!("BG {} synced from worker {}", bg_info.bg_id, source_worker);
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("Sync BG {} from {} failed: {}", bg_info.bg_id, source_worker, e);
                }
            }
        }
        
        Err(FsError::common("no available source for sync"))
    }
    
    /// 从指定 Worker 同步 BG 数据
    async fn sync_bg_from(&self, bg_id: u32, source_worker: u64) -> FsResult<()> {
        // 1. 连接源 Worker
        let mut stream = self.connect_to_worker(source_worker).await?;
        
        // 2. 请求 BG 数据
        stream.request_bg_data(bg_id).await?;
        
        // 3. 流式接收并写入
        while let Some(chunk) = stream.next().await {
            let block_data = chunk?;
            self.write_block(bg_id, block_data).await?;
        }
        
        // 4. 标记同步完成
        self.mark_bg_ready(bg_id).await?;
        
        Ok(())
    }
}
```

#### 3.12.9 心跳上报同步状态

Worker 心跳上报的 `BGStatusReport` 需包含 `replica_state` 字段（参见 2.4 节定义）：

- `Syncing`: 正在从其他副本同步数据
- `Ready`: 同步完成，可提供服务
- `Failed`: 同步失败

OperatorController 根据这些状态推进 Operator 步骤。

#### 3.12.10 监控指标（Prometheus）

```rust
/// Schedule 模块监控指标
pub struct ScheduleMetrics {
    /// Degraded BG 数量
    pub degraded_bg_gauge: IntGauge,
    
    /// 等待执行的 Operator 数量
    pub waiting_operator_gauge: IntGauge,
    
    /// 执行中的 Operator 数量
    pub running_operator_gauge: IntGauge,
    
    /// Operator 成功计数（按类型）
    pub operator_success_counter: IntCounterVec,
    
    /// Operator 失败计数（按类型）
    pub operator_failure_counter: IntCounterVec,
    
    /// Operator 执行耗时直方图
    pub operator_duration_histogram: HistogramVec,
    
    /// Lost 节点数量
    pub lost_node_gauge: IntGauge,
    
    /// BGTable 重建次数
    pub rebuild_counter: IntCounter,
}

impl ScheduleMetrics {
    pub fn register(registry: &Registry) -> Self {
        // 注册到 Prometheus（参考 curvine 现有监控实现）
        // 后续单独完善
        ...
    }
}
```

#### 3.12.11 文件结构

```
curvine-server/src/pd/schedule/
├── mod.rs                    # 模块导出
├── coordinator.rs            # Coordinator（调度协调器）
├── operator.rs               # Operator 定义（BGOperator, OpStep, OpStatus）
├── operator_controller.rs    # OperatorController
├── checker_controller.rs     # CheckerController
├── checker/
│   ├── mod.rs
│   ├── node_health.rs        # NodeHealthChecker
│   ├── replica.rs            # ReplicaChecker
│   └── lease.rs              # LeaseChecker
└── scheduler/
    ├── mod.rs
    └── rebuild.rs            # RebuildScheduler
```

---

### 3.13 边界情况处理

| 场景 | 处理方式 |
|------|---------|
| 首次注册的节点 | PD 分配 epoch=1，返回给节点持久化 |
| 节点重启后重新注册（状态为 Lost/Offline） | PD 分配 epoch=old_epoch+1，更新节点状态为 Starting |
| 心跳 epoch 与 PD 记录不一致 | 拒绝心跳，要求节点重新注册 |
| Worker 的存储介质/az 配置变更 | 强制状态 → Lost，要求重新注册（PD 分配新 epoch，重新分配 Pool） |
| 节点优雅关闭 | 节点发送 Offline 通知，PD 标记状态为 Offline |
| 节点心跳超时 | NodeManager 标记 Lost，NodeHealthChecker 标记 BG 为 Degraded |
| 节点 Lost 超过恢复窗口 | NodeHealthChecker 标记 Offline，从 Pool 移除，触发 BGTable 重建 |
| 节点 Lost 后恢复心跳 | NodeHealthChecker 清理记录，节点继续提供服务 |
| BG 副本节点部分 Lost | ReplicaChecker 检测 Degraded BG，生成 AddReplica Operator |
| BG 所有副本 Lost | 记录错误日志，等待手动介入 |
| 批量节点扩容 | 管理员先禁用自动重建，节点全部上线后再启用，一次性重建 |
| 单节点扩容 | RebuildScheduler 带冷却期调度，合并冷却期内的多次请求 |
| Hash 环重建时有并发请求 | 使用 RwLock，rebuild 获取写锁 |
| RocksDB 写入失败 | Manager 层捕获错误，拒绝 Raft 提议 |
| Operator 执行超时 | 标记 Timeout 状态，从 running_operators 移除 |
| 恢复过程中源节点也 Lost | Worker 自动切换到其他副本继续同步 |
| Lease 过期 | LeaseChecker 检测并生成 TransferLease Operator |
