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

#### 节点注册信息

```rust
node_id: u64                    // 全局唯一；沿用现有 worker 逻辑，自己生成唯一 id
node_type: enum {Worker, Meta, .......}
addr: {hostname, ip, rpc_port, web_port}
labels: map<string, string>     // 如 az/rack/media/group/...
stats: map<string, string>      // 如 load/memory/capactity
epoch: u64                      // 节点重新注册递增
version: str                    // 二进制版本号，后期兼容性考虑
status: enum {Starting, Live, Decommission, Blacklist, Lost}
last_heartbeat_ms: u64
```

#### 相关 RPC 接口

```
node_register(NodeInfo)
```

提供 RESTFUL API 方便查看当前的节点信息：

```
GET /api/v1/node/{node_type}
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

#### Worker 心跳接口

用于 worker 节点定期的心跳上报，心跳上报请求中除了基本数据外，需要携带：

- `storage_info: repeated StorageInfo`（容量、可用、fs_used、non_fs_used、reserved、storage_type 等）
- `metrics`: 可选（IOPS、带宽、延迟、load、inflight 等）
- `blockgroups`: 当前持有的 BlockGroup 列表摘要/block数量/使用容量统计等

---

### 2.5 Block Group 管理

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

### 2.6 MetaNode 管理

MetaNode 的核心目标是元数据扩展性。MetaNode 需要 PD 提供可扩展的路由能力，使 client 能在不同部署模式下正确选择 MetaNode（或 MetaRaftGroup）进行访问。

考虑到未来的扩展性，MetaNode 的"服务模式"作为可配置的注册模式：

- **MetaNodeMode::Proxy**：MetaNode 作为 proxy，元数据使用分布式 KV 存储，client 随机选一个 MetaNode 访问即可
- **MetaNodeMode::Shard**：MetaNode 带分片信息，client 必须按路径分片选择对应节点（仅作为扩展，暂不实现）
- **MetaNodeMode::Federation**：MetaNode 以 raft group 形式提供服务（leader RW、follower R），PD 维护路径表（path table）把路径映射到某个 MetaRaftGroup，client 根据路径表选择不同的 group metanode 访问

PD 中配置元数据节点模式，client 根据不同的模式，采取不同的元数据访问策略，metanode 节点的上报复用之前的注册字段，通过 label 字段来进行扩展。

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

## 三、模块实现

在当前 pd 模块引入如下三个 mod

- node: node 模块实现节点的注册和心跳管理，维护节点的信息，节点注册持久化 kv 中. 需要根据不同的节点角色做不同的实现，扩展为 trait
- pool: 实现数据部分的管理，主要为了划分不同的存储介质，当前简化 pool 的实现，一种存储介质默认创建一个pool, 当前仅实现 Memory、SSD、HDD 三种介质的, pool 通过 node 模块 manager 功能来获取worker节点信息，将 worker 节点自动划分为不同的 pool, 并定期统计用量数据
- bg: 实现  bg 的管理，实现一致性bg 的管理功能，提供 bg 创建， hash 环重建等功能
- cluster: 将上面三个模块组成起来，提供完整的功能，比如获取 pg table, 节点上下线时调用 bg 模块重建bg table, 并将bg 分配给worker, 更新 bg 信息等。对外提供 rpc 和 restful api 接口
