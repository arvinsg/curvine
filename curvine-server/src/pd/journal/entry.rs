// Copyright 2025 OPPO.
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

use curvine_common::state::{
    BGPrimary, BgId, BlockGroupInfo, ConfigInfo, MountInfo, NamespaceId, NamespaceInfo, NodeInfo,
    NodePayload, NodeState, PathRouteEntry, TableId,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum MountEntry {
    Add(MountAddEntry),
    Update(MountUpdateEntry),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountAddEntry {
    pub(crate) op_ms: u64,
    pub(crate) info: MountInfo,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountUpdateEntry {
    pub(crate) op_ms: u64,
    pub(crate) expected_mount_id: u32,
    pub(crate) expected_cv_path: String,
    pub(crate) expected_version: u64,
    pub(crate) info: MountInfo,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_ms: u64,
    pub(crate) id: u32,
    pub(crate) expected_cv_path: String,
    pub(crate) expected_version: u64,
}

// config
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConfigEntry {
    pub(crate) op_ms: u64,
    pub(crate) expected_version: u64,
    pub(crate) info: ConfigInfo,
}

/// Node entry — used for both registration and periodic save
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeEntry {
    pub op_ms: u64,
    pub info: NodeInfo,
}

/// Optional persistent node payload update.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum NodePayloadUpdate {
    Replace(NodePayload),
}

/// node state update with epoch/state CAS.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpdateNodeStateEntry {
    pub op_ms: u64,
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: Option<NodeState>,
    pub new_state: NodeState,
    pub state_since_ms: u64,
    #[serde(default)]
    pub last_heartbeat_ms: Option<u64>,
    #[serde(default)]
    pub payload_update: Option<NodePayloadUpdate>,
}

/// Periodic heartbeat checkpoint.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct HeartbeatCheckpointEntry {
    pub op_ms: u64,
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: Option<NodeState>,
    pub last_heartbeat_ms: u64,
}

/// Batch node state update with per-node epoch/state CAS.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BatchUpdateNodeStateEntry {
    pub op_ms: u64,
    pub entries: Vec<UpdateNodeStateEntry>,
}

/// Node deletion with epoch/state CAS.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeleteNodeEntry {
    pub op_ms: u64,
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: Option<NodeState>,
}

/// BG create entry (Raft log)
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGEntry {
    pub op_ms: u64,
    pub info: BlockGroupInfo,
}

/// BG update entry.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGUpdateEntry {
    pub op_ms: u64,
    pub bg_id: BgId,
    pub state: Option<curvine_common::state::BGState>,
    pub replica_set: Option<Vec<u32>>,
    pub isr: Option<Vec<u32>>,
    pub primary: Option<BGPrimary>,
    /// BG epoch the proposer based this entry on. `serde(default)` keeps
    /// pre-P2.1 logs decodable: legacy entries decode with expected_bg_epoch=0
    /// and rely on the `new_bg_epoch > info.bg_epoch` monotonicity check.
    #[serde(default)]
    pub expected_bg_epoch: u64,
    pub new_bg_epoch: u64,
    /// Table whose route epoch should be bumped at apply time if this BG mutation succeeds.
    pub bump_table_epoch: Option<TableId>,
}

/// Batch BG entry (Raft log) — atomically applies table + multiple BG creates/updates.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BatchBGEntry {
    pub op_ms: u64,
    /// Legacy single-table create field. New namespace creation uses `tables`.
    pub table: Option<super::super::bg::BGTable>,
    #[serde(default)]
    pub tables: Vec<super::super::bg::BGTable>,
    pub creates: Vec<BlockGroupInfo>,
    pub updates: Vec<BGUpdateEntry>,
    /// If present, updates the next BG ID counter atomically with other changes.
    #[serde(default)]
    pub next_bg_id: Option<BgId>,
    #[serde(default)]
    pub next_table_id: Option<TableId>,
    #[serde(default)]
    /// Table whose route epoch should be bumped at apply time if this batch mutates route-visible state.
    pub bump_table_epoch: Option<TableId>,
    /// P2.3: when `table` is set and this is true, apply requires the table to
    /// NOT already exist. Used by `create_table` to prevent two concurrent
    /// creates with the same `(pool_type, replica_count)` from clobbering each
    /// other's BGs (the second batch's CAS fails and orphan BGs are avoided).
    /// Pre-P2.3 entries decode with `expected_table_absent = false` (no guard).
    #[serde(default)]
    pub expected_table_absent: bool,
}

/// BG delete entry (Raft log).
///
/// `expected_bg_epoch` (P2.2) protects against deleting a BG whose epoch has
/// moved since the proposer's snapshot. `table_id` is kept for backward
/// compatibility but apply now reads `info.table_id` from the BG itself
/// instead of trusting this field, eliminating the "delete bumps wrong
/// table_epoch" risk described in §4.6.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGDeleteEntry {
    pub op_ms: u64,
    pub bg_id: BgId,
    pub table_id: TableId,
    /// BG epoch the proposer based the delete on. `serde(default)` keeps
    /// pre-P2.2 logs decodable.
    #[serde(default)]
    pub expected_bg_epoch: u64,
}

/// A single table epoch bump. Replica lifecycle state is leader-runtime only;
/// this entry only publishes a monotonically increasing route epoch.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TableEpochUpdate {
    pub table_id: TableId,
    pub expected_epoch: u64,
    pub new_epoch: u64,
}

/// Namespace create entry (Raft log). This atomically materializes the
/// NamespaceInfo and its initial Hash BGTable(s). Capacity BGTable creation is
/// intentionally rejected in phase 1 and reserved for write acceleration.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NamespaceCreateEntry {
    pub op_ms: u64,
    pub namespace: NamespaceInfo,
    pub bg_batch: BatchBGEntry,
    pub expected_next_namespace_id: NamespaceId,
    pub next_namespace_id: NamespaceId,
    pub expected_next_bg_id: BgId,
    pub next_bg_id: BgId,
}

/// Batch table epoch bump entry (Raft log).
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BumpTableEpochEntry {
    pub op_ms: u64,
    pub updates: Vec<TableEpochUpdate>,
}

/// Add or update one static MetaRoute path rule with table-version CAS.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PathRouteAddEntry {
    pub op_ms: u64,
    pub route: PathRouteEntry,
    pub expected_table_version: u64,
}

/// Remove one static MetaRoute path rule with table-version CAS.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PathRouteRemoveEntry {
    pub op_ms: u64,
    pub path: String,
    pub expected_table_version: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PdEntry {
    Noop,
    SetConfig(ConfigEntry),
    Mount(MountEntry),
    Unmount(UnMountEntry),

    // Node management
    RegisterNode(NodeEntry),
    SaveNode(NodeEntry),
    UpdateNodeState(UpdateNodeStateEntry),
    BatchUpdateNodeState(BatchUpdateNodeStateEntry),
    HeartbeatCheckpoint(HeartbeatCheckpointEntry),
    DeleteNode(DeleteNodeEntry),

    // Namespace management
    CreateNamespace(NamespaceCreateEntry),

    // BG management
    CreateBG(BGEntry),
    UpdateBG(BGUpdateEntry),
    DeleteBG(BGDeleteEntry),
    BatchBG(BatchBGEntry),
    BumpTableEpoch(BumpTableEpochEntry),

    // Path route (MetaNode Federation static mode)
    AddPathRoute(PathRouteAddEntry),
    RemovePathRoute(PathRouteRemoveEntry),
}

impl PdEntry {
    pub fn entry_type_str(&self) -> &'static str {
        match self {
            PdEntry::Noop => "noop",
            PdEntry::SetConfig(_) => "set_config",
            PdEntry::Mount(_) => "mount",
            PdEntry::Unmount(_) => "unmount",
            PdEntry::RegisterNode(_) => "register_node",
            PdEntry::SaveNode(_) => "save_node",
            PdEntry::UpdateNodeState(_) => "update_node_state",
            PdEntry::BatchUpdateNodeState(_) => "batch_update_node_state",
            PdEntry::HeartbeatCheckpoint(_) => "heartbeat_checkpoint",
            PdEntry::DeleteNode(_) => "delete_node",
            PdEntry::CreateNamespace(_) => "create_namespace",
            PdEntry::CreateBG(_) => "create_bg",
            PdEntry::UpdateBG(_) => "update_bg",
            PdEntry::DeleteBG(_) => "delete_bg",
            PdEntry::BatchBG(_) => "batch_bg",
            PdEntry::BumpTableEpoch(_) => "bump_table_epoch",
            PdEntry::AddPathRoute(_) => "add_path_route",
            PdEntry::RemovePathRoute(_) => "remove_path_route",
        }
    }
}
