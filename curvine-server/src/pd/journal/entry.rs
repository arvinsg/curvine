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

use crate::pd::bgtable::BGTable;
use curvine_common::state::{
    BGPrimary, BgId, BlockGroupInfo, ConfigInfo, MountInfo, NamespaceId, NamespaceInfo, NodeInfo,
    NodeState, PathRouteEntry, PeerInfo, RwPolicy,
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

/// Register or re-register a node.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RegisterNodeEntry {
    pub op_ms: u64,
    pub node: NodeInfo,
}

/// Remove a node after its lifecycle has completed.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoveNodeEntry {
    pub op_ms: u64,
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: NodeState,
}

/// Status-only node update. It can change the lifecycle state, advance the
/// persisted heartbeat timestamp, or do both.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeStatusUpdate {
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: NodeState,
    pub target_state: Option<NodeState>,
    pub heartbeat_ms: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpdateNodeStatusEntry {
    pub op_ms: u64,
    pub update: NodeStatusUpdate,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BatchUpdateNodeStatusEntry {
    pub op_ms: u64,
    pub updates: Vec<NodeStatusUpdate>,
}

/// Persistent node payload update. Runtime-only fields remain in memory and are
/// not part of this entry.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UpdateNodePayloadEntry {
    pub op_ms: u64,
    pub node_id: u32,
    pub expected_epoch: u64,
    pub expected_state: NodeState,
    pub patch: NodePayloadPatch,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum NodePayloadPatch {
    Meta(MetaNodePayloadPatch),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetaNodePayloadPatch {
    pub group_id: u32,
    pub group_epoch: u64,
    pub peers: Vec<PeerInfo>,
    pub rw_policy: RwPolicy,
}

/// BG id allocator entry.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGIdAllocatorEntry {
    pub op_ms: u64,
    pub expected_next_bg_id: BgId,
    pub next_bg_id: BgId,
}

/// BG create entry.
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
    pub expected_bg_epoch: u64,
    pub state: Option<curvine_common::state::BGState>,
    pub replica_set: Option<Vec<u32>>,
    pub isr: Option<Vec<u32>>,
    pub primary: Option<BGPrimary>,
}

/// Atomically applies multiple BG updates.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGBatchUpdateEntry {
    pub op_ms: u64,
    pub updates: Vec<BGUpdateEntry>,
}

/// BG delete entry.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGDeleteEntry {
    pub op_ms: u64,
    pub bg_id: BgId,
    pub expected_bg_epoch: u64,
}

/// Namespace create entry
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NamespaceCreateEntry {
    pub op_ms: u64,
    pub namespace: NamespaceInfo,
    pub tables: Vec<BGTable>,
    pub bgs: Vec<BlockGroupInfo>,
    pub expected_next_namespace_id: NamespaceId,
    pub next_namespace_id: NamespaceId,
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
    RegisterNode(RegisterNodeEntry),
    UpdateNodeStatus(UpdateNodeStatusEntry),
    BatchUpdateNodeStatus(BatchUpdateNodeStatusEntry),
    UpdateNodePayload(UpdateNodePayloadEntry),
    RemoveNode(RemoveNodeEntry),

    // Namespace management
    CreateNamespace(NamespaceCreateEntry),

    // BG management
    AllocateBGId(BGIdAllocatorEntry),
    CreateBG(BGEntry),
    UpdateBG(BGUpdateEntry),
    DeleteBG(BGDeleteEntry),
    BatchUpdateBG(BGBatchUpdateEntry),

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
            PdEntry::UpdateNodeStatus(_) => "update_node_status",
            PdEntry::BatchUpdateNodeStatus(_) => "batch_update_node_status",
            PdEntry::UpdateNodePayload(_) => "update_node_payload",
            PdEntry::RemoveNode(_) => "remove_node",
            PdEntry::CreateNamespace(_) => "create_namespace",
            PdEntry::AllocateBGId(_) => "allocate_bg_id",
            PdEntry::CreateBG(_) => "create_bg",
            PdEntry::UpdateBG(_) => "update_bg",
            PdEntry::DeleteBG(_) => "delete_bg",
            PdEntry::BatchUpdateBG(_) => "batch_update_bg",
            PdEntry::AddPathRoute(_) => "add_path_route",
            PdEntry::RemovePathRoute(_) => "remove_path_route",
        }
    }
}
