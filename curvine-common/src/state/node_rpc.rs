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

use crate::state::meta_node_info::{InodesStats, NodeGroupInfo};
use crate::state::node_info::{NodePayload, SystemStats};
use crate::state::node_state::{NodeAddress, NodeBase, NodeType};
use crate::state::task_node_info::TaskNodeStats;
use crate::state::worker_node_info::StorageStats;
use crate::state::PathRouteEntry;
use crate::state::{BGKind, BGStats, BgId, BlockGroupInfo, ReplicaState, TableId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{PeerInfo, RwPolicy};

/// Per-BG report from worker: replica epoch, state and stats.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerBGReport {
    #[serde(default)]
    pub kind: BGKind,
    pub bg_id: BgId,
    pub bg_epoch: u64,
    pub state: ReplicaState,
    #[serde(default)]
    pub stats: BGStats,
    #[serde(default)]
    pub isr_remove_candidates: Vec<u32>,
}

impl Default for WorkerBGReport {
    fn default() -> Self {
        Self {
            kind: BGKind::Hash,
            bg_id: 0,
            bg_epoch: 0,
            state: ReplicaState::Pending,
            stats: BGStats::default(),
            isr_remove_candidates: Vec::new(),
        }
    }
}

/// Node register request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub cluster_id: String,
    #[serde(flatten)]
    pub base: NodeBase,
    pub payload: NodePayload,
}

/// Heartbeat request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub cluster_id: String,
    pub node_id: u32,
    pub node_type: NodeType,
    pub epoch: u64,
    pub timestamp_ms: u64,
    pub address: NodeAddress,
    pub payload: HeartbeatPayload,
}

/// Heartbeat request payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatPayload {
    Worker(WorkerHeartbeatPayload),
    Meta(MetaHeartbeatPayload),
    Task(TaskHeartbeatPayload),
}

/// Worker heartbeat payload
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerHeartbeatPayload {
    pub storage_stats: HashMap<String, StorageStats>,
    pub sys_stats: SystemStats,
    /// Per-BG replica epoch, state and stats.
    #[serde(default)]
    pub bg_reports: Vec<WorkerBGReport>,
}

/// Meta heartbeat payload (is_leader, group_epoch, stats)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaHeartbeatPayload {
    pub group_id: u32,
    pub group_epoch: u64,
    pub is_leader: bool,
    pub peers: Vec<PeerInfo>,
    pub rw_policy: RwPolicy,
    pub inodes_stats: InodesStats,
    pub sys_stats: SystemStats,
}

/// TaskNode heartbeat payload.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskHeartbeatPayload {
    pub sys_stats: SystemStats,
    pub stats: TaskNodeStats,
}

/// Heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub error: Option<String>,
    pub epoch: u64,
    pub mount_version: u64,
    #[serde(default)]
    pub table_epochs: HashMap<TableId, u64>,
    pub payload: HeartbeatResponsePayload,
}

/// Heartbeat response payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatResponsePayload {
    Worker(WorkerHeartbeatResponse),
    Meta(MetaHeartbeatResponse),
    Task(TaskHeartbeatResponse),
}

/// Worker heartbeat response
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerHeartbeatResponse {
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<BgId>,
    pub update_bgs: Vec<BlockGroupInfo>,
}

/// TaskNode heartbeat response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskHeartbeatResponse {}

/// Path route table update. PD currently publishes a full static route table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathRouteUpdate {
    pub version: u64,
    pub routes: Vec<PathRouteEntry>,
}

/// Group update action (add or remove groups)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeGroupUpdateAction {
    AddGroup { groups: Vec<NodeGroupInfo> },
    RemoveGroup { group_ids: Vec<u64> },
}

/// Node group config update
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGroupUpdate {
    pub version: u64,
    pub action: NodeGroupUpdateAction,
}

/// Meta heartbeat response (path_route_update for static mode, node_group_update for group changes)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetaHeartbeatResponse {
    pub path_route_update: Option<PathRouteUpdate>,
    pub node_group_update: Option<NodeGroupUpdate>,
}
