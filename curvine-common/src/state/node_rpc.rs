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
use crate::state::meta_node_mode::PathRouteEntry;
use crate::state::node_info::{NodePayload, SystemStats};
use crate::state::node_state::{NodeAddress, NodeBase, NodeType};
use crate::state::worker_node_info::StorageStats;
use crate::state::BlockGroupInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{PeerInfo, RwPolicy};

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
}

/// Worker heartbeat payload
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerHeartbeatPayload {
    pub storage_stats: HashMap<String, StorageStats>,
    pub sys_stats: SystemStats,
    /// BGs currently held by the worker (for PD to validate assignments)
    #[serde(default)]
    pub bg_ids: Vec<u32>,
    /// BG epochs for staleness detection (bg_id -> epoch)
    #[serde(default)]
    pub bg_epochs: HashMap<u32, u64>,
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

/// Heartbeat response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub error: Option<String>,
    pub epoch: u64,
    pub config_version: u64,
    pub mount_version: u64,
    pub bg_version: u64,
    pub payload: HeartbeatResponsePayload,
}

/// Heartbeat response payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatResponsePayload {
    Worker(WorkerHeartbeatResponse),
    Meta(MetaHeartbeatResponse),
}

/// Worker heartbeat response
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerHeartbeatResponse {
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
    pub update_bgs: Vec<BlockGroupInfo>,
}

/// Route update action (full sync or incremental)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RouteUpdateAction {
    FullSync {
        routes: Vec<PathRouteEntry>,
    },
    Incremental {
        added: Vec<PathRouteEntry>,
        removed: Vec<String>,
    },
}

/// Path route table update (static mode)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathRouteUpdate {
    pub version: u64,
    pub action: RouteUpdateAction,
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
