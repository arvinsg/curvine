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

use curvine_common::state::BGLease;
use curvine_common::state::{
    BlockGroupInfo, ConfigInfo, MountInfo, NodeInfo, PathRouteEntry, PoolInfo,
};
use serde::{Deserialize, Serialize};

// mount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountEntry {
    pub(crate) op_ms: u64,
    pub(crate) info: MountInfo,
}

// umount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_ms: u64,
    pub(crate) id: u32,
}

// config
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConfigEntry {
    pub(crate) op_ms: u64,
    pub(crate) info: ConfigInfo,
}

/// Node entry (Raft log) — used for both registration and periodic save
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeEntry {
    pub op_ms: u64,
    pub info: NodeInfo,
}

/// Pool entry (Raft log) — used for worker add/remove persistence
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PoolEntry {
    pub op_ms: u64,
    pub info: PoolInfo,
}

/// BG create entry (Raft log)
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGEntry {
    pub op_ms: u64,
    pub info: BlockGroupInfo,
}

/// BG update entry (Raft log)
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BGUpdateEntry {
    pub op_ms: u64,
    pub bg_id: u32,
    pub state: Option<curvine_common::state::BGState>,
    pub replica_set: Option<Vec<u32>>,
    pub lease_owner: Option<BGLease>,
    #[serde(default)]
    pub bg_epoch: Option<u64>,
}

/// Batch BG entry (Raft log) — atomically applies table + multiple BG creates/updates.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BatchBGEntry {
    pub op_ms: u64,
    pub table: Option<super::super::bg::BGTable>,
    pub creates: Vec<BlockGroupInfo>,
    pub updates: Vec<BGUpdateEntry>,
    /// If present, updates the next BG ID counter atomically with other changes.
    #[serde(default)]
    pub next_bg_id: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PdEntry {
    Noop,
    SetConfig(ConfigEntry),
    Mount(MountEntry),
    Unmount(u32),

    // Node management
    RegisterNode(NodeEntry),
    SaveNode(NodeEntry),
    DeleteNode(u32),

    // Pool management
    SavePool(PoolEntry),

    // BG management
    CreateBG(BGEntry),
    UpdateBG(BGUpdateEntry),
    DeleteBG(u32),
    BatchBG(BatchBGEntry),

    // Path route (MetaNode Federation static mode)
    AddPathRoute(PathRouteEntry),
    RemovePathRoute(String),
}
