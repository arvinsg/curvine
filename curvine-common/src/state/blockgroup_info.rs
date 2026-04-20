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

use super::{NodeAddress, NodeState};
use serde::{Deserialize, Serialize};

/// BG state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BGState {
    /// Initializing, waiting for replica assignment
    Init,
    /// Assigned, metadata prepared but not fully active yet
    Assigned,
    /// Active and serving
    Active,
    /// Degraded due to replica lost
    Degraded,
    /// Recovering, adding replicas
    Recovering,
    /// Rebalancing due to topology/table changes
    Rebalancing,
    /// Deleting
    Deleting,
}

impl BGState {
    pub fn as_str(&self) -> &'static str {
        match self {
            BGState::Init => "init",
            BGState::Assigned => "assigned",
            BGState::Active => "active",
            BGState::Degraded => "degraded",
            BGState::Recovering => "recovering",
            BGState::Rebalancing => "rebalancing",
            BGState::Deleting => "deleting",
        }
    }

    pub const ALL: [BGState; 7] = [
        BGState::Init,
        BGState::Assigned,
        BGState::Active,
        BGState::Degraded,
        BGState::Recovering,
        BGState::Rebalancing,
        BGState::Deleting,
    ];
}

/// Replica lifecycle state (PD runtime, not Raft-persisted).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ReplicaState {
    Pending,
    Syncing,
    Active,
    Offline,
}

impl ReplicaState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicaState::Pending => "pending",
            ReplicaState::Syncing => "syncing",
            ReplicaState::Active => "active",
            ReplicaState::Offline => "offline",
        }
    }
}

/// Lease info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGLease {
    pub node_id: u32,
    pub epoch: u64,
    pub grant_time_ms: u64,
}

/// Replica detail for client response (address and state resolved from NodeManager at query time)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: u32,
    pub address: NodeAddress,
    pub state: NodeState,
}

/// BG operation state: tracks whether a BG is currently being operated on
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BGOpState {
    #[default]
    Idle,
    Recovering,
    Rebalancing,
    LeaseBalancing,
    Deleting,
}

/// BG stats
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BGStats {
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub block_count: u64,
    pub last_report_ms: u64,
}

/// BlockGroup info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupInfo {
    pub bg_id: u32,
    pub table_id: u32,
    pub bg_epoch: u64,
    pub replica_set: Vec<u32>,
    pub state: BGState,
    pub op_state: BGOpState,
    pub lease_owner: Option<BGLease>,

    #[serde(skip)]
    pub stats: BGStats,
}

/// BlockGroup view for client (replica_set expanded with address and node state)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupInfoView {
    pub bg_id: u32,
    pub table_id: u32,
    pub bg_epoch: u64,
    pub replica_set: Vec<ReplicaInfo>,
    pub state: BGState,
    pub op_state: BGOpState,
    pub lease_owner: Option<BGLease>,
}
