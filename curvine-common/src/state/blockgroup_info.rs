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

use super::{NodeAddress, NodeState, StorageType};
use serde::{Deserialize, Serialize};

/// Placement policy for replica selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPolicy {
    Default,
    CrossAZ,
}

/// BlockGroup policy (persisted)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupPolicy {
    pub storage_type: StorageType,
    pub replicas: u16,
    pub placement: PlacementPolicy,
}

/// BG state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BGState {
    /// Initializing, waiting for replica assignment
    Init,
    /// Assigned, replicas ready
    Assigned,
    /// Migrating (rebalance / tiering)
    Moving,
    /// Replica count below desired
    Degraded,
    /// Recovering, adding replicas
    Recovering,
    /// Deleting
    Deleting,
}

/// Lease info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGLease {
    pub node_id: u32,
    pub expire_time_ms: u64,
}

/// Replica detail for client response (address and state resolved from NodeManager at query time)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: u32,
    pub address: NodeAddress,
    pub state: NodeState,
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
    pub epoch: u64,
    pub replica_set: Vec<u32>,
    pub state: BGState,
    pub lease_owner: BGLease,

    #[serde(skip)]
    pub stats: BGStats,
}

/// BlockGroup view for client (replica_set expanded with address and node state)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupInfoView {
    pub bg_id: u32,
    pub table_id: u32,
    pub epoch: u64,
    pub replica_set: Vec<ReplicaInfo>,
    pub state: BGState,
    pub lease_owner: BGLease,
}
