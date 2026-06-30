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
use std::collections::HashMap;

pub type BgId = u64;
pub type TableId = u16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BGKind {
    Hash,
    Capacity,
}

impl Default for BGKind {
    fn default() -> Self {
        BGKind::Hash
    }
}

impl BGKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            BGKind::Hash => "hash",
            BGKind::Capacity => "capacity",
        }
    }
}

/// BG state persisted by PD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BGState {
    Init,
    Active,
    Degraded,
    Sealed,
}

impl BGState {
    pub fn as_str(&self) -> &'static str {
        match self {
            BGState::Init => "init",
            BGState::Active => "active",
            BGState::Degraded => "degraded",
            BGState::Sealed => "sealed",
        }
    }

    pub const ALL: [BGState; 4] = [
        BGState::Init,
        BGState::Active,
        BGState::Degraded,
        BGState::Sealed,
    ];
}

/// Replica lifecycle state reported by Worker and observed by PD.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ReplicaState {
    Pending,
    Recovering,
    Syncing,
    Active,
    Sealed,
    Draining,
}

impl Default for ReplicaState {
    fn default() -> Self {
        ReplicaState::Pending
    }
}

impl ReplicaState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReplicaState::Pending => "pending",
            ReplicaState::Recovering => "recovering",
            ReplicaState::Syncing => "syncing",
            ReplicaState::Active => "active",
            ReplicaState::Sealed => "sealed",
            ReplicaState::Draining => "draining",
        }
    }
}

/// Primary info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGPrimary {
    pub node_id: u32,
    pub epoch: u64,
    pub grant_time_ms: u64,
}

impl BGPrimary {
    pub fn new(node_id: u32, epoch: u64, grant_time_ms: u64) -> Self {
        Self {
            node_id,
            epoch,
            grant_time_ms,
        }
    }
}

/// Runtime replica state owned by a BG. Persistent membership remains in
/// `replica_set`; this vector is rebuilt from it after restore or leadership
/// changes and must not be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGReplica {
    pub worker_id: u32,
    #[serde(skip)]
    pub state: ReplicaState,
    #[serde(skip)]
    pub last_report_ms: u64,
    #[serde(skip)]
    pub isr_failures: u32,
    #[serde(skip)]
    pub isr_rejoin_block_until_ms: u64,
}

impl BGReplica {
    pub fn pending(worker_id: u32) -> Self {
        Self {
            worker_id,
            state: ReplicaState::Pending,
            last_report_ms: 0,
            isr_failures: 0,
            isr_rejoin_block_until_ms: 0,
        }
    }
}

/// Replica detail for client response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: u32,
    pub address: NodeAddress,
    pub state: NodeState,
    pub labels: HashMap<String, String>,
}

/// BG operation state: runtime-only scheduling operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BGOpState {
    #[default]
    Idle,
    Repairing,
    Rebalancing,
    PrimaryTransfer,
    Sealing,
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

/// BlockGroup info persisted by PD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupInfo {
    pub bg_id: BgId,
    pub table_id: TableId,
    pub kind: BGKind,
    pub bg_epoch: u64,
    pub replica_set: Vec<u32>,
    #[serde(default)]
    pub isr: Vec<u32>,
    pub state: BGState,
    pub primary: BGPrimary,

    #[serde(skip)]
    pub op_state: BGOpState,
    #[serde(skip)]
    pub replicas: Vec<BGReplica>,
    #[serde(skip)]
    pub stats: BGStats,
}

impl BlockGroupInfo {
    pub fn reset_replicas(&mut self) {
        self.replicas = self
            .replica_set
            .iter()
            .copied()
            .map(BGReplica::pending)
            .collect();
    }

    /// Align runtime replica state with the persistent replica_set.
    pub fn sync_replicas_with_replica_set(&mut self) {
        let old = std::mem::take(&mut self.replicas);
        self.replicas = self
            .replica_set
            .iter()
            .copied()
            .map(|worker_id| {
                old.iter()
                    .find(|replica| replica.worker_id == worker_id)
                    .cloned()
                    .unwrap_or_else(|| BGReplica::pending(worker_id))
            })
            .collect();
    }

    pub fn replica_state(&self, worker_id: u32) -> ReplicaState {
        self.replicas
            .iter()
            .find(|replica| replica.worker_id == worker_id)
            .map(|replica| replica.state)
            .unwrap_or(ReplicaState::Pending)
    }

    pub fn set_replica_state(
        &mut self,
        worker_id: u32,
        state: ReplicaState,
        report_time_ms: u64,
    ) -> bool {
        self.sync_replicas_with_replica_set();
        let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| replica.worker_id == worker_id)
        else {
            return false;
        };
        let changed = replica.state != state;
        replica.state = state;
        replica.last_report_ms = report_time_ms;
        changed
    }

    pub fn record_isr_failure(&mut self, worker_id: u32, now_ms: u64, delay_ms: u64) {
        self.sync_replicas_with_replica_set();
        if let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| replica.worker_id == worker_id)
        {
            replica.isr_failures = replica.isr_failures.saturating_add(1);
            replica.isr_rejoin_block_until_ms = now_ms.saturating_add(delay_ms);
        }
    }

    pub fn clear_isr_penalty(&mut self, worker_id: u32) {
        if let Some(replica) = self
            .replicas
            .iter_mut()
            .find(|replica| replica.worker_id == worker_id)
        {
            replica.isr_failures = 0;
            replica.isr_rejoin_block_until_ms = 0;
        }
    }

    pub fn is_isr_rejoin_blocked(&self, worker_id: u32, now_ms: u64) -> bool {
        self.replicas
            .iter()
            .find(|replica| replica.worker_id == worker_id)
            .map(|replica| now_ms < replica.isr_rejoin_block_until_ms)
            .unwrap_or(false)
    }

    pub fn isr_failure_count(&self, worker_id: u32) -> u32 {
        self.replicas
            .iter()
            .find(|replica| replica.worker_id == worker_id)
            .map(|replica| replica.isr_failures)
            .unwrap_or(0)
    }
}

/// Client BG route view. It intentionally exposes only replicas that
/// belong to the PD-published ISR / serving set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockGroupRouteView {
    pub bg_id: BgId,
    pub table_id: TableId,
    pub kind: BGKind,
    pub bg_epoch: u64,
    pub serving_replicas: Vec<ReplicaInfo>,
    pub state: BGState,
    pub primary: BGPrimary,
}
