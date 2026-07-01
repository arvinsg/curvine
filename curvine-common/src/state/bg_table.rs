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

use super::{BGKind, BlockGroupRouteView, CacheReplicaPolicy, TableId};
use orpc::common::Utils;
use serde::{Deserialize, Serialize};

/// Client-facing BGTable route summary built by PD.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BGTableSummary {
    Hash(HashBGTableSummary),
    Capacity(CapacityBGTableSummary),
}

impl BGTableSummary {
    pub fn table_id(&self) -> TableId {
        match self {
            BGTableSummary::Hash(s) => s.table_id,
            BGTableSummary::Capacity(s) => s.table_id,
        }
    }

    pub fn kind(&self) -> BGKind {
        match self {
            BGTableSummary::Hash(_) => BGKind::Hash,
            BGTableSummary::Capacity(_) => BGKind::Capacity,
        }
    }

    pub fn epoch(&self) -> u64 {
        match self {
            BGTableSummary::Hash(s) => s.epoch,
            BGTableSummary::Capacity(s) => s.epoch,
        }
    }

    pub fn lookup(&self, key: &[u8]) -> Option<&BlockGroupRouteView> {
        match self {
            BGTableSummary::Hash(s) => s.lookup(key),
            BGTableSummary::Capacity(_) => None,
        }
    }
}

impl Default for BGTableSummary {
    fn default() -> Self {
        BGTableSummary::Hash(HashBGTableSummary::default())
    }
}

/// Hash BGTable route summary built by PD.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HashBGTableSummary {
    pub table_id: TableId,
    pub epoch: u64,
    pub cache_replica_policy: CacheReplicaPolicy,
    /// Dense bucket route views. The index must match the Hash BGTable bucket index.
    pub buckets: Vec<BlockGroupRouteView>,
}

impl HashBGTableSummary {
    /// Lookup which BlockGroup (with replicas) to use for the given key.
    pub fn lookup(&self, key: &[u8]) -> Option<&BlockGroupRouteView> {
        if self.buckets.is_empty() {
            return None;
        }
        let hash = Utils::murmur3(key);
        let idx = hash as usize % self.buckets.len();
        self.buckets.get(idx)
    }
}

/// Capacity BGTable active-set summary. Data-plane write is reserved for later phases.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CapacityBGTableSummary {
    pub table_id: TableId,
    pub epoch: u64,
    pub active_bgs: Vec<BlockGroupRouteView>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{BGPrimary, BGState, BgId};

    fn sample_view(bg_id: BgId, table_id: TableId) -> BlockGroupRouteView {
        BlockGroupRouteView {
            bg_id,
            table_id,
            kind: BGKind::Hash,
            bg_epoch: 1,
            serving_replicas: vec![],
            state: BGState::Active,
            primary: BGPrimary {
                node_id: 0,
                epoch: 1,
                grant_time_ms: 0,
            },
        }
    }

    #[test]
    fn hash_summary_lookup() {
        let s = BGTableSummary::Hash(HashBGTableSummary {
            table_id: 1,
            epoch: 0,
            cache_replica_policy: CacheReplicaPolicy::default(),
            buckets: vec![
                sample_view(10, 1),
                sample_view(20, 1),
                sample_view(30, 1),
                sample_view(40, 1),
            ],
        });
        let bg = s.lookup(b"key");
        assert!(bg.is_some());
        let id = bg.unwrap().bg_id;
        assert!(id == 10 || id == 20 || id == 30 || id == 40);
    }

    #[test]
    fn capacity_summary_has_no_hash_lookup() {
        let s = BGTableSummary::Capacity(CapacityBGTableSummary {
            table_id: 2,
            epoch: 1,
            active_bgs: vec![],
        });
        assert!(s.lookup(b"x").is_none());
        assert_eq!(s.kind(), BGKind::Capacity);
    }
}
