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

use orpc::common::Utils;
use serde::{Deserialize, Serialize};

/// table_id encoding: (pool_id << 16) | replicas
#[inline]
pub fn table_id_replica_count(table_id: u32) -> u16 {
    (table_id & 0xFFFF) as u16
}

#[inline]
pub fn table_id_pool_id(table_id: u32) -> u16 {
    (table_id >> 16) as u16
}

/// Built by PD from BGTable + NodeManager; used by client SDK.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BGTableSummary {
    pub table_id: u32,
    pub bucket_count: u32,
    pub epoch: u64,
    pub last_rebuild_ms: u64,
    pub buckets: Vec<super::BlockGroupInfoView>,
}

impl BGTableSummary {
    pub fn replica_count(&self) -> u16 {
        table_id_replica_count(self.table_id)
    }

    pub fn pool_id(&self) -> u16 {
        table_id_pool_id(self.table_id)
    }

    /// Lookup which BlockGroup (with replicas) to use for the given key.
    pub fn lookup(&self, key: &[u8]) -> Option<&super::BlockGroupInfoView> {
        if self.buckets.is_empty() {
            return None;
        }
        let hash = Utils::murmur3(key);
        let idx = (hash % self.bucket_count) as usize;
        self.buckets.get(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{BGLease, BGState, BlockGroupInfoView};

    fn sample_view(bg_id: u32, table_id: u32) -> BlockGroupInfoView {
        BlockGroupInfoView {
            bg_id,
            table_id,
            epoch: 1,
            replica_set: vec![],
            state: BGState::Assigned,
            lease_owner: BGLease {
                node_id: 0,
                expire_time_ms: 0,
            },
        }
    }

    #[test]
    fn table_id_helpers() {
        let table_id = (2u32 << 16) | 3u32;
        assert_eq!(table_id_replica_count(table_id), 3);
        assert_eq!(table_id_pool_id(table_id), 2);
    }

    #[test]
    fn summary_replica_count_and_pool_id() {
        let s = BGTableSummary {
            table_id: (2 << 16) | 3,
            bucket_count: 4,
            epoch: 1,
            last_rebuild_ms: 0,
            buckets: vec![
                sample_view(1, 0),
                sample_view(2, 0),
                sample_view(3, 0),
                sample_view(4, 0),
            ],
        };
        assert_eq!(s.replica_count(), 3);
        assert_eq!(s.pool_id(), 2);
    }

    #[test]
    fn summary_lookup() {
        let s = BGTableSummary {
            table_id: 1,
            bucket_count: 4,
            epoch: 0,
            last_rebuild_ms: 0,
            buckets: vec![
                sample_view(10, 1),
                sample_view(20, 1),
                sample_view(30, 1),
                sample_view(40, 1),
            ],
        };
        let bg = s.lookup(b"key");
        assert!(bg.is_some());
        let id = bg.unwrap().bg_id;
        assert!(id == 10 || id == 20 || id == 30 || id == 40);
    }

    #[test]
    fn summary_lookup_empty_none() {
        let s = BGTableSummary {
            table_id: 1,
            bucket_count: 0,
            epoch: 0,
            last_rebuild_ms: 0,
            buckets: vec![],
        };
        assert!(s.lookup(b"x").is_none());
    }
}
