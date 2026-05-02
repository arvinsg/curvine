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

use curvine_common::state::{table_id_pool_id, table_id_replica_count};
use orpc::common::Utils;
use serde::{Deserialize, Serialize};

/// Aggregate stats for a BGTable (sum of its BGs' stats).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BGTableStats {
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub block_count: u64,
    pub last_report_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BGTable {
    pub table_id: u32,
    pub bucket_count: u32,
    pub buckets: Vec<u32>,
    pub epoch: u64,
    pub create_time_ms: u64,
    pub last_rebuild_ms: u64,
    /// Runtime aggregate; not persisted.
    #[serde(skip)]
    pub stats: BGTableStats,
}

impl BGTable {
    pub fn lookup(&self, key: &[u8]) -> u32 {
        if self.buckets.is_empty() {
            return 0;
        }
        let hash = Utils::murmur3(key);
        let idx = (hash % self.bucket_count) as usize;
        self.buckets.get(idx).copied().unwrap_or(0)
    }

    pub fn pool_id(&self) -> u16 {
        table_id_pool_id(self.table_id)
    }

    pub fn replica_count(&self) -> u16 {
        table_id_replica_count(self.table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_empty_buckets_returns_zero() {
        let t = BGTable {
            table_id: 1,
            bucket_count: 0,
            buckets: vec![],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        assert_eq!(t.lookup(b"key"), 0);
    }

    #[test]
    fn lookup_returns_bg_id_at_bucket_index() {
        let t = BGTable {
            table_id: 1,
            bucket_count: 4,
            buckets: vec![10, 20, 30, 40],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        let bg_id = t.lookup(b"some_key");
        assert!(bg_id == 10 || bg_id == 20 || bg_id == 30 || bg_id == 40);
    }

    #[test]
    fn lookup_is_deterministic() {
        let t = BGTable {
            table_id: 1,
            bucket_count: 8,
            buckets: vec![1, 2, 3, 4, 5, 6, 7, 8],
            epoch: 0,
            create_time_ms: 0,
            last_rebuild_ms: 0,
            stats: BGTableStats::default(),
        };
        assert_eq!(t.lookup(b"foo"), t.lookup(b"foo"));
        assert_eq!(t.lookup(b"bar"), t.lookup(b"bar"));
    }
}
