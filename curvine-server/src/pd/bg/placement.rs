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

use super::BGTable;
use curvine_common::state::{BGLease, BGState, BlockGroupInfo, BlockGroupPolicy};
use curvine_common::FsError;
use orpc::common::LocalTime;

/// Result of building a new BGTable: the table metadata plus the BG entries.
pub struct BuildTableResult {
    pub table: BGTable,
    pub bgs: Vec<BlockGroupInfo>,
}

/// Build a new BGTable with token-based round-robin placement.
///
/// - `table_id`: pre-computed table_id encoding pool_id and replica_count
/// - `bucket_count`: number of buckets (partitions) in the table
/// - `replica_count`: how many replicas per BG
/// - `workers`: list of live worker IDs available for placement
/// - `next_bg_id`: starting BG ID for allocation (caller manages the counter)
///
/// The algorithm sorts workers by current load (ascending), then for each bucket,
/// picks `replica_count` workers in round-robin order from the sorted list.
pub fn build_table(
    table_id: u32,
    bucket_count: u32,
    replica_count: u16,
    policy: BlockGroupPolicy,
    workers: &[u32],
    next_bg_id: u32,
) -> Result<BuildTableResult, FsError> {
    let rc = replica_count as usize;
    if workers.len() < rc {
        return Err(FsError::common(format!(
            "not enough workers for table: need {} replicas, have {} workers",
            rc,
            workers.len()
        )));
    }
    if bucket_count == 0 {
        return Err(FsError::common("bucket_count must be > 0"));
    }

    let now = LocalTime::mills();

    // Token-based round-robin: interleave workers to spread load
    let worker_count = workers.len();
    let mut sorted_workers: Vec<u32> = workers.to_vec();
    sorted_workers.sort();

    let mut bgs = Vec::with_capacity(bucket_count as usize);
    let mut buckets = Vec::with_capacity(bucket_count as usize);
    let mut offset = 0usize;

    for i in 0..bucket_count {
        let bg_id = next_bg_id + i;
        let mut replica_set = Vec::with_capacity(rc);
        for j in 0..rc {
            let idx = (offset + j) % worker_count;
            replica_set.push(sorted_workers[idx]);
        }
        offset = (offset + 1) % worker_count;

        let leader = replica_set[0];
        let bg = BlockGroupInfo {
            bg_id,
            table_id,
            epoch: 1,
            replica_set,
            state: BGState::Assigned,
            lease_owner: BGLease {
                node_id: leader,
                expire_time_ms: 0,
            },
            stats: Default::default(),
        };
        buckets.push(bg_id);
        bgs.push(bg);
    }

    let table = BGTable {
        table_id,
        policy,
        bucket_count,
        buckets,
        epoch: 1,
        create_time_ms: now,
        last_rebuild_ms: now,
    };

    Ok(BuildTableResult { table, bgs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{PlacementPolicy, StorageType};
    use std::collections::HashMap;

    fn default_policy(replicas: u16) -> BlockGroupPolicy {
        BlockGroupPolicy {
            storage_type: StorageType::Ssd,
            replicas,
            placement: PlacementPolicy::Default,
        }
    }

    #[test]
    fn build_table_basic() {
        let result = build_table(
            (2u32 << 16) | 3,
            4,
            3,
            default_policy(3),
            &[10, 20, 30, 40],
            1,
        )
        .unwrap();
        assert_eq!(result.table.bucket_count, 4);
        assert_eq!(result.bgs.len(), 4);
        assert_eq!(result.table.buckets.len(), 4);
        for bg in &result.bgs {
            assert_eq!(bg.replica_set.len(), 3);
            assert_eq!(bg.state, BGState::Assigned);
            assert_eq!(bg.epoch, 1);
        }
    }

    #[test]
    fn build_table_not_enough_workers() {
        let result = build_table(1, 4, 3, default_policy(3), &[10, 20], 1);
        assert!(result.is_err());
    }

    #[test]
    fn build_table_zero_buckets() {
        let result = build_table(1, 0, 3, default_policy(3), &[10, 20, 30], 1);
        assert!(result.is_err());
    }

    #[test]
    fn build_table_spread_is_balanced() {
        let result = build_table(
            (2u32 << 16) | 2,
            8,
            2,
            default_policy(2),
            &[1, 2, 3, 4],
            100,
        )
        .unwrap();
        let mut load: HashMap<u32, usize> = HashMap::new();
        for bg in &result.bgs {
            for &w in &bg.replica_set {
                *load.entry(w).or_default() += 1;
            }
        }
        let max_load = load.values().max().copied().unwrap_or(0);
        let min_load = load.values().min().copied().unwrap_or(0);
        assert!(max_load - min_load <= 2, "load imbalance: {:?}", load);
    }

    #[test]
    fn build_table_no_duplicate_replicas_per_bg() {
        let result = build_table(
            1, 10, 3, default_policy(3), &[1, 2, 3, 4, 5], 1,
        )
        .unwrap();
        for bg in &result.bgs {
            let unique: std::collections::HashSet<_> = bg.replica_set.iter().collect();
            assert_eq!(unique.len(), bg.replica_set.len(), "duplicate in bg {}", bg.bg_id);
        }
    }
}
