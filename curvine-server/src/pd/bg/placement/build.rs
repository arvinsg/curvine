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

use super::rule::PlacementRule;
use super::selector::{WorkerCandidate, WorkerSelector};
use super::select_workers_for_bg;
use crate::pd::bg::BGTable;
use curvine_common::state::{BGLease, BGState, BlockGroupInfo, BlockGroupPolicy};
use curvine_common::FsError;
use orpc::common::LocalTime;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::collections::HashSet;

/// Result of building a new BGTable: the table metadata plus the BG entries.
pub struct BuildTableResult {
    pub table: BGTable,
    pub bgs: Vec<BlockGroupInfo>,
}

/// Build a new BGTable with placement-aware replica selection.
///
/// - `table_id`: pre-computed table_id encoding pool_id and replica_count
/// - `bucket_count`: number of buckets (partitions) in the table
/// - `replica_count`: how many replicas per BG
/// - `workers`: list of live worker IDs available for placement
/// - `next_bg_id`: starting BG ID for allocation (caller manages the counter)
/// - `worker_labels`: label map per worker for placement-aware sorting
/// - `rules`: placement rules for isolation-aware selection
/// - `selector`: worker selection strategy (NormalizedSelector, RandomSelector, etc.)
pub fn build_table(
    table_id: u32,
    bucket_count: u32,
    replica_count: u16,
    policy: BlockGroupPolicy,
    workers: &[u32],
    next_bg_id: u32,
    worker_labels: &HashMap<u32, HashMap<String, String>>,
    rules: &[PlacementRule],
    selector: &dyn WorkerSelector,
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

    // Track per-worker BG count and lease count for balanced placement.
    let mut worker_bg_count: HashMap<u32, u32> = workers.iter().map(|&w| (w, 0)).collect();
    let mut worker_lease_count: HashMap<u32, u32> = workers.iter().map(|&w| (w, 0)).collect();

    // Shuffle BG selection order to eliminate greedy ordering bias.
    let mut bg_indices: Vec<u32> = (0..bucket_count).collect();
    bg_indices.shuffle(&mut thread_rng());

    let mut bgs_map: HashMap<u32, (u32, BlockGroupInfo)> =
        HashMap::with_capacity(bucket_count as usize);

    for &i in &bg_indices {
        let bg_id = next_bg_id + i;

        // Build candidates with current bg_count for load-aware scoring.
        let candidates: Vec<WorkerCandidate> = workers
            .iter()
            .map(|&wid| WorkerCandidate {
                worker_id: wid,
                bg_count: worker_bg_count.get(&wid).copied().unwrap_or(0),
                // Initial build: no capacity data yet, set to 0 (selector handles this).
                capacity_bytes: 0,
                used_bytes: 0,
                labels: worker_labels.get(&wid).cloned().unwrap_or_default(),
            })
            .collect();

        let replica_set = select_workers_for_bg(
            &candidates,
            rc,
            &HashSet::new(),
            &[],
            rules,
            selector,
            worker_labels,
        );

        if replica_set.len() < rc {
            return Err(FsError::common(format!(
                "could not select enough replicas for bg {}: got {}, need {}",
                bg_id,
                replica_set.len(),
                rc
            )));
        }

        // Update per-worker BG counts.
        for &w in &replica_set {
            *worker_bg_count.entry(w).or_default() += 1;
        }

        // Lease owner: pick the worker with the fewest leases for balance.
        let lease_owner = replica_set
            .iter()
            .min_by_key(|&&w| {
                (
                    worker_lease_count.get(&w).copied().unwrap_or(0),
                    w, // deterministic tiebreaker by worker_id
                )
            })
            .copied()
            .unwrap_or(replica_set[0]);
        *worker_lease_count.entry(lease_owner).or_default() += 1;

        let bg = BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: lease_owner,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        };
        bgs_map.insert(i, (bg_id, bg));
    }

    // Reconstruct ordered BGs and buckets in original index order.
    let mut bgs = Vec::with_capacity(bucket_count as usize);
    let mut buckets = Vec::with_capacity(bucket_count as usize);
    for i in 0..bucket_count {
        let (bg_id, bg) = bgs_map.remove(&i).unwrap();
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
    use crate::pd::bg::placement::NormalizedSelector;
    use curvine_common::state::{PlacementPolicy, StorageType};

    fn default_policy(replicas: u16) -> BlockGroupPolicy {
        BlockGroupPolicy {
            storage_type: StorageType::Ssd,
            replicas,
            placement: PlacementPolicy::Default,
        }
    }

    fn default_selector() -> NormalizedSelector {
        NormalizedSelector::default()
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
            &HashMap::new(),
            &[],
            &default_selector(),
        )
        .unwrap();
        assert_eq!(result.table.bucket_count, 4);
        assert_eq!(result.bgs.len(), 4);
        assert_eq!(result.table.buckets.len(), 4);
        for bg in &result.bgs {
            assert_eq!(bg.replica_set.len(), 3);
            assert_eq!(bg.state, BGState::Assigned);
            assert_eq!(bg.bg_epoch, 1);
        }
    }

    #[test]
    fn build_table_not_enough_workers() {
        let result = build_table(
            1, 4, 3, default_policy(3), &[10, 20], 1, &HashMap::new(), &[], &default_selector(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn build_table_zero_buckets() {
        let result = build_table(
            1, 0, 3, default_policy(3), &[10, 20, 30], 1, &HashMap::new(), &[], &default_selector(),
        );
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
            &HashMap::new(),
            &[],
            &default_selector(),
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
            &HashMap::new(), &[], &default_selector(),
        )
        .unwrap();
        for bg in &result.bgs {
            let unique: HashSet<_> = bg.replica_set.iter().collect();
            assert_eq!(unique.len(), bg.replica_set.len(), "duplicate in bg {}", bg.bg_id);
        }
    }

    #[test]
    fn build_table_with_placement_rules_isolates_az() {
        let mut labels = HashMap::new();
        labels.insert(1, {
            let mut m = HashMap::new();
            m.insert("az".to_string(), "az1".to_string());
            m
        });
        labels.insert(2, {
            let mut m = HashMap::new();
            m.insert("az".to_string(), "az2".to_string());
            m
        });
        labels.insert(3, {
            let mut m = HashMap::new();
            m.insert("az".to_string(), "az3".to_string());
            m
        });

        let rules = vec![PlacementRule {
            id: "cross-az".to_string(),
            label_constraints: vec![],
            location_labels: vec!["az".to_string()],
            isolation_level: 0,
        }];

        let result = build_table(
            (2u32 << 16) | 3,
            4,
            3,
            default_policy(3),
            &[1, 2, 3],
            1,
            &labels,
            &rules,
            &default_selector(),
        )
        .unwrap();

        // Each BG should use all 3 workers (different AZs)
        for bg in &result.bgs {
            let unique: HashSet<_> = bg.replica_set.iter().collect();
            assert_eq!(unique.len(), 3, "expected 3 unique workers per BG");
        }
    }

    #[test]
    fn build_table_lease_balance() {
        // 1024 BGs, 3 replicas, 9 workers — leases should be roughly uniform.
        let workers: Vec<u32> = (1..=9).collect();
        let result = build_table(
            (2u32 << 16) | 3,
            1024,
            3,
            default_policy(3),
            &workers,
            1,
            &HashMap::new(),
            &[],
            &default_selector(),
        )
        .unwrap();

        let mut lease_count: HashMap<u32, u32> = HashMap::new();
        for bg in &result.bgs {
            if let Some(ref lease) = bg.lease_owner {
                *lease_count.entry(lease.node_id).or_default() += 1;
            }
        }

        let expected_per_worker = 1024.0 / 9.0; // ~113.8
        for &count in lease_count.values() {
            let deviation = (count as f64 - expected_per_worker).abs();
            // Allow up to 15% deviation for randomized placement.
            assert!(
                deviation < expected_per_worker * 0.15,
                "lease count {} deviates too far from expected {:.1}: {:?}",
                count,
                expected_per_worker,
                lease_count
            );
        }
    }

    #[test]
    fn build_table_bg_ids_sequential() {
        // Even though selection order is shuffled, BG IDs in buckets should be sequential.
        let result = build_table(
            1, 4, 1, default_policy(1), &[1, 2], 100,
            &HashMap::new(), &[], &default_selector(),
        )
        .unwrap();
        assert_eq!(result.table.buckets, vec![100, 101, 102, 103]);
        for (i, bg) in result.bgs.iter().enumerate() {
            assert_eq!(bg.bg_id, 100 + i as u32);
        }
    }
}
