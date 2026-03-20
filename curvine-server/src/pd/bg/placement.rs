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
use crate::pd::schedule::placement::rule::PlacementRule;
use curvine_common::state::{
    BGLease, BGState, BlockGroupInfo, BlockGroupPolicy, PlacementPolicy, BG_FLAG_NONE,
};
use curvine_common::FsError;
use orpc::common::LocalTime;
use std::collections::HashMap;

/// Result of building a new BGTable: the table metadata plus the BG entries.
pub struct BuildTableResult {
    pub table: BGTable,
    pub bgs: Vec<BlockGroupInfo>,
}

/// Build a new BGTable with token-based placement, optionally placement-aware.
///
/// - `table_id`: pre-computed table_id encoding pool_id and replica_count
/// - `bucket_count`: number of buckets (partitions) in the table
/// - `replica_count`: how many replicas per BG
/// - `workers`: list of live worker IDs available for placement
/// - `next_bg_id`: starting BG ID for allocation (caller manages the counter)
/// - `worker_labels`: label map per worker for placement-aware sorting
/// - `rules`: placement rules for isolation-aware selection
///
/// The algorithm sorts workers, then for each bucket picks `replica_count`
/// workers in round-robin order. When rules are provided, workers are sorted
/// by isolation from already-selected workers in each bucket.
pub fn build_table(
    table_id: u32,
    bucket_count: u32,
    replica_count: u16,
    policy: BlockGroupPolicy,
    workers: &[u32],
    next_bg_id: u32,
    worker_labels: &HashMap<u32, HashMap<String, String>>,
    rules: &[PlacementRule],
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

    let worker_count = workers.len();
    let mut sorted_workers: Vec<u32> = workers.to_vec();
    sorted_workers.sort();

    let has_rules = !rules.is_empty()
        && rules.iter().any(|r| !r.location_labels.is_empty() || !r.label_constraints.is_empty());

    // Track BG count per worker for load balancing
    let mut worker_bg_count: HashMap<u32, u32> = sorted_workers.iter().map(|&w| (w, 0)).collect();

    let mut bgs = Vec::with_capacity(bucket_count as usize);
    let mut buckets = Vec::with_capacity(bucket_count as usize);
    let mut offset = 0usize;

    for i in 0..bucket_count {
        let bg_id = next_bg_id + i;

        let replica_set = if has_rules {
            // Placement-aware selection: pick replicas greedily by isolation
            select_replicas_with_isolation(
                rc,
                &sorted_workers,
                worker_labels,
                rules,
                &worker_bg_count,
            )
        } else {
            // Simple round-robin
            let mut rs = Vec::with_capacity(rc);
            for j in 0..rc {
                let idx = (offset + j) % worker_count;
                rs.push(sorted_workers[idx]);
            }
            offset = (offset + 1) % worker_count;
            rs
        };

        // Update worker BG counts
        for &w in &replica_set {
            *worker_bg_count.entry(w).or_default() += 1;
        }

        let leader = replica_set[0];
        let bg = BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            lease_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            flags: BG_FLAG_NONE,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: leader,
                expire_time_ms: 0,
            }),
            placement: PlacementPolicy::Default,
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

/// Select replicas for a single BG greedily, optimizing for isolation and load balance.
fn select_replicas_with_isolation(
    count: usize,
    workers: &[u32],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
    rules: &[PlacementRule],
    worker_bg_count: &HashMap<u32, u32>,
) -> Vec<u32> {
    let mut selected: Vec<u32> = Vec::with_capacity(count);
    let empty = HashMap::new();

    for _ in 0..count {
        let mut best_worker = None;
        let mut best_penalty = f64::MAX;
        let mut best_bg_count = u32::MAX;

        for &wid in workers {
            if selected.contains(&wid) {
                continue;
            }

            // Check constraints
            let labels = worker_labels.get(&wid).unwrap_or(&empty);
            let passes_constraints = rules.iter().all(|rule| {
                rule.label_constraints.iter().all(|c| c.matches(labels))
            });
            if !passes_constraints {
                continue;
            }

            // Compute isolation penalty against already-selected replicas
            let penalty: f64 = selected
                .iter()
                .map(|&sel| {
                    let sel_labels = worker_labels.get(&sel).unwrap_or(&empty);
                    compute_pair_penalty(labels, sel_labels, rules)
                })
                .sum();

            let bg_count = worker_bg_count.get(&wid).copied().unwrap_or(0);

            // Prefer: lower penalty, then lower bg_count
            if penalty < best_penalty || (penalty == best_penalty && bg_count < best_bg_count) {
                best_worker = Some(wid);
                best_penalty = penalty;
                best_bg_count = bg_count;
            }
        }

        if let Some(wid) = best_worker {
            selected.push(wid);
        } else {
            // Fallback: pick any non-selected worker
            for &wid in workers {
                if !selected.contains(&wid) {
                    selected.push(wid);
                    break;
                }
            }
        }
    }

    selected
}

fn compute_pair_penalty(
    labels_a: &HashMap<String, String>,
    labels_b: &HashMap<String, String>,
    rules: &[PlacementRule],
) -> f64 {
    let mut penalty = 0.0;
    for rule in rules {
        for (d, label_key) in rule.location_labels.iter().enumerate() {
            let val_a = labels_a.get(label_key);
            let val_b = labels_b.get(label_key);
            if val_a.is_some() && val_a == val_b {
                penalty += 1.0 / (d as f64 + 1.0);
                break;
            } else {
                break;
            }
        }
    }
    penalty
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{PlacementPolicy, StorageType};

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
            &HashMap::new(),
            &[],
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
        let result = build_table(1, 4, 3, default_policy(3), &[10, 20], 1, &HashMap::new(), &[]);
        assert!(result.is_err());
    }

    #[test]
    fn build_table_zero_buckets() {
        let result = build_table(1, 0, 3, default_policy(3), &[10, 20, 30], 1, &HashMap::new(), &[]);
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
            &HashMap::new(), &[],
        )
        .unwrap();
        for bg in &result.bgs {
            let unique: std::collections::HashSet<_> = bg.replica_set.iter().collect();
            assert_eq!(unique.len(), bg.replica_set.len(), "duplicate in bg {}", bg.bg_id);
        }
    }

    #[test]
    fn build_table_with_placement_rules_isolates_az() {
        use crate::pd::schedule::placement::rule::PlacementRule;

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
        )
        .unwrap();

        // Each BG should use all 3 workers (different AZs)
        for bg in &result.bgs {
            let unique: std::collections::HashSet<_> = bg.replica_set.iter().collect();
            assert_eq!(unique.len(), 3, "expected 3 unique workers per BG");
        }
    }
}
