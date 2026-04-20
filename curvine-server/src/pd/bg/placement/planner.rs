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

use super::context::PlacementContext;
use super::policy::{PlacementPolicy, PolicyState, RebuildOptions, ReplicaDecision};
use super::rule::{best_isolation_candidates, filter_min_isolation, PlacementRule};
use crate::pd::bg::BGTable;
use curvine_common::state::{BGLease, BGState, BlockGroupInfo};
use curvine_common::FsError;
use orpc::common::LocalTime;
use std::collections::HashSet;

/// Result of building a new BGTable.
pub struct BuildTableResult {
    pub table: BGTable,
    pub bgs: Vec<BlockGroupInfo>,
}

/// Result of rebuilding an existing BGTable.
pub struct RebuildTableResult {
    pub updated_bgs: Vec<BlockGroupInfo>,
}

/// Build a BGTable with all BGs allocated from scratch.
pub fn build_table(
    table_id: u32,
    bucket_count: u32,
    replica_count: u16,
    next_bg_id: u32,
    ctx: &PlacementContext<'_>,
    rule: &PlacementRule,
    policy: &dyn PlacementPolicy,
    st: &mut PolicyState,
) -> Result<BuildTableResult, FsError> {
    if bucket_count == 0 {
        return Err(FsError::common("bucket_count must be > 0".to_string()));
    }

    let worker_labels = ctx.worker_labels();
    let all_ids = ctx.worker_ids();
    let constrained_workers = rule.filter(&all_ids, &worker_labels);
    if constrained_workers.len() < replica_count as usize {
        return Err(FsError::common(format!(
            "not enough workers satisfying constraints: need {}, have {}",
            replica_count,
            constrained_workers.len()
        )));
    }

    let now = LocalTime::mills();
    let mut bgs: Vec<BlockGroupInfo> = Vec::with_capacity(bucket_count as usize);

    for i in 0..bucket_count {
        let bg_id = next_bg_id + i;
        let mut replica_set: Vec<u32> = Vec::with_capacity(replica_count as usize);
        let mut exclude = HashSet::new();

        for _ in 0..replica_count {
            // Hard isolation filter (if configured).
            let hard_filtered = if let Some(ref min_level) = rule.min_isolation_level {
                let f = filter_min_isolation(
                    &constrained_workers,
                    &replica_set,
                    min_level,
                    &rule.location_labels,
                    &worker_labels,
                );
                if f.is_empty() {
                    constrained_workers.clone()
                } else {
                    f
                }
            } else {
                constrained_workers.clone()
            };

            // Soft isolation preference.
            let best = best_isolation_candidates(
                &hard_filtered,
                &replica_set,
                &rule.location_labels,
                &worker_labels,
            );

            let mut pool_ids = match policy.select_bg_targets(ctx, st, &best, 1, &exclude) {
                Ok(v) => v,
                Err(_) => vec![],
            };
            if pool_ids.is_empty() {
                log::warn!(
                    "build_table bg {}: best isolation candidates exhausted, falling back to constrained (replicas={:?})",
                    bg_id, replica_set
                );
                pool_ids =
                    match policy.select_bg_targets(ctx, st, &constrained_workers, 1, &exclude) {
                        Ok(v) => v,
                        Err(_) => vec![],
                    };
            }
            if pool_ids.is_empty() {
                log::warn!(
                    "build_table bg {}: constrained workers exhausted, trying all remaining (replicas={:?})",
                    bg_id, replica_set
                );
                let fallback: Vec<u32> = constrained_workers
                    .iter()
                    .copied()
                    .filter(|wid| !exclude.contains(wid))
                    .collect();
                if !fallback.is_empty() {
                    pool_ids =
                        match policy.select_bg_targets(ctx, st, &fallback, 1, &HashSet::new()) {
                            Ok(v) => v,
                            Err(_) => vec![],
                        };
                }
            }

            if let Some(&wid) = pool_ids.first() {
                replica_set.push(wid);
                exclude.insert(wid);
                st.record_bg_change(None, wid);
            } else {
                log::warn!(
                    "build_table bg {}: no candidate available, allocated {}/{} replicas",
                    bg_id, replica_set.len(), replica_count
                );
                break;
            }
        }

        if replica_set.len() < replica_count as usize {
            return Err(FsError::common(format!(
                "could not allocate {} replicas for bg {}, only found {}",
                replica_count,
                bg_id,
                replica_set.len()
            )));
        }

        // Select lease owner from the replica set.
        let lease_owner_id = policy.select_lease_owner(st, &replica_set)?;
        st.record_lease_change(None, lease_owner_id);

        bgs.push(BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: Default::default(),
            lease_owner: Some(BGLease {
                node_id: lease_owner_id,
                epoch: 1,
                grant_time_ms: now,
            }),
            stats: Default::default(),
        });
    }

    let buckets: Vec<u32> = (next_bg_id..next_bg_id + bucket_count).collect();
    let table = BGTable {
        table_id,
        bucket_count,
        buckets,
        epoch: 1,
        create_time_ms: now,
        last_rebuild_ms: now,
    };

    Ok(BuildTableResult { table, bgs })
}

/// Rebuild an existing BGTable incrementally.
pub fn rebuild_table(
    _table: &BGTable,
    existing_bgs: &[BlockGroupInfo],
    ctx: &PlacementContext<'_>,
    rule: &PlacementRule,
    policy: &dyn PlacementPolicy,
    st: &mut PolicyState,
    options: &RebuildOptions,
) -> Result<RebuildTableResult, FsError> {
    let worker_labels = ctx.worker_labels();
    let all_ids = ctx.worker_ids();
    let constrained_workers = rule.filter(&all_ids, &worker_labels);
    let live_workers: HashSet<u32> = constrained_workers.iter().copied().collect();

    let now = LocalTime::mills();
    let mut updated_bgs: Vec<BlockGroupInfo> = Vec::new();

    for bg in existing_bgs {
        let budget = options.max_replace_budget(bg.replica_set.len() as u16);
        let mut replaced = 0;
        let mut new_replica_set = bg.replica_set.clone();
        let mut changed = false;

        for pos in 0..new_replica_set.len() {
            let decision = policy.classify_replica(ctx, st, bg, pos, &live_workers);

            match decision {
                ReplicaDecision::Keep => continue,

                ReplicaDecision::MustReplace(_reason) => {
                    if let Some(new_wid) = find_replacement(
                        ctx,
                        st,
                        rule,
                        policy,
                        &new_replica_set,
                        &constrained_workers,
                        &worker_labels,
                    )? {
                        let old_wid = new_replica_set[pos];
                        st.record_bg_change(Some(old_wid), new_wid);
                        new_replica_set[pos] = new_wid;
                        replaced += 1;
                        changed = true;
                    }
                }

                ReplicaDecision::TryReplace(_reason) => {
                    if replaced >= budget {
                        continue;
                    }
                    if let Some(new_wid) = find_replacement(
                        ctx,
                        st,
                        rule,
                        policy,
                        &new_replica_set,
                        &constrained_workers,
                        &worker_labels,
                    )? {
                        let old_wid = new_replica_set[pos];
                        st.record_bg_change(Some(old_wid), new_wid);
                        new_replica_set[pos] = new_wid;
                        replaced += 1;
                        changed = true;
                    }
                }
            }
        }

        // Check lease owner validity.
        let mut new_lease = bg.lease_owner.clone();
        if let Some(ref lease) = bg.lease_owner {
            if !live_workers.contains(&lease.node_id) || !new_replica_set.contains(&lease.node_id) {
                let alive_replicas: Vec<u32> = new_replica_set
                    .iter()
                    .copied()
                    .filter(|wid| live_workers.contains(wid))
                    .collect();
                if !alive_replicas.is_empty() {
                    let new_owner = policy.select_lease_owner(st, &alive_replicas)?;
                    st.record_lease_change(Some(lease.node_id), new_owner);
                    new_lease = Some(BGLease {
                        node_id: new_owner,
                        epoch: lease.epoch + 1,
                        grant_time_ms: now,
                    });
                    changed = true;
                }
            }
        }

        if changed {
            let mut updated = bg.clone();
            updated.replica_set = new_replica_set;
            updated.lease_owner = new_lease;
            updated.bg_epoch += 1;
            updated_bgs.push(updated);
        }
    }

    Ok(RebuildTableResult { updated_bgs })
}

/// Find a replacement worker for a replica position.
fn find_replacement(
    ctx: &PlacementContext<'_>,
    st: &mut PolicyState,
    rule: &PlacementRule,
    policy: &dyn PlacementPolicy,
    current_replicas: &[u32],
    constrained_workers: &[u32],
    worker_labels: &std::collections::HashMap<u32, std::collections::HashMap<String, String>>,
) -> Result<Option<u32>, FsError> {
    let exclude: HashSet<u32> = current_replicas.iter().copied().collect();

    let hard_filtered = if let Some(ref min_level) = rule.min_isolation_level {
        let f = filter_min_isolation(
            constrained_workers,
            current_replicas,
            min_level,
            &rule.location_labels,
            worker_labels,
        );
        if f.is_empty() {
            constrained_workers.to_vec()
        } else {
            f
        }
    } else {
        constrained_workers.to_vec()
    };

    let best = best_isolation_candidates(
        &hard_filtered,
        current_replicas,
        &rule.location_labels,
        worker_labels,
    );

    let mut targets = match policy.select_bg_targets(ctx, st, &best, 1, &exclude) {
        Ok(v) => v,
        Err(_) => vec![],
    };
    if targets.is_empty() {
        log::warn!(
            "find_replacement: best isolation exhausted, falling back (replicas={:?})",
            current_replicas
        );
        targets = match policy.select_bg_targets(ctx, st, constrained_workers, 1, &exclude) {
            Ok(v) => v,
            Err(_) => vec![],
        };
    }
    if targets.is_empty() {
        log::warn!(
            "find_replacement: constrained exhausted, trying all remaining (replicas={:?})",
            current_replicas
        );
        let fallback: Vec<u32> = constrained_workers
            .iter()
            .copied()
            .filter(|wid| !exclude.contains(wid))
            .collect();
        if !fallback.is_empty() {
            targets = match policy.select_bg_targets(ctx, st, &fallback, 1, &HashSet::new()) {
                Ok(v) => v,
                Err(_) => vec![],
            };
        }
    }

    Ok(targets.first().copied())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bg::placement::context::WorkerLoadSnapshot;
    use crate::pd::bg::placement::quota_policy::QuotaPolicy;
    use curvine_common::state::BGOpState;
    use std::collections::HashMap;

    fn make_ctx<'a>(
        workers: &'a HashMap<u32, WorkerLoadSnapshot>,
        bucket_count: u32,
        replica_count: u16,
    ) -> PlacementContext<'a> {
        PlacementContext {
            workers,
            bucket_count,
            replica_count,
            tolerant_ratio: 0.1,
            lease_tolerant_ratio: 0.1,
        }
    }

    fn make_snapshots(ids: &[u32]) -> HashMap<u32, WorkerLoadSnapshot> {
        ids.iter()
            .map(|&wid| {
                (
                    wid,
                    WorkerLoadSnapshot {
                        worker_id: wid,
                        actual_bg: 0,
                        actual_lease: 0,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_lease_in: 0,
                        pending_lease_out: 0,
                        capacity_bytes: 1000,
                        used_bytes: 100,
                        labels: HashMap::new(),
                    },
                )
            })
            .collect()
    }

    fn make_snapshots_with_bg(data: &[(u32, u32)]) -> HashMap<u32, WorkerLoadSnapshot> {
        data.iter()
            .map(|&(wid, bg)| {
                (
                    wid,
                    WorkerLoadSnapshot {
                        worker_id: wid,
                        actual_bg: bg,
                        actual_lease: 0,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_lease_in: 0,
                        pending_lease_out: 0,
                        capacity_bytes: 1000,
                        used_bytes: 100,
                        labels: HashMap::new(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn test_build_table_basic() {
        let workers = make_snapshots(&[1, 2, 3]);
        let ctx = make_ctx(&workers, 4, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        let result = build_table(1, 4, 2, 100, &ctx, &rule, &policy, &mut st).unwrap();

        assert_eq!(result.table.bucket_count, 4);
        assert_eq!(result.bgs.len(), 4);
        for bg in &result.bgs {
            assert_eq!(bg.replica_set.len(), 2);
            let set: HashSet<u32> = bg.replica_set.iter().copied().collect();
            assert_eq!(set.len(), 2);
            assert!(bg.lease_owner.is_some());
        }
    }

    #[test]
    fn test_build_table_not_enough_workers() {
        let workers = make_snapshots(&[1]);
        let ctx = make_ctx(&workers, 4, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        let result = build_table(1, 4, 2, 100, &ctx, &rule, &policy, &mut st);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_table_zero_buckets() {
        let workers = make_snapshots(&[1, 2]);
        let ctx = make_ctx(&workers, 0, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        let result = build_table(1, 0, 2, 100, &ctx, &rule, &policy, &mut st);
        assert!(result.is_err());
    }

    #[test]
    fn test_rebuild_replaces_invalid_nodes() {
        // Worker 1 is not available (not in snapshot).
        let workers = make_snapshots_with_bg(&[(2, 1), (3, 0)]);
        let ctx = make_ctx(&workers, 1, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        let existing_bgs = vec![BlockGroupInfo {
            bg_id: 100,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1, 2], // worker 1 is invalid
            state: BGState::Active,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: 2,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }];

        let table = BGTable {
            table_id: 1,
            bucket_count: 1,
            buckets: vec![100],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
        };

        let options = RebuildOptions::default();
        let result = rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            &policy,
            &mut st,
            &options,
        )
        .unwrap();

        assert_eq!(result.updated_bgs.len(), 1);
        assert!(!result.updated_bgs[0].replica_set.contains(&1));
        assert!(result.updated_bgs[0].replica_set.contains(&3));
    }

    #[test]
    fn test_rebuild_no_changes() {
        let workers = make_snapshots_with_bg(&[(1, 1), (2, 1)]);
        let ctx = make_ctx(&workers, 1, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        let existing_bgs = vec![BlockGroupInfo {
            bg_id: 100,
            table_id: 1,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            state: BGState::Active,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }];

        let table = BGTable {
            table_id: 1,
            bucket_count: 1,
            buckets: vec![100],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
        };

        let options = RebuildOptions::default();
        let result = rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            &policy,
            &mut st,
            &options,
        )
        .unwrap();
        assert!(result.updated_bgs.is_empty());
    }

    #[test]
    fn test_rebuild_over_quota_with_budget() {
        // 3 workers, bucket=4, replica=2, total=8
        // Worker 1 has 5 BGs (over quota of ~3)
        // Workers 2,3 have 1 each
        // New worker 4 has 0
        let workers = make_snapshots_with_bg(&[(1, 5), (2, 1), (3, 1), (4, 0)]);
        let ctx = make_ctx(&workers, 4, 2);
        let rule = PlacementRule::default_rule();
        let policy = QuotaPolicy::new();
        let mut st = policy.prepare(&ctx).unwrap();

        // Create BGs where worker 1 is heavily used.
        let existing_bgs: Vec<BlockGroupInfo> = (0..4)
            .map(|i| BlockGroupInfo {
                bg_id: 100 + i,
                table_id: 1,
                bg_epoch: 1,
                replica_set: vec![1, (i % 3 + 2)], // worker 1 always, worker 2/3/4 rotating
                state: BGState::Active,
                op_state: BGOpState::Idle,
                lease_owner: Some(BGLease {
                    node_id: 1,
                    epoch: 1,
                    grant_time_ms: 0,
                }),
                stats: Default::default(),
            })
            .collect();

        let table = BGTable {
            table_id: 1,
            bucket_count: 4,
            buckets: vec![100, 101, 102, 103],
            epoch: 1,
            create_time_ms: 0,
            last_rebuild_ms: 0,
        };

        let options = RebuildOptions::default(); // max 50% = 1 per BG
        let result = rebuild_table(
            &table,
            &existing_bgs,
            &ctx,
            &rule,
            &policy,
            &mut st,
            &options,
        )
        .unwrap();

        // Some BGs should be updated (worker 1 replaced by worker 4).
        assert!(!result.updated_bgs.is_empty());
        // Worker 4 should now have some BGs.
        let w4_count = result
            .updated_bgs
            .iter()
            .filter(|bg| bg.replica_set.contains(&4))
            .count();
        assert!(w4_count > 0, "new worker 4 should receive BGs");
    }
}
