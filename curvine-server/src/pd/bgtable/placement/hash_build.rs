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

use super::context::HashPlacementContext;
use super::hash_policy::{HashPlacementPolicy, HashPolicyState, RebuildOptions, ReplicaDecision};
use super::rule::{best_isolation_candidates, filter_min_isolation, Labels, PlacementRule};
use crate::pd::bgtable::BGTable;
use curvine_common::state::{
    BGKind, BGPrimary, BGState, BgId, BlockGroupInfo, CacheReplicaPolicy, LabelMatch, NamespaceId,
    StorageType,
};
use curvine_common::FsError;
use orpc::common::LocalTime;
use std::collections::HashSet;

/// Result of building a new BGTable.
pub struct BuildHashTableResult {
    pub table: BGTable,
    pub bgs: Vec<BlockGroupInfo>,
}

/// Result of rebuilding an existing BGTable.
pub struct RebuildHashTableResult {
    pub updated_bgs: Vec<BlockGroupInfo>,
}

/// How to construct a fresh Hash BGTable's rows. Bundles the table-shape inputs
/// that are NOT already carried by `HashPlacementContext` (which supplies
/// `table_id` / `bucket_count` / `replica_count`).
pub struct HashTableSpec {
    pub namespace_id: NamespaceId,
    pub pool_type: StorageType,
    pub next_bg_id: BgId,
    pub worker_labels: Vec<LabelMatch>,
    pub cache_replica_policy: CacheReplicaPolicy,
}

/// A single placement computation over fixed inputs: the load context, the hard
/// constraint rule, the balance policy, and the pre-filtered candidate workers.
/// `build` / `rebuild` / the selection cascade are its methods, so those inputs
/// are threaded once via `&self` instead of down every call. The mutable
/// `HashPolicyState` is passed per call because it evolves as placements are
/// recorded.
pub struct HashPlanner<'a> {
    ctx: &'a HashPlacementContext<'a>,
    rule: &'a PlacementRule,
    policy: &'a dyn HashPlacementPolicy,
    /// Workers passing label constraints (computed once from `ctx` + `rule`).
    constrained_workers: Vec<u32>,
    /// Per-worker labels (computed once from `ctx`).
    worker_labels: Labels,
}

impl<'a> HashPlanner<'a> {
    pub fn new(
        ctx: &'a HashPlacementContext<'a>,
        rule: &'a PlacementRule,
        policy: &'a dyn HashPlacementPolicy,
    ) -> Self {
        let worker_labels = ctx.worker_labels();
        let constrained_workers = rule.filter(&ctx.worker_ids(), &worker_labels);
        Self {
            ctx,
            rule,
            policy,
            constrained_workers,
            worker_labels,
        }
    }

    /// Build a BGTable with all BGs allocated from scratch. `table_id` /
    /// `bucket_count` / `replica_count` come from the context; `spec` supplies
    /// the remaining table-shape inputs.
    pub fn build_table(
        &self,
        spec: &HashTableSpec,
        state: &mut HashPolicyState,
    ) -> Result<BuildHashTableResult, FsError> {
        let bucket_count = self.ctx.bucket_count;
        let replica_count = self.ctx.replica_count;
        let table_id = self.ctx.table_id;
        if bucket_count == 0 {
            return Err(FsError::common("bucket_count must be > 0".to_string()));
        }
        if self.constrained_workers.len() < replica_count as usize {
            return Err(FsError::common(format!(
                "not enough workers satisfying constraints: need {}, have {}",
                replica_count,
                self.constrained_workers.len()
            )));
        }

        let now = LocalTime::mills();
        let mut bgs: Vec<BlockGroupInfo> = Vec::with_capacity(bucket_count as usize);

        for i in 0..bucket_count {
            let bg_id = spec.next_bg_id + i as BgId;
            let replica_set = self.allocate_replicas(bg_id, replica_count, state)?;
            let primary_id = self.policy.select_primary(state, &replica_set)?;
            state.record_primary_change(None, primary_id);

            bgs.push(BlockGroupInfo {
                bg_id,
                table_id,
                bg_epoch: 1,
                replica_set: replica_set.clone(),
                isr: replica_set,
                kind: BGKind::Hash,
                state: BGState::Active,
                op_state: Default::default(),
                primary: BGPrimary {
                    node_id: primary_id,
                    epoch: 1,
                    grant_time_ms: now,
                },
                replicas: Default::default(),
                stats: Default::default(),
            });
        }

        let buckets: Vec<BgId> = (0..bucket_count)
            .map(|i| spec.next_bg_id + i as BgId)
            .collect();
        let table = BGTable::new_hash_table_with_config(
            table_id,
            spec.namespace_id,
            spec.pool_type,
            replica_count,
            buckets,
            spec.worker_labels.clone(),
            spec.cache_replica_policy.clone(),
        );

        Ok(BuildHashTableResult { table, bgs })
    }

    /// Allocate `replica_count` distinct replicas for a new BG.
    fn allocate_replicas(
        &self,
        bg_id: BgId,
        replica_count: u16,
        state: &mut HashPolicyState,
    ) -> Result<Vec<u32>, FsError> {
        let mut replica_set: Vec<u32> = Vec::with_capacity(replica_count as usize);
        let mut exclude = HashSet::new();
        for _ in 0..replica_count {
            let Some(wid) = self.select_with_fallback(&replica_set, &exclude, state) else {
                break;
            };
            replica_set.push(wid);
            exclude.insert(wid);
            state.record_bg_change(None, wid);
        }
        if replica_set.len() < replica_count as usize {
            return Err(FsError::common(format!(
                "could not allocate {} replicas for bg {}, only found {}",
                replica_count,
                bg_id,
                replica_set.len()
            )));
        }
        Ok(replica_set)
    }

    /// Rebuild an existing BGTable incrementally.
    pub fn rebuild_table(
        &self,
        existing_bgs: &[BlockGroupInfo],
        options: &RebuildOptions,
        state: &mut HashPolicyState,
    ) -> Result<RebuildHashTableResult, FsError> {
        let live_workers: HashSet<u32> = self.constrained_workers.iter().copied().collect();
        let now = LocalTime::mills();
        let mut updated_bgs: Vec<BlockGroupInfo> = Vec::new();

        for bg in existing_bgs {
            let budget = options.max_replace_budget(bg.replica_set.len() as u16);
            let mut new_replica_set = bg.replica_set.clone();
            let mut replaced = 0;
            let mut changed = false;

            for pos in 0..new_replica_set.len() {
                let decision = self.policy.classify_replica(self.ctx, state, bg, pos, &live_workers);
                let allowed = match decision {
                    ReplicaDecision::Keep => false,
                    ReplicaDecision::MustReplace(_) => true,
                    ReplicaDecision::TryReplace(_) => replaced < budget,
                };
                if allowed && self.replace_replica_at(pos, &mut new_replica_set, state)? {
                    replaced += 1;
                    changed = true;
                }
            }

            let new_primary =
                self.reelect_primary_if_needed(bg, &new_replica_set, &live_workers, now, state)?;
            if new_primary.is_some() {
                changed = true;
            }

            if changed {
                let mut updated = bg.clone();
                updated.replica_set = new_replica_set;
                if let Some(primary) = new_primary {
                    updated.primary = primary;
                }
                updated.bg_epoch += 1;
                updated_bgs.push(updated);
            }
        }

        Ok(RebuildHashTableResult { updated_bgs })
    }

    /// Replace the replica at `pos` with a fresh worker if one can be selected.
    /// Returns whether a replacement happened.
    fn replace_replica_at(
        &self,
        pos: usize,
        replica_set: &mut [u32],
        state: &mut HashPolicyState,
    ) -> Result<bool, FsError> {
        let exclude: HashSet<u32> = replica_set.iter().copied().collect();
        match self.select_with_fallback(replica_set, &exclude, state) {
            Some(new_wid) => {
                state.record_bg_change(Some(replica_set[pos]), new_wid);
                replica_set[pos] = new_wid;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Re-elect a primary if the current one is dead or no longer a replica.
    /// Returns the new primary (and records the change), or `None` if unchanged.
    fn reelect_primary_if_needed(
        &self,
        bg: &BlockGroupInfo,
        replica_set: &[u32],
        live_workers: &HashSet<u32>,
        now: u64,
        state: &mut HashPolicyState,
    ) -> Result<Option<BGPrimary>, FsError> {
        let owner_ok =
            live_workers.contains(&bg.primary.node_id) && replica_set.contains(&bg.primary.node_id);
        if owner_ok {
            return Ok(None);
        }
        let alive: Vec<u32> = replica_set
            .iter()
            .copied()
            .filter(|wid| live_workers.contains(wid))
            .collect();
        if alive.is_empty() {
            return Ok(None);
        }
        let new_owner = self.policy.select_primary(state, &alive)?;
        state.record_primary_change(Some(bg.primary.node_id), new_owner);
        Ok(Some(BGPrimary {
            node_id: new_owner,
            epoch: bg.primary.epoch + 1,
            grant_time_ms: now,
        }))
    }

    /// Pick one worker via the standard selection cascade:
    ///   1. best-isolation candidates (strictest),
    ///   2. legal candidates (label constraints + hard min-isolation),
    ///   3. remaining legal candidates after excluding current replicas.
    pub(crate) fn select_with_fallback(
        &self,
        current_replicas: &[u32],
        exclude: &HashSet<u32>,
        state: &mut HashPolicyState,
    ) -> Option<u32> {
        let hard_filtered = if let Some(ref min_level) = self.rule.min_isolation_level {
            let filtered = filter_min_isolation(
                &self.constrained_workers,
                current_replicas,
                min_level,
                &self.rule.location_labels,
                &self.worker_labels,
            );
            if filtered.is_empty() {
                log::debug!(
                    "Hard isolation exhausted, no candidate available (level={}, replicas={:?})",
                    min_level,
                    current_replicas
                );
                return None;
            }
            filtered
        } else {
            self.constrained_workers.clone()
        };
        let best = best_isolation_candidates(
            &hard_filtered,
            current_replicas,
            &self.rule.location_labels,
            &self.worker_labels,
        );

        if let Some(picked) = self.try_pick(state, &best, exclude) {
            return Some(picked);
        }
        log::warn!(
            "Best isolation exhausted, falling back to legal candidates (replicas={:?})",
            current_replicas
        );
        if let Some(picked) = self.try_pick(state, &hard_filtered, exclude) {
            return Some(picked);
        }
        log::warn!(
            "Legal candidates exhausted, trying remaining legal candidates (replicas={:?})",
            current_replicas
        );
        let fallback: Vec<u32> = hard_filtered
            .iter()
            .copied()
            .filter(|w| !exclude.contains(w))
            .collect();
        if fallback.is_empty() {
            return None;
        }
        self.try_pick(state, &fallback, &HashSet::new())
    }

    fn try_pick(
        &self,
        state: &HashPolicyState,
        candidates: &[u32],
        exclude: &HashSet<u32>,
    ) -> Option<u32> {
        match self.policy.select_bg_targets(self.ctx, state, candidates, 1, exclude) {
            Ok(v) => v.into_iter().next(),
            Err(e) => {
                log::debug!("policy.select_bg_targets failed: {}", e);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::bgtable::placement::{HashQuotaPolicy, WorkerLoadSnapshot};
    use curvine_common::state::{BGKind, BGOpState, StorageType};
    use std::collections::HashMap;

    fn make_ctx<'a>(
        workers: &'a HashMap<u32, WorkerLoadSnapshot>,
        bucket_count: u32,
        replica_count: u16,
    ) -> HashPlacementContext<'a> {
        HashPlacementContext {
            common: crate::pd::bgtable::placement::PlacementContext {
                workers,
                tolerant_ratio: 0.1,
                primary_tolerant_ratio: 0.1,
            },
            table_id: 1,
            bucket_count,
            replica_count,
        }
    }

    fn test_spec() -> HashTableSpec {
        HashTableSpec {
            namespace_id: 0,
            pool_type: StorageType::Ssd,
            next_bg_id: 100,
            worker_labels: vec![],
            cache_replica_policy: CacheReplicaPolicy::default(),
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
                        actual_primary: 0,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_primary_in: 0,
                        pending_primary_out: 0,
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
                        actual_primary: 0,
                        pending_bg_add: 0,
                        pending_bg_remove: 0,
                        pending_primary_in: 0,
                        pending_primary_out: 0,
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
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let result = HashPlanner::new(&ctx, &rule, &policy)
            .build_table(&test_spec(), &mut state)
            .unwrap();

        assert_eq!(
            result
                .table
                .hash_table()
                .expect("hash table")
                .bucket_count(),
            4
        );
        assert_eq!(result.bgs.len(), 4);
        for bg in &result.bgs {
            assert_eq!(bg.replica_set.len(), 2);
            let set: HashSet<u32> = bg.replica_set.iter().copied().collect();
            assert_eq!(set.len(), 2);
            assert!(bg.replica_set.contains(&bg.primary.node_id));
        }
    }

    #[test]
    fn test_build_table_not_enough_workers() {
        let workers = make_snapshots(&[1]);
        let ctx = make_ctx(&workers, 4, 2);
        let rule = PlacementRule::default_rule();
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let result = HashPlanner::new(&ctx, &rule, &policy).build_table(&test_spec(), &mut state);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_table_zero_buckets() {
        let workers = make_snapshots(&[1, 2]);
        let ctx = make_ctx(&workers, 0, 2);
        let rule = PlacementRule::default_rule();
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let result = HashPlanner::new(&ctx, &rule, &policy).build_table(&test_spec(), &mut state);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_table_respects_hard_min_isolation() {
        let mut workers = make_snapshots(&[1, 2]);
        workers
            .get_mut(&1)
            .unwrap()
            .labels
            .insert("az".into(), "az-a".into());
        workers
            .get_mut(&2)
            .unwrap()
            .labels
            .insert("az".into(), "az-a".into());

        let ctx = make_ctx(&workers, 1, 2);
        let rule = PlacementRule {
            id: "hard-az".into(),
            label_constraints: vec![],
            location_labels: vec!["az".into()],
            min_isolation_level: Some("az".into()),
        };
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let result = HashPlanner::new(&ctx, &rule, &policy).build_table(&test_spec(), &mut state);
        assert!(result.is_err());
    }

    #[test]
    fn test_rebuild_replaces_invalid_nodes() {
        // Worker 1 is not available (not in snapshot).
        let workers = make_snapshots_with_bg(&[(2, 1), (3, 0)]);
        let ctx = make_ctx(&workers, 1, 2);
        let rule = PlacementRule::default_rule();
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let existing_bgs = vec![BlockGroupInfo {
            bg_id: 100,
            table_id: 1,
            kind: BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1, 2], // worker 1 is invalid
            isr: vec![1, 2],
            state: BGState::Active,
            op_state: BGOpState::Idle,
            primary: BGPrimary {
                node_id: 2,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        }];

        let result = HashPlanner::new(&ctx, &rule, &policy)
            .rebuild_table(&existing_bgs, &RebuildOptions::default(), &mut state)
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
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        let existing_bgs = vec![BlockGroupInfo {
            bg_id: 100,
            table_id: 1,
            kind: BGKind::Hash,
            bg_epoch: 1,
            replica_set: vec![1, 2],
            isr: vec![1, 2],
            state: BGState::Active,
            op_state: BGOpState::Idle,
            primary: BGPrimary {
                node_id: 1,
                epoch: 1,
                grant_time_ms: 0,
            },
            stats: Default::default(),
            replicas: Default::default(),
        }];

        let table = BGTable::new_hash_table_with_config(
            1,
            0,
            StorageType::Ssd,
            3,
            vec![100],
            vec![],
            Default::default(),
        );
        let _ = table;

        let result = HashPlanner::new(&ctx, &rule, &policy)
            .rebuild_table(&existing_bgs, &RebuildOptions::default(), &mut state)
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
        let policy = HashQuotaPolicy::new();
        let mut state = policy.prepare_hash(&ctx).unwrap();

        // Create BGs where worker 1 is heavily used.
        let existing_bgs: Vec<BlockGroupInfo> = (0..4)
            .map(|i| BlockGroupInfo {
                bg_id: 100 + i,
                table_id: 1,
                kind: BGKind::Hash,
                bg_epoch: 1,
                replica_set: vec![1, (i % 3 + 2) as u32], // worker 1 always, worker 2/3/4 rotating
                isr: vec![1, (i % 3 + 2) as u32],
                state: BGState::Active,
                op_state: BGOpState::Idle,
                primary: BGPrimary {
                    node_id: 1,
                    epoch: 1,
                    grant_time_ms: 0,
                },
                stats: Default::default(),
                replicas: Default::default(),
            })
            .collect();

        let table = BGTable::new_hash_table_with_config(
            1,
            0,
            StorageType::Ssd,
            3,
            vec![100, 101, 102, 103],
            vec![],
            Default::default(),
        );
        let _ = table;

        // max 50% = 1 per BG
        let result = HashPlanner::new(&ctx, &rule, &policy)
            .rebuild_table(&existing_bgs, &RebuildOptions::default(), &mut state)
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
