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

use super::{build_pending_influence, BaseScheduler, Scheduler};
use crate::pd::bgtable::placement::build_hash_table_snapshot;
use crate::pd::bgtable::placement::{
    best_isolation_candidates, create_hash_policy, filter_min_isolation, is_bg_gap_sufficient,
    isolation_score, HashPlacementContext, PlacementContext,
};
use crate::pd::config::keys;
use crate::pd::coordinator::policy::is_hash_balance_candidate;
use crate::pd::coordinator::{
    BGOperator, CoordinatorContext, CoordinatorEvent, OpPriority, OperatorBuilder, OperatorKind,
};
use curvine_common::state::{BGKind, ReplicaState};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct HashReplicaBalanceScheduler {
    last_register_ms: AtomicU64,
}

impl Scheduler for HashReplicaBalanceScheduler {
    fn name(&self) -> &str {
        "hash-replica-balance-scheduler"
    }

    fn schedule(&self, ctx: &CoordinatorContext) -> Vec<BGOperator> {
        let mut result = Vec::new();

        let max_ops_per_table =
            ctx.config_manager
                .get_u32(keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE) as usize;

        let tolerant_ratio =
            ctx.config_manager
                .get_u32(keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS) as f64
                / 10000.0;

        let policy_strategy = ctx.config_manager.get_string(keys::PD_BG_BALANCE_POLICY);

        let tables = ctx.bgtable_manager.list_tables();

        for table in &tables {
            if table.kind() != BGKind::Hash {
                continue;
            }
            let pool_type = table.storage_type();
            let pool = match ctx.pool_manager.get_pool(pool_type) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Build per-table snapshot with operator influence.
            let influence = build_pending_influence(&ctx.operator_controller);
            let worker_snapshots = build_hash_table_snapshot(
                table.table_id(),
                &ctx.bgtable_manager,
                &ctx.pool_manager,
                &influence,
                pool.media,
            );

            if worker_snapshots.len() < 2 {
                continue;
            }

            let placement_ctx = HashPlacementContext {
                common: PlacementContext {
                    workers: &worker_snapshots,
                    tolerant_ratio,
                    primary_tolerant_ratio: tolerant_ratio,
                },
                table_id: table.table_id(),
                bucket_count: table.hash_table().expect("hash table").bucket_count(),
                replica_count: table.replica_count(),
            };

            let balance_policy = create_hash_policy(&policy_strategy);
            let mut st = match balance_policy.prepare_hash(&placement_ctx) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let rule = ctx
                .bgtable_manager
                .hash_placement()
                .placement_rule_for_table(table);
            let worker_labels = placement_ctx.worker_labels();

            // Find source workers (overloaded).
            let source_workers: Vec<u32> = placement_ctx
                .worker_ids()
                .into_iter()
                .filter(|&wid| balance_policy.is_bg_overloaded(&placement_ctx, &st, wid))
                .collect();

            let mut table_ops = 0;

            for &source_id in &source_workers {
                if table_ops >= max_ops_per_table {
                    break;
                }

                let source_bgs = ctx.bgtable_manager.bg().bgs_on_worker(BGKind::Hash, source_id, None);

                for bg in &source_bgs {
                    if table_ops >= max_ops_per_table {
                        break;
                    }
                    if bg.table_id != table.table_id() {
                        continue;
                    }
                    if !is_hash_balance_candidate(bg) {
                        continue;
                    }
                    // Skip BGs with non-Active replicas (already in flux)
                    let serving = ctx
                        .bgtable_manager
                        .bg()
                        .active_replica_workers(BGKind::Hash, bg.bg_id);
                    if serving.len() != bg.replica_set.len() {
                        continue;
                    }

                    // Use Resident view for topology evaluation
                    let resident = ctx.bgtable_manager.bg().replica_set_workers(BGKind::Hash, bg.bg_id);

                    // Get legal targets: isolation → policy filter → selector pick.
                    let all_ids = placement_ctx.worker_ids();
                    let constrained = rule.filter(&all_ids, &worker_labels);
                    let hard_filtered = if let Some(ref min_level) = rule.min_isolation_level {
                        let filtered = filter_min_isolation(
                            &constrained,
                            &resident,
                            min_level,
                            &rule.location_labels,
                            &worker_labels,
                        );
                        if filtered.is_empty() {
                            continue;
                        }
                        filtered
                    } else {
                        constrained.clone()
                    };
                    let best = best_isolation_candidates(
                        &hard_filtered,
                        &resident,
                        &rule.location_labels,
                        &worker_labels,
                    );
                    let exclude: HashSet<u32> = resident.iter().copied().collect();
                    let picked = match balance_policy.select_bg_targets(
                        &placement_ctx,
                        &st,
                        &best,
                        1,
                        &exclude,
                    ) {
                        Ok(v) if !v.is_empty() => v[0],
                        _ => continue,
                    };

                    // Safety check 1: source-target gap must be large enough.
                    if !is_bg_gap_sufficient(&st, source_id, picked, tolerant_ratio) {
                        continue;
                    }

                    // Safety check 2: isolation must not worsen.
                    if !rule.location_labels.is_empty() {
                        let old_iso =
                            isolation_score(&resident, &rule.location_labels, &worker_labels);
                        let mut new_set = resident.clone();
                        if let Some(pos) = new_set.iter().position(|&w| w == source_id) {
                            new_set[pos] = picked;
                        }
                        let new_iso =
                            isolation_score(&new_set, &rule.location_labels, &worker_labels);
                        if new_iso < old_iso {
                            continue;
                        }
                    }

                    // Build operator: AddReplica → (optional TransferPrimary) → RemoveReplica.
                    let mut builder = OperatorBuilder::new(
                        BGKind::Hash,
                        OperatorKind::Balance,
                        bg.bg_id,
                        format!("Balance BG: move from worker {} to {}", source_id, picked),
                    )
                    .bg_epoch(bg.bg_epoch)
                    .priority(OpPriority::BG_BALANCE)
                    .add_replica(picked)
                    .wait_replica_ready(picked, ReplicaState::Active);

                    if bg.primary.node_id == source_id {
                        let to_worker = ctx.pick_primary_fallback(bg, source_id, picked);
                        builder = builder.transfer_primary(source_id, to_worker);
                    }

                    builder = builder.remove_replica(source_id);
                    result.push(builder.build());

                    st.record_bg_change(Some(source_id), picked);
                    table_ops += 1;
                    break; // One BG per source per cycle.
                }
            }
        }

        result
    }

    fn is_schedule_allowed(&self, ctx: &CoordinatorContext) -> bool {
        if !ctx
            .config_manager
            .get_bool(keys::PD_SCHEDULE_BALANCE_BG_ENABLED)
        {
            return false;
        }
        let delay_ms = ctx
            .config_manager
            .get_u64(keys::PD_SCHEDULE_BALANCE_POST_REGISTER_DELAY_MS);
        let last = self.last_register_ms.load(Ordering::Relaxed);
        last == 0 || orpc::common::LocalTime::mills().saturating_sub(last) >= delay_ms
    }

    fn min_interval(&self) -> Duration {
        BaseScheduler::MIN_INTERVAL
    }

    fn next_interval(&self, current: Duration) -> Duration {
        BaseScheduler::default_next_interval(current)
    }

    fn on_event(&self, event: &CoordinatorEvent) {
        if let CoordinatorEvent::WorkerJoinedPools { event_time_ms, .. } = event {
            self.last_register_ms
                .store(*event_time_ms, Ordering::Relaxed);
        }
    }

    fn on_leader_start(&self) {
        self.last_register_ms
            .store(orpc::common::LocalTime::mills(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::coordinator::checker::tests_common::{decompose, Fixture};
    use curvine_common::state::BGOpState;
    use curvine_common::state::StorageType;
    use std::collections::HashMap;

    #[test]
    fn name_and_type() {
        let s = HashReplicaBalanceScheduler::default();
        assert_eq!(s.name(), "hash-replica-balance-scheduler");
    }

    fn config_with(overrides: &[(&str, &str)]) -> HashMap<String, String> {
        overrides
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn seed_imbalanced_bgs(
        f: &Fixture,
        table_id: curvine_common::state::TableId,
        workers: &[u32],
        replica_count: usize,
        bg_count: u32,
    ) {
        assert!(workers.len() >= replica_count);
        let replica_set: Vec<u32> = workers[..replica_count].to_vec();
        let bg_ids: Vec<curvine_common::state::BgId> =
            (0..bg_count).map(|i| 1_000 + i as u64).collect();
        for &bg_id in &bg_ids {
            f.insert_bg(bg_id, table_id, replica_set.clone(), None);
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);
    }

    #[test]
    fn disabled_by_config_reports_not_allowed() {
        let f = Fixture::with_overrides(config_with(&[(
            crate::pd::config::keys::PD_SCHEDULE_BALANCE_BG_ENABLED,
            "false",
        )]));
        assert!(!HashReplicaBalanceScheduler::default().is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn enabled_by_default() {
        let f = Fixture::new();
        assert!(HashReplicaBalanceScheduler::default().is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn no_ops_when_fewer_than_two_workers() {
        let f = Fixture::new();
        f.add_worker(100, StorageType::Ssd, &[]);
        f.insert_table(StorageType::Ssd, 3);
        assert!(HashReplicaBalanceScheduler::default()
            .schedule(&f.ctx)
            .is_empty());
    }

    #[test]
    fn no_ops_when_balanced() {
        // 4 workers, 4 BGs each with replica_count=3 → 12 replica slots / 4 workers = 3 each.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        let bg_ids: Vec<curvine_common::state::BgId> = vec![10, 11, 12, 13];
        for (bg_id, set) in [
            (10, vec![100, 101, 102]),
            (11, vec![101, 102, 103]),
            (12, vec![102, 103, 100]),
            (13, vec![103, 100, 101]),
        ] {
            f.insert_bg(bg_id, table_id, set, None);
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        assert!(
            ops.is_empty(),
            "balanced cluster should produce no ops, got {:?}",
            ops
        );
    }

    #[test]
    fn imbalanced_triggers_balance_op() {
        // 4 workers, 8 BGs all pinned to first 3 → worker 103 has 0 replicas.
        // bucket_count=8 → per-worker quota = 24/4 = 6, threshold = 7. Overloaded 100/101/102 hold 8.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        seed_imbalanced_bgs(&f, table_id, &[100, 101, 102, 103], 3, 8);

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        assert!(
            !ops.is_empty(),
            "imbalanced cluster should produce at least one op"
        );

        let op = &ops[0];
        assert_eq!(op.priority, OpPriority::BG_BALANCE);
        let (add, remove, _) = decompose(op);
        assert_eq!(add.len(), 1, "one AddReplica");
        assert_eq!(remove.len(), 1, "one RemoveReplica");
        assert!(
            [100, 101, 102].contains(&remove[0]),
            "source from overloaded workers"
        );
        assert_eq!(add[0], 103, "target is the underloaded worker");
    }

    #[test]
    fn max_ops_per_cycle_caps_output() {
        let f = Fixture::with_overrides(config_with(&[(
            crate::pd::config::keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            "1",
        )]));
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        seed_imbalanced_bgs(&f, table_id, &[100, 101, 102, 103], 3, 8);

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        assert_eq!(ops.len(), 1, "max_ops=1 enforces single op per cycle");
    }

    #[test]
    fn non_idle_bgs_are_skipped() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        seed_imbalanced_bgs(&f, table_id, &[100, 101, 102, 103], 3, 8);
        for bg_id in 1_000..1_008 {
            f.ctx
                .bgtable_manager
                .bg()
                .set_op_state(BGKind::Hash, bg_id, BGOpState::Repairing);
        }

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        assert!(ops.is_empty(), "all BGs non-Idle → no balance ops");
    }

    #[test]
    fn source_primary_triggers_primary_transfer() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        let bg_ids: Vec<curvine_common::state::BgId> = (0..8).map(|i| 1_000 + i as u64).collect();
        for &bg_id in &bg_ids {
            f.insert_bg(bg_id, table_id, vec![100, 101, 102], Some(100));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        let op = ops.iter().find(|o| {
            let (_, remove, _) = decompose(o);
            remove.first() == Some(&100)
        });
        let op = op.expect("expected an op whose source is primary 100");
        let (_, _, transfer) = decompose(op);
        assert_eq!(
            transfer.len(),
            1,
            "primary removal requires TransferPrimary"
        );
        assert_eq!(transfer[0].0, 100, "transfer from primary");
        assert!(
            [101, 102, 103].contains(&transfer[0].1),
            "transfer to available replica/target, got {}",
            transfer[0].1
        );
    }

    #[test]
    fn source_not_primary_has_no_primary_transfer() {
        // Primary = one of the overloaded workers, but another overloaded worker can also
        // be the source. When source != primary, the op must NOT contain TransferPrimary.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], StorageType::Ssd);
        let table_id = f.insert_table(StorageType::Ssd, 3);
        let bg_ids: Vec<curvine_common::state::BgId> = (0..8).map(|i| 1_000 + i as u64).collect();
        for &bg_id in &bg_ids {
            f.insert_bg(bg_id, table_id, vec![100, 101, 102], Some(100));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);

        let ops = HashReplicaBalanceScheduler::default().schedule(&f.ctx);
        // For each op, the presence of TransferPrimary must match (source == primary).
        for op in &ops {
            let (_, remove, transfer) = decompose(op);
            let source = remove[0];
            let bg_id = op.bg_id;
            let bg = f.ctx.bgtable_manager.bg().get_bg(BGKind::Hash, bg_id).unwrap();
            let primary = bg.primary.node_id;
            if source == primary {
                assert_eq!(
                    transfer.len(),
                    1,
                    "source==primary → expected TransferPrimary"
                );
            } else {
                assert!(
                    transfer.is_empty(),
                    "source!=primary → expected no TransferPrimary, got {:?}",
                    transfer
                );
            }
        }
    }
}
