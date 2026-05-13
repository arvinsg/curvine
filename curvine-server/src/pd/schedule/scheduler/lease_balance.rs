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
use crate::pd::bg::placement::context::build_table_snapshot;
use crate::pd::bg::placement::{create_policy, is_lease_gap_sufficient, PlacementContext};
use crate::pd::config::keys;
use crate::pd::schedule::{
    BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind, ScheduleEvent,
};
use curvine_common::state::BGOpState;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct LeaseBalanceScheduler {
    last_register_ms: AtomicU64,
}

impl Scheduler for LeaseBalanceScheduler {
    fn name(&self) -> &str {
        "lease-balance-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        let mut result = Vec::new();

        let max_ops_per_table =
            ctx.config_manager
                .get_u32(keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE) as usize;

        let tolerant_ratio =
            ctx.config_manager
                .get_u32(keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS) as f64
                / 10000.0;

        let policy_strategy = ctx.config_manager.get_string(keys::PD_BG_BALANCE_POLICY);

        let tables = ctx.bg_manager.list_tables();

        for table in &tables {
            let pool_id = table.pool_id();
            let pool = match ctx.pool_manager.get_pool(pool_id) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let influence = build_pending_influence(&ctx.operator_controller);
            let worker_snapshots = build_table_snapshot(
                table.table_id,
                &ctx.bg_manager,
                &ctx.pool_manager,
                &influence,
                pool.media,
            );

            if worker_snapshots.len() < 2 {
                continue;
            }

            let placement_ctx = PlacementContext {
                workers: &worker_snapshots,
                bucket_count: table.bucket_count,
                replica_count: table.replica_count(),
                tolerant_ratio,
                lease_tolerant_ratio: tolerant_ratio,
            };

            let balance_policy = create_policy(&policy_strategy);
            let mut st = match balance_policy.prepare(&placement_ctx) {
                Ok(s) => s,
                Err(_) => continue,
            };

            // Find source workers (lease overloaded).
            let source_workers: Vec<u32> = placement_ctx
                .worker_ids()
                .into_iter()
                .filter(|&wid| balance_policy.is_lease_overloaded(&placement_ctx, &st, wid))
                .collect();

            let mut table_ops = 0;

            for &source_id in &source_workers {
                if table_ops >= max_ops_per_table {
                    break;
                }

                let source_bgs = ctx.bg_manager.get_bgs_on_worker(source_id);

                for bg in &source_bgs {
                    if table_ops >= max_ops_per_table {
                        break;
                    }
                    if bg.op_state != BGOpState::Idle {
                        continue;
                    }
                    if bg.table_id != table.table_id {
                        continue;
                    }
                    // Source must be the lease owner.
                    if bg.lease_owner.as_ref().map(|l| l.node_id) != Some(source_id) {
                        continue;
                    }

                    // Target must be in replica_set and pass policy filter.
                    let replica_targets: Vec<u32> = bg
                        .replica_set
                        .iter()
                        .copied()
                        .filter(|&w| w != source_id)
                        .collect();

                    let target = match balance_policy.select_lease_owner(&st, &replica_targets) {
                        Ok(t) => t,
                        Err(_) => continue,
                    };

                    if !is_lease_gap_sufficient(&st, source_id, target, tolerant_ratio) {
                        continue;
                    }

                    let builder = OperatorBuilder::new(
                        OperatorKind::LeaseTransfer,
                        bg.bg_id,
                        format!(
                            "Lease balance: transfer from worker {} to {}",
                            source_id, target
                        ),
                    )
                    .bg_epoch(bg.bg_epoch)
                    .priority(OpPriority::LEASE_BALANCE)
                    .transfer_lease(source_id, target);

                    result.push(builder.build());

                    st.record_lease_change(Some(source_id), target);
                    table_ops += 1;
                    break; // One lease per source per cycle.
                }
            }
        }

        result
    }

    fn is_schedule_allowed(&self, ctx: &ManagerContext) -> bool {
        if !ctx
            .config_manager
            .get_bool(keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED)
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

    fn on_event(&self, event: &ScheduleEvent) {
        if let ScheduleEvent::WorkerJoinedPools { event_time_ms, .. } = event {
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
    use crate::pd::config::keys;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use std::collections::HashMap;

    #[test]
    fn name_and_type() {
        let s = LeaseBalanceScheduler::default();
        assert_eq!(s.name(), "lease-balance-scheduler");
    }

    fn config_with(overrides: &[(&str, &str)]) -> HashMap<String, String> {
        overrides
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Seed `bg_count` BGs with the same replica_set where worker `owner` is the lease holder.
    /// Returns bg_ids so the caller can `set_table_buckets`.
    fn seed_bgs_with_owner(
        f: &Fixture,
        table_id: u32,
        replica_set: Vec<u32>,
        owner: u32,
        bg_count: u32,
    ) -> Vec<u32> {
        let bg_ids: Vec<u32> = (0..bg_count).map(|i| 2_000 + i).collect();
        for &bg_id in &bg_ids {
            f.insert_bg(bg_id, table_id, replica_set.clone(), Some(owner));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);
        bg_ids
    }

    #[test]
    fn disabled_by_config() {
        let f = Fixture::with_overrides(config_with(&[(
            keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED,
            "false",
        )]));
        assert!(!LeaseBalanceScheduler::default().is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn enabled_by_default() {
        let f = Fixture::new();
        assert!(LeaseBalanceScheduler::default().is_schedule_allowed(&f.ctx));
    }

    #[test]
    fn no_ops_when_fewer_than_two_workers() {
        let f = Fixture::new();
        f.add_worker(100, POOL_ID_SSD, &[]);
        f.insert_table(POOL_ID_SSD, 3);
        assert!(LeaseBalanceScheduler::default().schedule(&f.ctx).is_empty());
    }

    #[test]
    fn no_ops_when_lease_balanced() {
        // Each worker is lease owner for exactly 2 BGs (8 BGs / 4 workers = 2).
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let replica_set = vec![100, 101, 102, 103]; // Not quite — replica_count=3. Use rotation instead.
        let _ = replica_set;
        let bg_ids: Vec<u32> = (0..8)
            .collect::<Vec<u32>>()
            .iter()
            .map(|i| 2_000 + i)
            .collect();
        let owners = [100, 101, 102, 103, 100, 101, 102, 103];
        for (i, &bg_id) in bg_ids.iter().enumerate() {
            let owner = owners[i];
            // Ensure owner is in replica_set.
            let mut rs = vec![100, 101, 102];
            if !rs.contains(&owner) {
                rs[0] = owner;
            }
            f.insert_bg(bg_id, table_id, rs, Some(owner));
            f.activate_all_replicas(bg_id);
        }
        f.set_table_buckets(table_id, &bg_ids);

        let ops = LeaseBalanceScheduler::default().schedule(&f.ctx);
        assert!(
            ops.is_empty(),
            "balanced leases should yield no ops, got {:?}",
            ops
        );
    }

    #[test]
    fn overloaded_lease_owner_triggers_transfer() {
        // Worker 100 owns ALL 8 leases; other replicas in replica_set get zero.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        seed_bgs_with_owner(&f, table_id, vec![100, 101, 102], 100, 8);

        let ops = LeaseBalanceScheduler::default().schedule(&f.ctx);
        assert!(
            !ops.is_empty(),
            "overloaded lease owner should yield at least one transfer"
        );

        for op in &ops {
            assert_eq!(op.priority, OpPriority::LEASE_BALANCE);
            let (add, remove, transfer) = decompose(op);
            assert!(
                add.is_empty() && remove.is_empty(),
                "lease balance uses TransferLease only"
            );
            assert_eq!(transfer.len(), 1, "one TransferLease");
            let (from, to) = transfer[0];
            assert_eq!(from, 100, "source is overloaded lease owner");
            assert!(
                [101, 102].contains(&to),
                "target is a replica peer (was {})",
                to
            );
        }
    }

    #[test]
    fn max_ops_per_cycle_caps_output() {
        let f = Fixture::with_overrides(config_with(&[(
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            "1",
        )]));
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        seed_bgs_with_owner(&f, table_id, vec![100, 101, 102], 100, 8);

        let ops = LeaseBalanceScheduler::default().schedule(&f.ctx);
        assert_eq!(ops.len(), 1, "max_ops=1 enforces single op per cycle");
    }

    #[test]
    fn skips_bg_when_source_is_not_lease_owner() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        seed_bgs_with_owner(&f, table_id, vec![100, 101, 103], 103, 8);

        let ops = LeaseBalanceScheduler::default().schedule(&f.ctx);
        for op in &ops {
            let (_, _, transfer) = decompose(op);
            assert_eq!(
                transfer[0].0, 103,
                "source must be the overloaded lease holder"
            );
        }
    }

    #[test]
    fn skips_non_idle_bgs() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg_ids = seed_bgs_with_owner(&f, table_id, vec![100, 101, 102], 100, 8);
        for &bg_id in &bg_ids {
            f.ctx
                .bg_manager
                .set_op_state(bg_id, curvine_common::state::BGOpState::Recovering);
        }

        let ops = LeaseBalanceScheduler::default().schedule(&f.ctx);
        assert!(ops.is_empty(), "all BGs non-Idle → no lease balance ops");
    }
}
