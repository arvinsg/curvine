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
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::BGOpState;
use std::time::Duration;

pub struct LeaseBalanceScheduler;

impl Scheduler for LeaseBalanceScheduler {
    fn name(&self) -> &str {
        "lease-balance-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        let mut result = Vec::new();

        let max_ops_per_table = ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT,
        ) as usize;

        let tolerant_ratio = ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS,
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS_DEFAULT,
        ) as f64
            / 10000.0;

        let policy_strategy = ctx.config_manager.get_string(
            keys::PD_BG_BALANCE_POLICY,
            keys::PD_BG_BALANCE_POLICY_DEFAULT,
        );

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
        ctx.config_manager.get_bool(
            keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED,
            keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED_DEFAULT,
        )
    }

    fn min_interval(&self) -> Duration {
        BaseScheduler::MIN_INTERVAL
    }

    fn next_interval(&self, current: Duration) -> Duration {
        BaseScheduler::default_next_interval(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_type() {
        let s = LeaseBalanceScheduler;
        assert_eq!(s.name(), "lease-balance-scheduler");
    }
}
