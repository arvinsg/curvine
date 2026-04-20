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
use crate::pd::bg::placement::{
    best_isolation_candidates, create_policy, filter_min_isolation,
    isolation_score, is_bg_gap_sufficient, PlacementContext,
};
use crate::pd::config::keys;
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::bg::placement::context::build_table_snapshot;
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BGOpState, ReplicaState};
use std::collections::HashSet;
use std::time::Duration;

pub struct BGBalanceScheduler;

impl Scheduler for BGBalanceScheduler {
    fn name(&self) -> &str {
        "bg-balance-scheduler"
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

            // Build per-table snapshot with operator influence.
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

            let rule = ctx.bg_manager.placement_rule();
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
                    // Skip BGs with non-Active replicas (already in flux)
                    let serving = ctx.bg_manager.get_serving_replicas(bg.bg_id);
                    if serving.len() != bg.replica_set.len() {
                        continue;
                    }

                    // Use Resident view for topology evaluation
                    let resident = ctx.bg_manager.get_resident_replicas(bg.bg_id);

                    // Get legal targets: isolation → policy filter → selector pick.
                    let all_ids = placement_ctx.worker_ids();
                    let constrained = rule.filter(&all_ids, &worker_labels);
                    let hard_filtered = if let Some(ref min_level) = rule.min_isolation_level {
                        let f = filter_min_isolation(
                            &constrained,
                            &resident,
                            min_level,
                            &rule.location_labels,
                            &worker_labels,
                        );
                        if f.is_empty() { constrained.clone() } else { f }
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
                        let old_iso = isolation_score(
                            &resident,
                            &rule.location_labels,
                            &worker_labels,
                        );
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

                    // Build operator: AddReplica → (optional TransferLease) → RemoveReplica.
                    let mut builder = OperatorBuilder::new(
                        OperatorKind::Balance,
                        bg.bg_id,
                        format!(
                            "Balance BG: move from worker {} to {}",
                            source_id, picked
                        ),
                    )
                    .bg_epoch(bg.bg_epoch)
                    .priority(OpPriority::BG_BALANCE)
                    .add_replica(picked)
                    .wait_replica_ready(picked, ReplicaState::Active);

                    if bg
                        .lease_owner
                        .as_ref()
                        .map(|l| l.node_id == source_id)
                        .unwrap_or(false)
                    {
                        let to_worker = bg
                            .replica_set
                            .iter()
                            .filter(|&&w| w != source_id)
                            .find(|&&w| ctx.pool_manager.is_worker_available(w))
                            .copied()
                            .unwrap_or(picked);
                        builder = builder.transfer_lease(source_id, to_worker);
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

    fn is_schedule_allowed(&self, ctx: &ManagerContext) -> bool {
        ctx.config_manager.get_bool(
            keys::PD_SCHEDULE_BALANCE_BG_ENABLED,
            keys::PD_SCHEDULE_BALANCE_BG_ENABLED_DEFAULT,
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
        let s = BGBalanceScheduler;
        assert_eq!(s.name(), "bg-balance-scheduler");
    }
}
