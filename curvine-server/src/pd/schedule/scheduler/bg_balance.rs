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

use super::{BaseScheduler, Scheduler, SchedulerContext};
use crate::pd::bg::placement::{
    create_policy, create_selector, PlacementContext, PlacementRule,
};
use crate::pd::config::keys;
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use crate::pd::schedule::snapshot::build_table_snapshot;
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BGOpState;
use curvine_common::FsResult;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// BG balance scheduler: moves BGs from overloaded workers to underloaded ones.
///
/// Operates per-table using the unified policy framework:
/// BalancePolicy identifies sources, filters targets; WorkerSelector picks from legal set.
pub struct BGBalanceScheduler {
    ctx: Arc<CoordinatorContext>,
}

impl BGBalanceScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl Scheduler for BGBalanceScheduler {
    fn name(&self) -> &str {
        "bg-balance-scheduler"
    }

    fn scheduler_type(&self) -> &str {
        "bg-balance"
    }

    fn schedule(&self, ctx: &SchedulerContext<'_>) -> Vec<BGOperator> {
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

        let tables = ctx.bg_manager.list_tables();

        for table in &tables {
            let pool_id = table.pool_id();
            let pool = match ctx.pool_manager.get_pool(pool_id) {
                Ok(p) => p,
                Err(_) => continue,
            };

            // Build per-table snapshot with operator influence.
            let worker_snapshots = build_table_snapshot(
                table.table_id,
                ctx.bg_manager,
                ctx.pool_manager,
                Some(ctx.operator_controller),
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

            let balance_policy = create_policy("quota");
            let mut st = match balance_policy.prepare(&placement_ctx) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let rules = ctx.bg_manager.get_pool_placement_rules(pool_id);
            let rule = rules
                .first()
                .cloned()
                .unwrap_or_else(PlacementRule::default_rule);
            let worker_labels = placement_ctx.worker_labels();

            let mut selector = create_selector("quota");
            selector.init_from_policy(&placement_ctx, &st);

            // Find source workers (overloaded).
            let source_workers: Vec<u32> = placement_ctx
                .worker_ids()
                .into_iter()
                .filter(|&wid| balance_policy.should_rebalance_bg_from(&placement_ctx, &st, wid))
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

                    // Get legal targets: isolation → policy filter → selector pick.
                    let constrained = rule.filter(&worker_labels);
                    let isolated = rule.filter_isolated(
                        &constrained,
                        &bg.replica_set,
                        &worker_labels,
                    );
                    let exclude: HashSet<u32> = bg.replica_set.iter().copied().collect();
                    let targets =
                        balance_policy.filter_bg_targets(&placement_ctx, &st, &isolated, &exclude);

                    if targets.is_empty() {
                        continue;
                    }

                    let picked = match selector.select(&mut st, &targets, 1, &exclude) {
                        Ok(p) if !p.is_empty() => p[0],
                        _ => continue,
                    };

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
                    .priority(50)
                    .add_replica(picked);

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

    fn is_schedule_allowed(&self, ctx: &SchedulerContext<'_>) -> bool {
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

    fn encode_config(&self) -> FsResult<serde_json::Value> {
        Ok(serde_json::json!({
            "type": self.scheduler_type(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_type() {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let s = BGBalanceScheduler::new(ctx);
        assert_eq!(s.name(), "bg-balance-scheduler");
        assert_eq!(s.scheduler_type(), "bg-balance");
    }
}
