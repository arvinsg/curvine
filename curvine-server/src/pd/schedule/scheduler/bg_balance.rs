use super::{BaseScheduler, Scheduler, SchedulerContext, compute_pool_scores, should_balance};
use crate::pd::bg::placement::check_violations;
use crate::pd::config::keys;
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BGOpState;
use curvine_common::FsResult;
use std::sync::Arc;
use std::time::Duration;

/// BG count balance scheduler: moves BGs from overloaded workers to underloaded ones.
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

        let max_ops = ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT,
        ) as usize;

        let tolerant_ratio_bps = ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS,
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS_DEFAULT,
        );

        for pool in ctx.pool_manager.list_active_pools() {
            if result.len() >= max_ops {
                break;
            }

            let live_workers = ctx.pool_manager.get_live_workers(pool.pool_id);
            let mut scores =
                compute_pool_scores(&live_workers, &pool, ctx.bg_manager, Some(ctx.operator_controller));
            if scores.len() < 2 {
                continue;
            }

            scores.sort_by(|a, b| {
                b.bg_score
                    .partial_cmp(&a.bg_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let total_bg: f64 = scores.iter().map(|s| s.bg_count as f64).sum();
            let mean_score = total_bg / scores.len() as f64;

            let source = &scores[0];
            let target = scores.last().unwrap();

            if !should_balance(source.bg_score, target.bg_score, mean_score, tolerant_ratio_bps) {
                continue;
            }

            let source_bgs = ctx.bg_manager.get_bgs_on_worker(source.worker_id);
            let pool_id = pool.pool_id;

            for bg in &source_bgs {
                if result.len() >= max_ops {
                    break;
                }
                if bg.op_state != BGOpState::Idle {
                    continue;
                }
                if (bg.table_id >> 16) as u16 != pool_id {
                    continue;
                }
                if bg.replica_set.contains(&target.worker_id) {
                    continue;
                }

                // Placement safeguard
                let rules = ctx.bg_manager.get_pool_placement_rules(pool_id);
                if rules
                    .iter()
                    .any(|r| !r.label_constraints.is_empty() || !r.location_labels.is_empty())
                {
                    let worker_ids: Vec<u32> = {
                        let mut ids = bg.replica_set.clone();
                        ids.push(target.worker_id);
                        ids
                    };
                    let worker_labels = ctx.pool_manager.get_workers_labels(&worker_ids);

                    let old_violations = check_violations(&bg.replica_set, &rules, &worker_labels);
                    let mut new_set = bg.replica_set.clone();
                    if let Some(pos) = new_set.iter().position(|&w| w == source.worker_id) {
                        new_set[pos] = target.worker_id;
                    }
                    let new_violations = check_violations(&new_set, &rules, &worker_labels);

                    if new_violations.has_violation() && !old_violations.has_violation() {
                        continue;
                    }
                    if new_violations.total() > old_violations.total() {
                        continue;
                    }
                }

                let mut builder = OperatorBuilder::new(
                    OperatorKind::Balance,
                    bg.bg_id,
                    format!(
                        "Balance BG: move from worker {} (score={:.1}) to {} (score={:.1})",
                        source.worker_id, source.bg_score, target.worker_id, target.bg_score
                    ),
                )
                .bg_epoch(bg.bg_epoch)
                .priority(50)
                .add_replica(target.worker_id);

                if bg
                    .lease_owner
                    .as_ref()
                    .map(|l| l.node_id == source.worker_id)
                    .unwrap_or(false)
                {
                    let to_worker = bg
                        .replica_set
                        .iter()
                        .filter(|&&w| w != source.worker_id)
                        .find(|&&w| ctx.pool_manager.is_worker_available(w))
                        .copied()
                        .unwrap_or(target.worker_id);
                    builder = builder.transfer_lease(source.worker_id, to_worker);
                }

                builder = builder.remove_replica(source.worker_id);
                result.push(builder.build());
                break; // One BG per source-target pair per cycle
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
