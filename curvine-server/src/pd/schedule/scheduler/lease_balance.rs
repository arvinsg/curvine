use super::{BaseScheduler, Scheduler, SchedulerContext, compute_pool_scores, should_balance};
use crate::pd::config::keys;
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BGOpState;
use curvine_common::FsResult;
use std::sync::Arc;
use std::time::Duration;

/// Lease balance scheduler: transfers lease ownership from overloaded to underloaded workers.
pub struct LeaseBalanceScheduler {
    ctx: Arc<CoordinatorContext>,
}

impl LeaseBalanceScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl Scheduler for LeaseBalanceScheduler {
    fn name(&self) -> &str {
        "lease-balance-scheduler"
    }

    fn scheduler_type(&self) -> &str {
        "lease-balance"
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
                b.leader_score
                    .partial_cmp(&a.leader_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let total_leaders: f64 = scores.iter().map(|s| s.leader_count as f64).sum();
            let mean_score = total_leaders / scores.len() as f64;

            let source = &scores[0];
            let target = scores.last().unwrap();

            if !should_balance(
                source.leader_score,
                target.leader_score,
                mean_score,
                tolerant_ratio_bps,
            ) {
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
                if bg.lease_owner.as_ref().map(|l| l.node_id) != Some(source.worker_id) {
                    continue;
                }
                if !bg.replica_set.contains(&target.worker_id) {
                    continue;
                }

                let op = OperatorBuilder::new(
                    OperatorKind::LeaseTransfer,
                    bg.bg_id,
                    format!(
                        "Balance lease: transfer from {} (score={:.1}) to {} (score={:.1})",
                        source.worker_id, source.leader_score, target.worker_id, target.leader_score
                    ),
                )
                .bg_epoch(bg.bg_epoch)
                .priority(40)
                .transfer_lease(source.worker_id, target.worker_id)
                .build();

                result.push(op);
                break; // One lease transfer per source-target pair per cycle
            }
        }

        result
    }

    fn is_schedule_allowed(&self, ctx: &SchedulerContext<'_>) -> bool {
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
        let s = LeaseBalanceScheduler::new(ctx);
        assert_eq!(s.name(), "lease-balance-scheduler");
        assert_eq!(s.scheduler_type(), "lease-balance");
    }
}
