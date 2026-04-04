use super::CheckerContext;
use crate::pd::bg::placement::{check_violations, find_worst_replica};
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BlockGroupInfo;
use std::sync::Arc;

pub struct PlacementRuleChecker {
    ctx: Arc<CoordinatorContext>,
}

impl PlacementRuleChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl super::Checker for PlacementRuleChecker {
    fn name(&self) -> &str {
        "placement-rule-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &CheckerContext<'_>) -> Option<BGOperator> {
        if !ctx.config_manager.get_bool(
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED,
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED_DEFAULT,
        ) {
            return None;
        }

        let pool_id = (bg.table_id >> 16) as u16;
        let rules = ctx.bg_manager.get_pool_placement_rules(pool_id);

        if rules
            .iter()
            .all(|r| r.label_constraints.is_empty() && r.location_labels.is_empty())
        {
            return None;
        }

        let worker_labels = ctx.pool_manager.get_workers_labels(&bg.replica_set);
        let current_violations = check_violations(&bg.replica_set, &rules, &worker_labels);

        if !current_violations.has_violation() {
            return None;
        }

        let (worst_worker, _score) = find_worst_replica(&bg.replica_set, &rules, &worker_labels)?;

        let all_worker_ids: Vec<u32> = ctx.pool_manager.get_live_workers(pool_id);
        let all_worker_labels = ctx.pool_manager.get_workers_labels(&all_worker_ids);

        let mut best_replacement = None;
        let mut best_violations_total = current_violations.total();

        for &candidate in &all_worker_ids {
            if bg.replica_set.contains(&candidate) {
                continue;
            }
            let mut new_set = bg.replica_set.clone();
            if let Some(pos) = new_set.iter().position(|&w| w == worst_worker) {
                new_set[pos] = candidate;
            }

            let mut combined_labels = worker_labels.clone();
            if let Some(lbl) = all_worker_labels.get(&candidate) {
                combined_labels.insert(candidate, lbl.clone());
            }

            let new_violations = check_violations(&new_set, &rules, &combined_labels);
            if new_violations.total() < best_violations_total {
                best_replacement = Some(candidate);
                best_violations_total = new_violations.total();
            }
        }

        let new_worker = best_replacement?;

        let mut builder = OperatorBuilder::new(
            OperatorKind::Balance,
            bg.bg_id,
            format!(
                "Placement fix: replace {} with {} (violations {} -> {})",
                worst_worker,
                new_worker,
                current_violations.total(),
                best_violations_total,
            ),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(90)
        .add_replica(new_worker);

        if bg
            .lease_owner
            .as_ref()
            .map(|l| l.node_id == worst_worker)
            .unwrap_or(false)
        {
            let to_worker = bg
                .replica_set
                .iter()
                .filter(|&&w| w != worst_worker)
                .find(|&&w| ctx.pool_manager.is_worker_available(w))
                .copied()
                .unwrap_or(new_worker);
            builder = builder.transfer_lease(worst_worker, to_worker);
        }

        builder = builder.remove_replica(worst_worker);
        Some(builder.build())
    }

    fn priority(&self) -> u32 {
        30
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use curvine_common::state::{BGLease, BGOpState, BGState, BlockGroupInfo};

    fn test_ctx() -> Arc<CoordinatorContext> {
        crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        )
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    fn checker_ctx(ctx: &CoordinatorContext) -> CheckerContext<'_> {
        CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
        }
    }

    #[test]
    fn no_ops_when_no_rules() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker::new(ctx.clone());
        let bg = make_bg(1, 0x0001_0001, vec![100, 101, 102]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg.clone() })
            .unwrap();
        let cctx = checker_ctx(&ctx);
        assert!(checker.check_bg(&bg, &cctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker::new(ctx);
        assert_eq!(checker.name(), "placement-rule-checker");
        assert_eq!(checker.priority(), 30);
    }
}
