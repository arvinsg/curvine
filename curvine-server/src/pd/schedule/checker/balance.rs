use super::{CheckResult, CheckerContext};
use crate::pd::config::keys;
use crate::pd::schedule::balance::worker_score::{compute_pool_scores, should_balance};
use crate::pd::schedule::operator::{OperatorBuilder, OperatorKind};
use crate::pd::schedule::placement::fit::{fit_bg, is_better_fit};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BGOpState;
use std::sync::Arc;

// ==================== BGBalanceChecker ====================

pub struct BGBalanceChecker {
    ctx: Arc<CoordinatorContext>,
}

impl BGBalanceChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl super::Checker for BGBalanceChecker {
    fn name(&self) -> &str {
        "bg-balance-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager.get_u64(
            keys::PD_SCHEDULE_BALANCE_BG_INTERVAL_MS,
            keys::PD_SCHEDULE_BALANCE_BG_INTERVAL_MS_DEFAULT,
        )
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();

        if !self.ctx.config_manager.get_bool(
            keys::PD_SCHEDULE_BALANCE_BG_ENABLED,
            keys::PD_SCHEDULE_BALANCE_BG_ENABLED_DEFAULT,
        ) {
            return result;
        }

        let max_ops = self.ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT,
        ) as usize;

        let tolerant_ratio_bps = self.ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS,
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS_DEFAULT,
        );

        for pool in ctx.pool_manager.list_active_pools() {
            if result.bg_operators.len() >= max_ops {
                break;
            }

            let mut scores = compute_pool_scores(&pool, ctx.bg_manager);
            if scores.len() < 2 {
                continue;
            }

            // Sort descending by bg_score
            scores.sort_by(|a, b| b.bg_score.partial_cmp(&a.bg_score).unwrap_or(std::cmp::Ordering::Equal));

            let total_bg: f64 = scores.iter().map(|s| s.bg_count as f64).sum();
            let mean_score = total_bg / scores.len() as f64;

            let source = &scores[0];
            let target = scores.last().unwrap();

            if !should_balance(source.bg_score, target.bg_score, mean_score, tolerant_ratio_bps) {
                continue;
            }

            // Find a BG on source to move to target
            let source_bgs = ctx.bg_manager.get_bgs_on_worker(source.worker_id);
            let pool_id = pool.pool_id;

            for bg in &source_bgs {
                if result.bg_operators.len() >= max_ops {
                    break;
                }
                if bg.op_state != BGOpState::Idle {
                    continue;
                }
                // Only BGs in this pool
                if (bg.table_id >> 16) as u16 != pool_id {
                    continue;
                }
                // Target must not already be in replica_set
                if bg.replica_set.contains(&target.worker_id) {
                    continue;
                }

                // Placement Safeguard: check that moving doesn't worsen placement
                let rules = ctx.bg_manager.get_pool_placement_rules(pool_id, bg.placement);
                if rules.iter().any(|r| !r.label_constraints.is_empty() || !r.location_labels.is_empty()) {
                    let worker_ids: Vec<u32> = {
                        let mut ids = bg.replica_set.clone();
                        ids.push(target.worker_id);
                        ids
                    };
                    let worker_labels = ctx.pool_manager.get_worker_labels(&worker_ids);

                    let old_fit = fit_bg(&bg.replica_set, &rules, &worker_labels);
                    let mut new_set = bg.replica_set.clone();
                    if let Some(pos) = new_set.iter().position(|&w| w == source.worker_id) {
                        new_set[pos] = target.worker_id;
                    }
                    let new_fit = fit_bg(&new_set, &rules, &worker_labels);

                    // Only allow if new fit is not worse
                    if !new_fit.is_perfect() && !is_better_fit(&old_fit, &new_fit) && new_fit.has_violation() && !old_fit.has_violation() {
                        continue; // Would worsen placement
                    }
                }

                // Build operator
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

                if bg.lease_owner.as_ref().map(|l| l.node_id == source.worker_id).unwrap_or(false) {
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
                result.bg_operators.push(builder.build());
                break; // One BG per source-target pair per cycle
            }
        }

        result
    }
}

// ==================== LeaseBalanceChecker ====================

pub struct LeaseBalanceChecker {
    ctx: Arc<CoordinatorContext>,
}

impl LeaseBalanceChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl super::Checker for LeaseBalanceChecker {
    fn name(&self) -> &str {
        "lease-balance-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager.get_u64(
            keys::PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS,
            keys::PD_SCHEDULE_BALANCE_LEADER_INTERVAL_MS_DEFAULT,
        )
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();

        if !self.ctx.config_manager.get_bool(
            keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED,
            keys::PD_SCHEDULE_BALANCE_LEADER_ENABLED_DEFAULT,
        ) {
            return result;
        }

        let max_ops = self.ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT,
        ) as usize;

        let tolerant_ratio_bps = self.ctx.config_manager.get_u32(
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS,
            keys::PD_SCHEDULE_BALANCE_TOLERANT_RATIO_BPS_DEFAULT,
        );

        for pool in ctx.pool_manager.list_active_pools() {
            if result.bg_operators.len() >= max_ops {
                break;
            }

            let mut scores = compute_pool_scores(&pool, ctx.bg_manager);
            if scores.len() < 2 {
                continue;
            }

            // Sort descending by leader_score
            scores.sort_by(|a, b| {
                b.leader_score
                    .partial_cmp(&a.leader_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let total_leaders: f64 = scores.iter().map(|s| s.leader_count as f64).sum();
            let mean_score = total_leaders / scores.len() as f64;

            let source = &scores[0];
            let target = scores.last().unwrap();

            if !should_balance(source.leader_score, target.leader_score, mean_score, tolerant_ratio_bps) {
                continue;
            }

            // Find a BG where source is lease owner and target is in replica_set
            let source_bgs = ctx.bg_manager.get_bgs_on_worker(source.worker_id);
            let pool_id = pool.pool_id;

            for bg in &source_bgs {
                if result.bg_operators.len() >= max_ops {
                    break;
                }
                if bg.op_state != BGOpState::Idle {
                    continue;
                }
                if (bg.table_id >> 16) as u16 != pool_id {
                    continue;
                }
                // Source must be lease owner
                if bg.lease_owner.as_ref().map(|l| l.node_id) != Some(source.worker_id) {
                    continue;
                }
                // Target must be in replica_set
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

                result.bg_operators.push(op);
                break; // One lease transfer per source-target pair per cycle
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use crate::pd::schedule::CoordinatorContext;
    use curvine_common::state::{
        BGLease, BGOpState, BGState, BlockGroupInfo, PlacementPolicy, BG_FLAG_NONE,
    };

    fn test_ctx() -> Arc<CoordinatorContext> {
        let store: Arc<dyn crate::pd::store::KvStore> =
            Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new());
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(crate::pd::config::ConfigManager::new(
            store.clone(),
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        let node_store = Arc::new(crate::pd::node::NodeStore::new(store.clone()));
        let node_mgr = Arc::new(crate::pd::node::NodeManager::new(
            node_store,
            config.clone(),
            jc.clone(),
        ));
        let pool_store = Arc::new(crate::pd::pool::PoolStore::new(store.clone()));
        let pool_mgr = Arc::new(crate::pd::pool::PoolManager::new(
            pool_store,
            node_mgr.clone(),
            jc.clone(),
        ));
        let bg_store = Arc::new(crate::pd::bg::BGStore::new(store));
        let bg_mgr = Arc::new(crate::pd::bg::BGManager::new(
            bg_store,
            pool_mgr.clone(),
            jc.clone(),
        ));
        Arc::new(CoordinatorContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bg_manager: bg_mgr,
            config_manager: config,
            journal_client: jc,
            leader_checker: Arc::new(crate::pd::schedule::coordinator::AlwaysLeader),
        })
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            lease_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            flags: BG_FLAG_NONE,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                expire_time_ms: 0,
            }),
            placement: PlacementPolicy::Default,
            stats: Default::default(),
        }
    }

    fn checker_ctx<'a>(ctx: &'a CoordinatorContext) -> CheckerContext<'a> {
        CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
            suspect_bgs: vec![],
        }
    }

    #[test]
    fn bg_balance_no_ops_empty() {
        let ctx = test_ctx();
        let checker = BGBalanceChecker::new(ctx.clone());
        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        assert!(result.bg_operators.is_empty());
    }

    #[test]
    fn lease_balance_no_ops_empty() {
        let ctx = test_ctx();
        let checker = LeaseBalanceChecker::new(ctx.clone());
        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        assert!(result.bg_operators.is_empty());
    }

    #[test]
    fn bg_balance_checker_name() {
        let ctx = test_ctx();
        let checker = BGBalanceChecker::new(ctx);
        assert_eq!(checker.name(), "bg-balance-checker");
    }

    #[test]
    fn lease_balance_checker_name() {
        let ctx = test_ctx();
        let checker = LeaseBalanceChecker::new(ctx);
        assert_eq!(checker.name(), "lease-balance-checker");
    }
}
