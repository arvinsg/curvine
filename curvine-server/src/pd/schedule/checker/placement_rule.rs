use super::{CheckResult, CheckerContext};
use crate::pd::schedule::operator::{OperatorBuilder, OperatorKind};
use crate::pd::schedule::placement::fit::{fit_bg, is_better_fit};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::{BGOpState, BG_FLAG_PLACEMENT_VIOLATION};
use std::collections::HashMap;
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

    fn interval_ms(&self) -> u64 {
        self.ctx.config_manager.get_u64(
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS,
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_INTERVAL_MS_DEFAULT,
        )
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();

        if !self.ctx.config_manager.get_bool(
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED,
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED_DEFAULT,
        ) {
            return result;
        }

        let max_ops = self.ctx.config_manager.get_u32(
            crate::pd::config::keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE,
            crate::pd::config::keys::PD_SCHEDULE_BALANCE_MAX_OPS_PER_CYCLE_DEFAULT,
        ) as usize;

        // Check all BGs with placement violation flag
        let violation_bgs: Vec<_> = ctx
            .bg_manager
            .list_bgs()
            .into_iter()
            .filter(|bg| bg.op_state == BGOpState::Idle)
            .filter(|bg| (bg.flags & BG_FLAG_PLACEMENT_VIOLATION) != 0)
            .collect();

        for bg in violation_bgs {
            if result.bg_operators.len() >= max_ops {
                break;
            }

            let pool_id = (bg.table_id >> 16) as u16;
            let rules = ctx.bg_manager.get_pool_placement_rules(pool_id);

            if rules.iter().all(|r| r.label_constraints.is_empty() && r.location_labels.is_empty()) {
                // No meaningful rules — clear the flag
                ctx.bg_manager.clear_bg_flag(bg.bg_id, BG_FLAG_PLACEMENT_VIOLATION);
                continue;
            }

            let worker_labels = ctx.pool_manager.get_worker_labels(
                &bg.replica_set,
            );
            let current_fit = fit_bg(&bg.replica_set, &rules, &worker_labels);

            if current_fit.is_perfect() {
                ctx.bg_manager.clear_bg_flag(bg.bg_id, BG_FLAG_PLACEMENT_VIOLATION);
                continue;
            }

            // Find the worst replica (highest isolation penalty contribution)
            let worst_replica = find_worst_replica(&bg.replica_set, &rules, &worker_labels);
            let Some(worst_worker) = worst_replica else {
                continue;
            };

            // Find a replacement worker that improves the fit
            let pool = match ctx.pool_manager.get_pool(pool_id) {
                Ok(p) => p,
                Err(_) => continue,
            };

            let all_worker_ids: Vec<u32> = pool.allocatable_workers.iter().copied().collect();
            let all_worker_labels = ctx.pool_manager.get_worker_labels(&all_worker_ids);

            let mut best_replacement = None;
            let mut best_new_fit = None;

            for &candidate in &all_worker_ids {
                if bg.replica_set.contains(&candidate) {
                    continue;
                }
                // Simulate replacement
                let mut new_set = bg.replica_set.clone();
                if let Some(pos) = new_set.iter().position(|&w| w == worst_worker) {
                    new_set[pos] = candidate;
                }

                let mut combined_labels = worker_labels.clone();
                if let Some(lbl) = all_worker_labels.get(&candidate) {
                    combined_labels.insert(candidate, lbl.clone());
                }

                let new_fit = fit_bg(&new_set, &rules, &combined_labels);
                if is_better_fit(&current_fit, &new_fit) {
                    if best_new_fit.is_none() || is_better_fit(best_new_fit.as_ref().unwrap(), &new_fit) {
                        best_replacement = Some(candidate);
                        best_new_fit = Some(new_fit);
                    }
                }
            }

            let Some(new_worker) = best_replacement else {
                continue;
            };

            // Build operator: AddReplica + [TransferLease] + RemoveReplica
            let mut builder = OperatorBuilder::new(
                OperatorKind::Balance,
                bg.bg_id,
                format!(
                    "Placement fix: replace {} with {} for isolation",
                    worst_worker, new_worker
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
            result.bg_operators.push(builder.build());
        }

        result
    }
}

/// Find the replica that contributes most to placement violation.
fn find_worst_replica(
    replica_set: &[u32],
    rules: &[crate::pd::schedule::placement::PlacementRule],
    worker_labels: &HashMap<u32, HashMap<String, String>>,
) -> Option<u32> {
    if replica_set.is_empty() {
        return None;
    }

    let empty = HashMap::new();
    let mut worst_worker = None;
    let mut worst_score = -1.0f64;

    for &worker in replica_set {
        let labels = worker_labels.get(&worker).unwrap_or(&empty);
        let mut score = 0.0f64;

        // Penalty for constraint violations
        for rule in rules {
            if !rule.label_constraints.iter().all(|c| c.matches(labels)) {
                score += 100.0; // High penalty for constraint violation
            }
        }

        // Isolation penalty against other replicas
        for &other in replica_set {
            if other == worker {
                continue;
            }
            let other_labels = worker_labels.get(&other).unwrap_or(&empty);
            for rule in rules {
                for (d, label_key) in rule.location_labels.iter().enumerate() {
                    let val_w = labels.get(label_key);
                    let val_o = other_labels.get(label_key);
                    if val_w.is_some() && val_w == val_o {
                        score += 1.0 / (d as f64 + 1.0);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }

        if score > worst_score {
            worst_score = score;
            worst_worker = Some(worker);
        }
    }

    worst_worker
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
    use crate::pd::schedule::CoordinatorContext;
    use curvine_common::state::{BGLease, BGOpState, BGState, BlockGroupInfo, BG_FLAG_NONE};

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
            1024,
            vec![3],
            vec![],
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

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>, flags: u32) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            flags,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn no_ops_when_no_violation_flag() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker::new(ctx.clone());

        let bg = make_bg(1, 0x0001_0001, vec![100, 101, 102], BG_FLAG_NONE);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let cctx = CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
            suspect_bgs: vec![],
        };
        let result = checker.check(&cctx);
        assert!(result.bg_operators.is_empty());
    }

    #[test]
    fn skips_non_idle_bg() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker::new(ctx.clone());

        let mut bg = make_bg(1, 0x0001_0001, vec![100, 101], BG_FLAG_PLACEMENT_VIOLATION);
        bg.op_state = BGOpState::Recovering;
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry { op_ms: 0, info: bg })
            .unwrap();

        let cctx = CheckerContext {
            pool_manager: &ctx.pool_manager,
            bg_manager: &ctx.bg_manager,
            node_manager: &ctx.node_manager,
            config_manager: &ctx.config_manager,
            suspect_bgs: vec![],
        };
        let result = checker.check(&cctx);
        assert!(result.bg_operators.is_empty());
    }

    #[test]
    fn name_returns_correct_value() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker::new(ctx);
        assert_eq!(checker.name(), "placement-rule-checker");
    }
}
