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

use super::{CheckResult, CheckerContext};
use crate::pd::schedule::operator::{BGOperator, OperatorBuilder, OperatorKind};
use curvine_common::state::{
    BGOpState, BlockGroupInfo, NodeState, BG_FLAG_NONE, BG_FLAG_ON_DECOMMISSION_NODE,
    BG_FLAG_OVER_REPLICATED, BG_FLAG_UNAVAILABLE, BG_FLAG_UNDER_REPLICATED,
};
use std::sync::Arc;

pub struct ReplicaChecker {
    ctx: Arc<crate::pd::schedule::CoordinatorContext>,
}

impl ReplicaChecker {
    pub fn new(ctx: Arc<crate::pd::schedule::CoordinatorContext>) -> Self {
        Self { ctx }
    }

    /// Build a decommission repair operator: AddReplica + (optional) TransferLease + RemoveReplica
    fn build_decommission_repair(
        &self,
        bg: &BlockGroupInfo,
        decom_worker: u32,
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let pool_id = (bg.table_id >> 16) as u16;

        let new_workers = ctx
            .pool_manager
            .select_workers_for_bg(pool_id, 1, table.policy.placement, &bg.replica_set)
            .ok()?;
        let new_worker = *new_workers.first()?;

        let mut builder = OperatorBuilder::new(
            OperatorKind::DecommissionRepair,
            bg.bg_id,
            format!("Decommission repair: replace {} with {}", decom_worker, new_worker),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(120)
        .add_replica(new_worker);

        if bg
            .lease_owner
            .as_ref()
            .map(|l| l.node_id == decom_worker)
            .unwrap_or(false)
        {
            let to_worker = bg
                .replica_set
                .iter()
                .filter(|&&w| w != decom_worker)
                .find(|&&w| ctx.pool_manager.is_worker_available(w))
                .copied()
                .unwrap_or(new_worker);
            builder = builder.transfer_lease(decom_worker, to_worker);
        }

        Some(builder.remove_replica(decom_worker).build())
    }

    /// Build a repair operator for under-replicated BGs: AddReplica steps
    fn build_under_replicated_repair(
        &self,
        bg: &BlockGroupInfo,
        available_replicas: &[u32],
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.policy.replicas as usize;
        if available_replicas.len() >= desired {
            return None;
        }
        let needed = desired - available_replicas.len();
        let pool_id = (bg.table_id >> 16) as u16;
        let new_workers = ctx
            .pool_manager
            .select_workers_for_bg(pool_id, needed as u16, table.policy.placement, &bg.replica_set)
            .ok()?;
        if new_workers.is_empty() {
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Add {} replicas", new_workers.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(100);

        for &w in &new_workers {
            builder = builder.add_replica(w);
        }

        Some(builder.build())
    }

    /// Build a shrink operator for over-replicated BGs: RemoveReplica steps
    fn build_over_replicated_repair(
        &self,
        bg: &BlockGroupInfo,
        available_replicas: &[u32],
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.policy.replicas as usize;
        if available_replicas.len() <= desired {
            return None;
        }
        let excess = available_replicas.len() - desired;
        let lease_owner_id = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);

        let mut to_remove: Vec<u32> = available_replicas
            .iter()
            .filter(|&&w| w != lease_owner_id)
            .copied()
            .take(excess)
            .collect();
        if to_remove.len() < excess {
            to_remove.extend(
                available_replicas
                    .iter()
                    .filter(|&&w| w == lease_owner_id)
                    .copied()
                    .take(excess - to_remove.len()),
            );
        }

        if to_remove.is_empty() {
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Remove {} excess replicas", to_remove.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(60);

        for &w in &to_remove {
            builder = builder.remove_replica(w);
        }

        Some(builder.build())
    }
}

impl ReplicaChecker {
    /// Check a single BG and return operators if repair is needed.
    fn check_single_bg(
        &self,
        bg: &BlockGroupInfo,
        ctx: &CheckerContext<'_>,
        result: &mut CheckResult,
        max_concurrent: usize,
    ) -> bool {
        if bg.op_state != BGOpState::Idle {
            return false;
        }
        let replica_flags =
            BG_FLAG_UNDER_REPLICATED | BG_FLAG_UNAVAILABLE | BG_FLAG_ON_DECOMMISSION_NODE | BG_FLAG_OVER_REPLICATED;
        if (bg.flags & replica_flags) == 0 {
            return false;
        }

        let mut generated = false;

        // Decommission repair: highest priority
        if (bg.flags & BG_FLAG_ON_DECOMMISSION_NODE) != 0 {
            let decom_workers: Vec<u32> = bg
                .replica_set
                .iter()
                .filter(|&&w| {
                    ctx.pool_manager
                        .get_worker_node(w)
                        .map(|n| n.state == NodeState::Decommission)
                        .unwrap_or(false)
                })
                .copied()
                .collect();
            for decom_worker in decom_workers {
                if result.bg_operators.len() >= max_concurrent {
                    break;
                }
                if let Some(op) = self.build_decommission_repair(bg, decom_worker, ctx) {
                    result.bg_operators.push(op);
                    generated = true;
                }
            }
            return generated;
        }

        let available_replicas: Vec<u32> = bg
            .replica_set
            .iter()
            .filter(|w| ctx.pool_manager.is_worker_available(**w))
            .copied()
            .collect();

        // Under-replicated / unavailable: add replicas
        if (bg.flags & (BG_FLAG_UNDER_REPLICATED | BG_FLAG_UNAVAILABLE)) != 0 {
            if available_replicas.is_empty() {
                log::error!("BG {} all replicas lost!", bg.bg_id);
                return false;
            }
            if let Some(op) = self.build_under_replicated_repair(bg, &available_replicas, ctx) {
                result.bg_operators.push(op);
                generated = true;
            }
            return generated;
        }

        // Over-replicated: remove excess replicas
        if (bg.flags & BG_FLAG_OVER_REPLICATED) != 0 {
            if let Some(op) = self.build_over_replicated_repair(bg, &available_replicas, ctx) {
                result.bg_operators.push(op);
                generated = true;
            }
        }

        generated
    }
}

impl super::Checker for ReplicaChecker {
    fn name(&self) -> &str {
        "replica-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64("pd.schedule.patrol_interval_ms", 10_000)
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();
        let max_concurrent = self
            .ctx
            .config_manager
            .get_u32("pd.recovery.max_concurrent", 10) as usize;

        let mut processed_bg_ids = std::collections::HashSet::new();

        // Phase 1: Priority-check suspect BGs
        for bg in &ctx.suspect_bgs {
            if result.bg_operators.len() >= max_concurrent {
                break;
            }
            processed_bg_ids.insert(bg.bg_id);
            if !self.check_single_bg(bg, ctx, &mut result, max_concurrent) {
                // No issue found — clear suspect
                ctx.bg_manager.clear_suspect(bg.bg_id);
            }
        }

        // Phase 2: Full scan, skip already-processed
        let replica_flags =
            BG_FLAG_UNDER_REPLICATED | BG_FLAG_UNAVAILABLE | BG_FLAG_ON_DECOMMISSION_NODE | BG_FLAG_OVER_REPLICATED;

        let candidate_bgs: Vec<_> = ctx
            .bg_manager
            .list_bgs()
            .into_iter()
            .filter(|bg| !processed_bg_ids.contains(&bg.bg_id))
            .filter(|bg| bg.flags != BG_FLAG_NONE)
            .filter(|bg| (bg.flags & replica_flags) != 0)
            .collect();

        for bg in candidate_bgs {
            if result.bg_operators.len() >= max_concurrent {
                break;
            }
            self.check_single_bg(&bg, ctx, &mut result, max_concurrent);
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
        BGLease, BGState, BlockGroupInfo, BG_FLAG_NONE,
    };

    fn test_ctx(
        overrides: std::collections::HashMap<String, String>,
    ) -> Arc<CoordinatorContext> {
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
            overrides,
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
    fn no_ops_for_healthy_bgs() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = ReplicaChecker::new(ctx.clone());

        // Insert a BG with BG_FLAG_NONE (healthy)
        let bg = make_bg(1, 0x0001_0001, vec![100, 101, 102], BG_FLAG_NONE);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        assert!(
            result.bg_operators.is_empty(),
            "healthy BGs should produce no operators"
        );
    }

    #[test]
    fn under_replicated_no_workers_returns_empty() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = ReplicaChecker::new(ctx.clone());

        // Insert a BG flagged as under-replicated; no pool workers registered
        let bg = make_bg(
            1,
            0x0001_0001,
            vec![100, 101],
            BG_FLAG_UNDER_REPLICATED,
        );
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        // No allocatable workers in pool, so no AddReplica ops can be generated
        assert!(
            result.bg_operators.is_empty(),
            "under-replicated BG with no available workers should produce no operators"
        );
    }

    #[test]
    fn skips_bg_with_non_idle_op_state() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = ReplicaChecker::new(ctx.clone());

        // Insert a BG with under-replicated flag but op_state != Idle
        let mut bg = make_bg(
            1,
            0x0001_0001,
            vec![100, 101],
            BG_FLAG_UNDER_REPLICATED,
        );
        bg.op_state = BGOpState::Recovering;
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        assert!(
            result.bg_operators.is_empty(),
            "BG with non-idle op_state should be skipped"
        );
    }

    #[test]
    fn name_returns_correct_value() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = ReplicaChecker::new(ctx);
        assert_eq!(checker.name(), "replica-checker");
    }
}
