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
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::{BGOpState, BG_FLAG_LEASE_INVALID};
use std::collections::HashSet;
use std::sync::Arc;

/// LeaseValidityChecker: detects lease expiry and LEASE_INVALID flags,
/// generates LeaseTransfer operators.
pub struct LeaseValidityChecker {
    ctx: Arc<CoordinatorContext>,
}

impl LeaseValidityChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self { ctx }
    }
}

impl super::Checker for LeaseValidityChecker {
    fn name(&self) -> &str {
        "lease-validity-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64("pd.schedule.lease_check_interval_ms", 10_000)
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();
        let mut processed_bg_ids = HashSet::new();

        // Phase 1: Priority-check suspect BGs for lease issues
        for bg in &ctx.suspect_bgs {
            processed_bg_ids.insert(bg.bg_id);
            if bg.op_state != BGOpState::Idle {
                continue;
            }
            if bg.flags & BG_FLAG_LEASE_INVALID != 0 {
                if let Some(op) = self.build_lease_transfer(bg, ctx) {
                    result.bg_operators.push(op);
                }
            } else {
                // No lease issue — clear suspect (from lease checker perspective)
                ctx.bg_manager.clear_suspect(bg.bg_id);
            }
        }

        // Phase 2: LEASE_INVALID flag-based detection
        let flagged_bgs: Vec<_> = ctx
            .bg_manager
            .list_bgs()
            .into_iter()
            .filter(|bg| bg.flags & BG_FLAG_LEASE_INVALID != 0)
            .filter(|bg| bg.op_state == BGOpState::Idle)
            .filter(|bg| !processed_bg_ids.contains(&bg.bg_id))
            .collect();

        for bg in flagged_bgs {
            if let Some(op) = self.build_lease_transfer(&bg, ctx) {
                result.bg_operators.push(op);
            }
        }

        result
    }
}

impl LeaseValidityChecker {
    fn build_lease_transfer(
        &self,
        bg: &curvine_common::state::BlockGroupInfo,
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        let new_owner = bg
            .replica_set
            .iter()
            .find(|w| ctx.pool_manager.is_worker_available(**w))
            .copied()?;

        let old_worker = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        if new_owner == old_worker {
            return None;
        }

        Some(
            OperatorBuilder::new(
                OperatorKind::LeaseTransfer,
                bg.bg_id,
                format!("Transfer lease from {} to {}", old_worker, new_owner),
            )
            .bg_epoch(bg.bg_epoch)
            .transfer_lease(old_worker, new_owner)
            .priority(80)
            .build(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::{Checker, CheckerContext};
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
    fn lease_invalid_flag_no_available_workers_returns_empty() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = LeaseValidityChecker::new(ctx.clone());

        // Insert a BG with LEASE_INVALID flag
        let bg = make_bg(1, 0x0001_0001, vec![100, 101], BG_FLAG_LEASE_INVALID);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg,
            })
            .unwrap();

        let cctx = checker_ctx(&ctx);
        let result = checker.check(&cctx);
        // No workers registered in pool, so is_worker_available returns false
        // for all replicas -> no LeaseTransfer can be built
        assert!(
            result.bg_operators.is_empty(),
            "lease invalid with no available workers should produce no operators"
        );
    }

    #[test]
    fn no_ops_for_valid_lease() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = LeaseValidityChecker::new(ctx.clone());

        // Insert a BG with no LEASE_INVALID flag (valid lease)
        let bg = make_bg(1, 0x0001_0001, vec![100, 101], BG_FLAG_NONE);
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
            "valid lease should produce no operators"
        );
    }

    #[test]
    fn skips_bg_with_non_idle_op_state() {
        let ctx = test_ctx(std::collections::HashMap::new());
        let checker = LeaseValidityChecker::new(ctx.clone());

        // Insert a BG with LEASE_INVALID flag but op_state != Idle
        let mut bg = make_bg(1, 0x0001_0001, vec![100, 101], BG_FLAG_LEASE_INVALID);
        bg.op_state = BGOpState::LeaseBalancing;
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
        let checker = LeaseValidityChecker::new(ctx);
        assert_eq!(checker.name(), "lease-validity-checker");
    }
}
