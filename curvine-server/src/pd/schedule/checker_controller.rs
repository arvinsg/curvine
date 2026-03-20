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

use super::checker::{BGPushCommand, Checker, CheckerContext, default_checkers};
use super::operator::BGOperator;
use super::operator_controller::OperatorController;
use super::CoordinatorContext;
use std::sync::Arc;

pub struct CheckerController {
    checkers: Vec<Box<dyn Checker>>,
    last_run_ms: Vec<u64>,
    operator_controller: Arc<OperatorController>,
    ctx: Arc<CoordinatorContext>,
}

impl CheckerController {
    pub fn new(
        operator_controller: Arc<OperatorController>,
        ctx: Arc<CoordinatorContext>,
    ) -> Self {
        let checkers = default_checkers(ctx.clone());
        let last_run_ms = vec![0u64; checkers.len()];
        Self {
            checkers,
            last_run_ms,
            operator_controller,
            ctx,
        }
    }

    pub fn patrol(&mut self) -> (Vec<BGOperator>, Vec<BGPushCommand>) {
        let now = orpc::common::LocalTime::mills();

        let max_checks = self.ctx.config_manager.get_u32(
            crate::pd::config::keys::PD_SCHEDULE_SUSPECT_MAX_CHECKS,
            crate::pd::config::keys::PD_SCHEDULE_SUSPECT_MAX_CHECKS_DEFAULT,
        );
        let ttl = self.ctx.config_manager.get_u64(
            crate::pd::config::keys::PD_SCHEDULE_SUSPECT_TTL_MS,
            crate::pd::config::keys::PD_SCHEDULE_SUSPECT_TTL_MS_DEFAULT,
        );
        let suspect_bgs = self.ctx.bg_manager.take_suspect_bgs(max_checks, ttl);

        let checker_ctx = CheckerContext {
            pool_manager: self.ctx.pool_manager.as_ref(),
            bg_manager: self.ctx.bg_manager.as_ref(),
            node_manager: self.ctx.node_manager.as_ref(),
            config_manager: self.ctx.config_manager.as_ref(),
            suspect_bgs,
        };

        let mut added_ops = Vec::new();
        let mut push_commands = Vec::new();
        for (i, checker) in self.checkers.iter().enumerate() {
            if now.saturating_sub(self.last_run_ms[i]) < checker.interval_ms() {
                continue;
            }
            self.last_run_ms[i] = now;
            let result = checker.check(&checker_ctx);
            for mut op in result.bg_operators {
                op.id = self.operator_controller.next_operator_id();
                if self.operator_controller.add_operator(op.clone()) {
                    added_ops.push(op);
                }
            }
            push_commands.extend(result.bg_push_commands);
        }
        (added_ops, push_commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx(
        overrides: std::collections::HashMap<String, String>,
    ) -> (Arc<CoordinatorContext>, Arc<OperatorController>) {
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
        let ctx = Arc::new(super::super::CoordinatorContext {
            node_manager: node_mgr,
            pool_manager: pool_mgr,
            bg_manager: bg_mgr,
            config_manager: config.clone(),
            journal_client: jc,
            leader_checker: Arc::new(crate::pd::schedule::coordinator::AlwaysLeader),
        });
        let op_ctrl = Arc::new(OperatorController::new(config, ctx.bg_manager.clone()));
        (ctx, op_ctrl)
    }

    #[test]
    fn new_creates_controller_with_checkers() {
        let (ctx, op_ctrl) = test_ctx(std::collections::HashMap::new());
        let mut controller = CheckerController::new(op_ctrl, ctx);
        // Should not panic; checkers are initialized and patrol can be called
        let (ops, cmds) = controller.patrol();
        // Just verify it returns without error
        drop(ops);
        drop(cmds);
    }

    #[test]
    fn first_patrol_runs_all_checkers() {
        let (ctx, op_ctrl) = test_ctx(std::collections::HashMap::new());
        let mut controller = CheckerController::new(op_ctrl, ctx);
        // On the first call, last_run_ms is all 0, so all checkers should run.
        // With no BGs in the system, the result should be empty but all checkers
        // should have executed (last_run_ms updated to non-zero).
        let (ops, cmds) = controller.patrol();
        assert!(ops.is_empty(), "no operators expected with empty cluster");
        assert!(cmds.is_empty(), "no push commands expected with empty cluster");
        // Verify last_run_ms was updated (all should be > 0 now)
        for ts in &controller.last_run_ms {
            assert!(*ts > 0, "last_run_ms should be updated after first patrol");
        }
    }

    #[test]
    fn second_patrol_within_interval_skips() {
        let (ctx, op_ctrl) = test_ctx(std::collections::HashMap::new());
        let mut controller = CheckerController::new(op_ctrl, ctx);
        // First patrol: runs all checkers, updates last_run_ms
        let _ = controller.patrol();
        // Second patrol immediately: all checker intervals are >= 10_000ms,
        // so within the same millisecond (or a few ms later) they should all be skipped.
        let (ops, cmds) = controller.patrol();
        assert!(
            ops.is_empty(),
            "second patrol within interval should produce no operators"
        );
        assert!(
            cmds.is_empty(),
            "second patrol within interval should produce no push commands"
        );
    }

    #[test]
    fn patrol_returns_empty_with_no_bgs() {
        let (ctx, op_ctrl) = test_ctx(std::collections::HashMap::new());
        let mut controller = CheckerController::new(op_ctrl, ctx);
        // With no BGs registered in the system, patrol should return empty results
        let (ops, cmds) = controller.patrol();
        assert!(ops.is_empty(), "no operators expected when no BGs exist");
        assert!(cmds.is_empty(), "no push commands expected when no BGs exist");
    }
}
