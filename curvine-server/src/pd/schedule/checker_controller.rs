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

use super::checker::bg_assignment::BGAssignmentChecker;
use super::checker::{BGPushCommand, Checker, CheckerContext, default_checkers};
use super::operator::BGOperator;
use super::operator_controller::OperatorController;
use super::CoordinatorContext;
use curvine_common::state::BGOpState;
use std::sync::Arc;

/// Runs all checkers in priority order during patrol.
///
/// For each BG, checkers execute in priority order (lower = first).
/// The first checker producing an operator wins (short-circuit).
/// BGAssignmentChecker runs separately (worker-dimension).
pub struct CheckerController {
    checkers: Vec<Box<dyn Checker>>,
    assignment_checker: BGAssignmentChecker,
    operator_controller: Arc<OperatorController>,
    ctx: Arc<CoordinatorContext>,
}

impl CheckerController {
    pub fn new(
        operator_controller: Arc<OperatorController>,
        ctx: Arc<CoordinatorContext>,
    ) -> Self {
        let checkers = default_checkers(ctx.clone());
        let assignment_checker = BGAssignmentChecker::new(ctx.clone());
        Self {
            checkers,
            assignment_checker,
            operator_controller,
            ctx,
        }
    }

    /// Run one patrol cycle:
    /// 1. Per-BG patrol with priority short-circuit (suspect BGs first, then full scan)
    /// 2. Worker-dimension BGAssignment check
    pub fn patrol(&self) -> (Vec<BGOperator>, Vec<BGPushCommand>) {
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
        };

        let mut added_ops = Vec::new();

        // Phase 1: Priority-check suspect BGs
        let mut processed_bg_ids = std::collections::HashSet::new();
        for bg in &suspect_bgs {
            processed_bg_ids.insert(bg.bg_id);
            if bg.op_state != BGOpState::Idle {
                continue;
            }
            if let Some(mut op) = self.check_single_bg(bg, &checker_ctx) {
                op.id = self.operator_controller.next_operator_id();
                if self.operator_controller.add_operator(op.clone()) {
                    added_ops.push(op);
                }
            } else {
                self.ctx.bg_manager.clear_suspect(bg.bg_id);
            }
        }

        // Phase 2: Full scan of Idle BGs
        let all_bgs = self.ctx.bg_manager.list_bgs();
        for bg in &all_bgs {
            if processed_bg_ids.contains(&bg.bg_id) {
                continue;
            }
            if bg.op_state != BGOpState::Idle {
                continue;
            }
            if let Some(mut op) = self.check_single_bg(bg, &checker_ctx) {
                op.id = self.operator_controller.next_operator_id();
                if self.operator_controller.add_operator(op.clone()) {
                    added_ops.push(op);
                }
            }
        }

        // Phase 3: Worker-dimension BGAssignment check
        let push_commands = self.assignment_checker.patrol_workers(&checker_ctx);

        (added_ops, push_commands)
    }

    /// Check a single BG with all checkers in priority order (short-circuit).
    fn check_single_bg(
        &self,
        bg: &curvine_common::state::BlockGroupInfo,
        ctx: &CheckerContext<'_>,
    ) -> Option<BGOperator> {
        for checker in &self.checkers {
            if let Some(op) = checker.check_bg(bg, ctx) {
                return Some(op);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> (Arc<CoordinatorContext>, Arc<OperatorController>) {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let op_ctrl = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        (ctx, op_ctrl)
    }

    #[test]
    fn new_creates_controller_with_checkers() {
        let (ctx, op_ctrl) = test_ctx();
        let controller = CheckerController::new(op_ctrl, ctx);
        let (ops, cmds) = controller.patrol();
        assert!(ops.is_empty());
        assert!(cmds.is_empty());
    }

    #[test]
    fn patrol_returns_empty_with_no_bgs() {
        let (ctx, op_ctrl) = test_ctx();
        let controller = CheckerController::new(op_ctrl, ctx);
        let (ops, cmds) = controller.patrol();
        assert!(ops.is_empty());
        assert!(cmds.is_empty());
    }

    #[test]
    fn patrol_is_idempotent() {
        let (ctx, op_ctrl) = test_ctx();
        let controller = CheckerController::new(op_ctrl, ctx);
        let (ops1, _) = controller.patrol();
        let (ops2, _) = controller.patrol();
        assert_eq!(ops1.len(), ops2.len());
    }
}
