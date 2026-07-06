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

use super::checker::{default_checkers, Checker};
use super::operator::BGOperator;
use super::operator_controller::OperatorController;
use super::CoordinatorContext;
use curvine_common::state::{BGOpState, NodeState, NodeType};
use std::sync::Arc;

/// Runs all checkers in priority order during patrol.
///
/// Two phases per patrol cycle:
/// 1. Per-BG scan: checkers in priority order, first hit wins (short-circuit)
/// 2. Per-worker scan: checkers in priority order per worker, first hit wins (short-circuit)
pub struct CheckerController {
    checkers: Vec<Box<dyn Checker>>,
    operator_controller: Arc<OperatorController>,
    ctx: Arc<CoordinatorContext>,
}

impl CheckerController {
    pub fn new(operator_controller: Arc<OperatorController>, ctx: Arc<CoordinatorContext>) -> Self {
        let checkers = default_checkers();
        Self {
            checkers,
            operator_controller,
            ctx,
        }
    }

    /// Run one patrol cycle. Returns operators added during this cycle.
    pub fn patrol(&self) -> Vec<BGOperator> {
        if !self
            .ctx
            .config_manager
            .get_bool(crate::pd::config::keys::PD_SCHEDULE_CHECKER_ENABLED)
        {
            return vec![];
        }

        let mut added_ops = Vec::new();
        let mut rejected = 0u32;

        // Phase 1: Per-BG scan
        let all_bgs = self
            .ctx
            .bgtable_manager
            .bg()
            .list_bgs(curvine_common::state::BGKind::Hash, None);
        for bg in &all_bgs {
            if bg.op_state != BGOpState::Idle {
                continue;
            }
            for checker in &self.checkers {
                if let Some(mut op) = checker.check_bg(bg, &self.ctx) {
                    op.id = self.operator_controller.next_operator_id();
                    if self.operator_controller.add_operator(op.clone()) {
                        added_ops.push(op);
                    } else {
                        rejected += 1;
                        log::warn!(
                            "checker '{}': operator for bg {} rejected by operator_controller",
                            checker.name(),
                            bg.bg_id
                        );
                    }
                    break;
                }
            }
        }

        // Phase 2: Per-worker scan
        let workers = self
            .ctx
            .node_manager
            .get_nodes_by_type(NodeType::Worker)
            .into_iter()
            .filter(|n| n.state == NodeState::Live)
            .collect::<Vec<_>>();

        for worker in &workers {
            for checker in &self.checkers {
                if let Some(mut op) = checker.check_worker(worker, &self.ctx) {
                    op.id = self.operator_controller.next_operator_id();
                    if self.operator_controller.add_operator(op.clone()) {
                        added_ops.push(op);
                    } else {
                        rejected += 1;
                        log::warn!(
                            "checker '{}': operator for worker {} rejected by operator_controller",
                            checker.name(),
                            worker.base.node_id
                        );
                    }
                    break;
                }
            }
        }

        if rejected > 0 {
            log::warn!(
                "checker patrol: {} operator(s) rejected by operator_controller",
                rejected
            );
        }

        added_ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> Arc<CoordinatorContext> {
        crate::pd::coordinator::checker::tests_common::test_context(std::collections::HashMap::new())
    }

    #[test]
    fn patrol_returns_empty_with_no_bgs() {
        let ctx = test_ctx();
        let controller = CheckerController::new(ctx.operator_controller.clone(), ctx);
        let ops = controller.patrol();
        assert!(ops.is_empty());
    }

    #[test]
    fn patrol_is_idempotent() {
        let ctx = test_ctx();
        let controller = CheckerController::new(ctx.operator_controller.clone(), ctx);
        let ops1 = controller.patrol();
        let ops2 = controller.patrol();
        assert_eq!(ops1.len(), ops2.len());
    }
}
