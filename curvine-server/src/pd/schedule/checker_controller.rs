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
    operator_controller: Arc<OperatorController>,
    ctx: Arc<CoordinatorContext>,
}

impl CheckerController {
    pub fn new(
        operator_controller: Arc<OperatorController>,
        ctx: Arc<CoordinatorContext>,
    ) -> Self {
        let checkers = default_checkers(ctx.clone());
        Self {
            checkers,
            operator_controller,
            ctx,
        }
    }

    pub fn patrol(&self) -> (Vec<BGOperator>, Vec<BGPushCommand>) {
        let checker_ctx = CheckerContext {
            pool_manager: self.ctx.pool_manager.as_ref(),
            bg_manager: self.ctx.bg_manager.as_ref(),
            node_manager: self.ctx.node_manager.as_ref(),
            config_manager: self.ctx.config_manager.as_ref(),
        };

        let mut added_ops = Vec::new();
        let mut push_commands = Vec::new();
        for checker in &self.checkers {
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
