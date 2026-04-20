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

use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BlockGroupInfo, NodeInfo, NodePayload};
use std::collections::HashSet;

pub struct BGAssignmentChecker;

impl super::Checker for BGAssignmentChecker {
    fn name(&self) -> &str {
        "bg-assignment-checker"
    }

    fn check_bg(&self, _bg: &BlockGroupInfo, _ctx: &ManagerContext) -> Option<BGOperator> {
        None
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::BG_ASSIGNMENT
    }

    fn check_worker(&self, worker: &NodeInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        let node_id = worker.base.node_id;
        let expected_bgs = ctx.bg_manager.get_bgs_on_worker(node_id);
        let expected_ids: HashSet<u32> = expected_bgs.iter().map(|bg| bg.bg_id).collect();

        let reported_ids: HashSet<u32> = match &worker.payload {
            NodePayload::Worker(ref p) => p.bg_epochs.keys().copied().collect(),
            _ => return None,
        };

        // Missing: PD expects worker to have this BG but worker doesn't report it.
        // Return the first missing BG as an operator (short-circuit per worker).
        for &bg_id in expected_ids.difference(&reported_ids) {
            if let Some(bg) = ctx.bg_manager.get_bg(bg_id) {
                return Some(
                    OperatorBuilder::new(
                        OperatorKind::Repair,
                        bg_id,
                        format!("Assignment sync: push BG {} to worker {}", bg_id, node_id),
                    )
                    .bg_epoch(bg.bg_epoch)
                    .priority(OpPriority::ASSIGNMENT_SYNC)
                    .add_replica(node_id)
                    .build(),
                );
            }
        }

        // Extra: worker reports a BG that PD doesn't expect it to have.
        // Record for direct removal via heartbeat response (not via operator).
        for &bg_id in reported_ids.difference(&expected_ids) {
            ctx.bg_manager.add_extra_remove_bg(node_id, bg_id);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::Checker;

    #[test]
    fn name_and_priority() {
        let checker = BGAssignmentChecker;
        assert_eq!(checker.name(), "bg-assignment-checker");
        assert_eq!(checker.priority(), 40);
    }
}
