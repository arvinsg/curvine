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

use super::{BGPushCommand, CheckResult, CheckerContext};
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::NodeType;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

/// Compares worker-reported BG list with expected assignments;
/// generates push commands for missing or extra BGs.
/// Also stores pending push commands for heartbeat delivery.
pub struct BGAssignmentChecker {
    ctx: Arc<CoordinatorContext>,
    pending_push_commands: DashMap<u32, BGPushCommand>,
}

impl BGAssignmentChecker {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        Self {
            ctx,
            pending_push_commands: DashMap::new(),
        }
    }

    /// Take pending push command for a worker (consumed on heartbeat response).
    pub fn take_pending_command(&self, worker_id: u32) -> Option<BGPushCommand> {
        self.pending_push_commands.remove(&worker_id).map(|e| e.1)
    }

    /// Check if there is a pending push command for a worker.
    pub fn has_pending_command(&self, worker_id: u32) -> bool {
        self.pending_push_commands.contains_key(&worker_id)
    }
}

impl super::Checker for BGAssignmentChecker {
    fn name(&self) -> &str {
        "bg-assignment-checker"
    }

    fn interval_ms(&self) -> u64 {
        self.ctx
            .config_manager
            .get_u64("pd.schedule.bg_check_interval_ms", 10_000)
    }

    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult {
        let mut result = CheckResult::default();

        let workers = ctx
            .node_manager
            .get_nodes_by_type(NodeType::Worker)
            .into_iter()
            .filter(|n| n.state == curvine_common::state::NodeState::Live)
            .collect::<Vec<_>>();

        for worker in &workers {
            let node_id = worker.base.node_id;
            let expected_bgs = ctx.bg_manager.get_bgs_on_worker(node_id);
            let expected_ids: HashSet<u32> = expected_bgs.iter().map(|bg| bg.bg_id).collect();

            let reported_ids: HashSet<u32> = match &worker.payload {
                curvine_common::state::NodePayload::Worker(ref p) => {
                    p.bg_ids.iter().copied().collect()
                }
                _ => continue,
            };

            let missing: Vec<u32> = expected_ids.difference(&reported_ids).copied().collect();
            let extra: Vec<u32> = reported_ids.difference(&expected_ids).copied().collect();

            if missing.is_empty() && extra.is_empty() {
                continue;
            }

            let add_bgs: Vec<_> = missing
                .iter()
                .filter_map(|&bg_id| ctx.bg_manager.get_bg(bg_id))
                .collect();

            let cmd = BGPushCommand {
                worker_id: node_id,
                add_bgs,
                remove_bgs: extra,
            };

            self.pending_push_commands.insert(node_id, cmd.clone());
            result.bg_push_commands.push(cmd);
        }

        result
    }
}
