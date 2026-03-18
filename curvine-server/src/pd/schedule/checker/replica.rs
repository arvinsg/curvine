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
use crate::pd::schedule::operator::{BGOperator, OpStatus, OpStep};
use curvine_common::state::BGState;
use std::sync::Arc;

pub struct ReplicaChecker {
    ctx: Arc<crate::pd::schedule::CoordinatorContext>,
}

impl ReplicaChecker {
    pub fn new(ctx: Arc<crate::pd::schedule::CoordinatorContext>) -> Self {
        Self { ctx }
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
            .get_u32("pd.recovery.max_concurrent", 10);

        let degraded_bgs = ctx.bg_manager.get_bgs_by_state(BGState::Degraded);

        for bg in degraded_bgs {
            if result.bg_operators.len() >= max_concurrent as usize {
                break;
            }

            let available_replicas: Vec<u32> = bg
                .replica_set
                .iter()
                .filter(|w| ctx.pool_manager.is_worker_available(**w))
                .copied()
                .collect();

            if available_replicas.is_empty() {
                log::error!("BG {} all replicas lost!", bg.bg_id);
                continue;
            }

            let table = match ctx.bg_manager.get_table(bg.table_id) {
                Some(t) => t,
                None => continue,
            };

            let needed = table.policy.replicas as usize - available_replicas.len();
            if needed == 0 {
                continue;
            }

            let pool_id = (bg.table_id >> 16) as u16;
            let new_workers = match ctx.pool_manager.select_workers_for_bg(
                pool_id,
                needed as u16,
                table.policy.placement,
                &bg.replica_set,
            ) {
                Ok(w) => w,
                Err(_) => continue,
            };

            if new_workers.is_empty() {
                log::warn!("No available worker for BG {} recovery", bg.bg_id);
                continue;
            }

            let mut steps = Vec::new();
            for worker_id in &new_workers {
                steps.push(OpStep::AddReplica { worker_id: *worker_id });
                steps.push(OpStep::WaitSync { worker_id: *worker_id });
            }

            let op = BGOperator {
                id: 0,
                bg_id: bg.bg_id,
                description: format!("Add {} replicas", new_workers.len()),
                steps,
                current_step: 0,
                status: OpStatus::Pending,
                create_time_ms: orpc::common::LocalTime::mills(),
                priority: 100,
            };
            result.bg_operators.push(op);
        }

        result
    }
}
