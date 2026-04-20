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

use super::scheduler::{default_schedulers, Scheduler};
use super::ManagerContext;
use crate::pd::node::NodeEvent;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Manages all schedulers.
pub struct SchedulerController {
    schedulers: Vec<Arc<dyn Scheduler>>,
    ctx: Arc<ManagerContext>,
}

impl SchedulerController {
    pub fn new(ctx: Arc<ManagerContext>) -> Self {
        let schedulers = default_schedulers(ctx.clone())
            .into_iter()
            .map(|s| -> Arc<dyn Scheduler> { s.into() })
            .collect();
        Self { schedulers, ctx }
    }

    /// Spawn one task per scheduler on the shared runtime.
    pub fn run_all(&self, token: CancellationToken) {
        let rt = &self.ctx.runtime;
        for scheduler in &self.schedulers {
            let scheduler = scheduler.clone();
            let ctx = self.ctx.clone();
            let t = token.clone();
            rt.spawn(async move {
                Self::run_scheduler(scheduler, ctx, t).await;
            });
        }
    }

    async fn run_scheduler(
        scheduler: Arc<dyn Scheduler>,
        ctx: Arc<ManagerContext>,
        token: CancellationToken,
    ) {
        let mut interval = scheduler.min_interval();
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(interval) => {}
            }

            if !scheduler.is_schedule_allowed(&ctx) {
                interval = scheduler.next_interval(interval);
                continue;
            }

            let ops = scheduler.schedule(&ctx);
            if ops.is_empty() {
                interval = scheduler.next_interval(interval);
            } else {
                for mut op in ops {
                    op.id = ctx.operator_controller.next_operator_id();
                    ctx.operator_controller.add_operator(op);
                }
                interval = scheduler.min_interval();
            }
        }
        log::info!("Scheduler '{}' stopped", scheduler.name());
    }

    pub fn on_event(&self, event: &NodeEvent) {
        for scheduler in &self.schedulers {
            scheduler.on_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_controller_constructs() {
        let ctx = crate::pd::schedule::checker::tests_common::test_context(
            std::collections::HashMap::new(),
        );
        let _controller = SchedulerController::new(ctx);
    }
}
