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

use crate::pd::bg::BGManager;
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::BGUpdateEntry;
use crate::pd::journal::{self, PdEntry};
use crate::pd::node::{NodeEvent, NodeManager};
use crate::pd::pool::PoolManager;
use curvine_common::state::BGLease;
use curvine_common::FsResult;
use orpc::common::LocalTime;
use std::sync::Arc;
use std::time::Duration;

use super::checker::BGPushCommand;
use super::checker_controller::CheckerController;
use super::operator::{BGCommands, RebuildReason};
use super::operator_controller::OperatorController;
use super::rebuild_scheduler::RebuildScheduler;

/// Shared context for schedule (checkers, scheduler, coordinator)
pub struct CoordinatorContext {
    pub node_manager: Arc<NodeManager>,
    pub pool_manager: Arc<PoolManager>,
    pub bg_manager: Arc<BGManager>,
    pub config_manager: Arc<ConfigManager>,
    pub journal_client: Arc<journal::Client>,
}

impl CoordinatorContext {
    /// Propose BG state change (e.g. Degraded). Caller should be PD leader.
    pub fn propose_bg_state(
        &self,
        bg_id: u32,
        state: curvine_common::state::BGState,
    ) -> FsResult<()> {
        let entry = BGUpdateEntry {
            op_ms: LocalTime::mills(),
            bg_id,
            state: Some(state),
            replica_set: None,
            lease_owner: None,
        };
        self.journal_client.propose(PdEntry::UpdateBG(entry))
    }

    /// Propose BG lease owner change (e.g. transfer lease to another replica). Caller should be PD leader.
    pub fn propose_bg_lease(&self, bg_id: u32, lease: BGLease) -> FsResult<()> {
        let entry = BGUpdateEntry {
            op_ms: LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: None,
            lease_owner: Some(lease),
        };
        self.journal_client.propose(PdEntry::UpdateBG(entry))
    }
}

/// Schedule coordinator: runs checkers, operator controller, rebuild scheduler.
/// Also listens for NodeEvent and dispatches to schedulers.
pub struct Coordinator {
    ctx: Arc<CoordinatorContext>,
    checker_controller: CheckerController,
    operator_controller: Arc<OperatorController>,
    rebuild_scheduler: Arc<RebuildScheduler>,
    pending_push_commands: dashmap::DashMap<u32, BGPushCommand>,
}

impl Coordinator {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let checker_controller = CheckerController::new(operator_controller.clone(), ctx.clone());
        let rebuild_scheduler = Arc::new(RebuildScheduler::new(ctx.clone()));
        Self {
            ctx,
            checker_controller,
            operator_controller,
            rebuild_scheduler,
            pending_push_commands: dashmap::DashMap::new(),
        }
    }

    /// Start background loops. Call once after creation.
    pub fn run(self: Arc<Self>, mut event_rx: tokio::sync::broadcast::Receiver<NodeEvent>) {
        let coord = self.clone();
        tokio::spawn(async move {
            coord.patrol_loop().await;
        });
        let coord = self.clone();
        tokio::spawn(async move {
            coord.operator_check_loop().await;
        });
        let coord = self.clone();
        tokio::spawn(async move {
            coord.rebuild_loop().await;
        });
        let coord = self.clone();
        tokio::spawn(async move {
            coord.event_loop(&mut event_rx).await;
        });
    }

    async fn event_loop(&self, rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>) {
        use super::checker::Scheduler;
        loop {
            match rx.recv().await {
                Ok(event) => {
                    self.rebuild_scheduler.on_event(&event);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("Coordinator event loop lagged {} events", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    log::info!("Coordinator event channel closed, exiting event loop");
                    break;
                }
            }
        }
    }

    async fn patrol_loop(&self) {
        loop {
            let interval = self
                .ctx
                .config_manager
                .get_u64("pd.schedule.patrol_interval_ms", 10_000);
            tokio::time::sleep(Duration::from_millis(interval)).await;
            let (_ops, push_commands) = self.checker_controller.patrol();
            for cmd in push_commands {
                self.pending_push_commands.insert(cmd.worker_id, cmd);
            }
        }
    }

    async fn operator_check_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let now = orpc::common::LocalTime::mills();
            self.operator_controller.check_progress(now);
            self.operator_controller.dispatch_next();
        }
    }

    async fn rebuild_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(5000)).await;
            self.rebuild_scheduler.check_and_rebuild().await;
        }
    }

    /// Dispatch operator commands + pending push commands for a worker.
    pub fn dispatch_operators(&self, worker_id: u32) -> BGCommands {
        let mut commands = self.operator_controller.dispatch_to_worker(worker_id);

        if let Some((_, push_cmd)) = self.pending_push_commands.remove(&worker_id) {
            commands.add_bgs.extend(push_cmd.add_bgs);
            commands.remove_bgs.extend(push_cmd.remove_bgs);
        }
        commands
    }

    pub fn on_worker_joined(&self, node_id: u32, pool_ids: Vec<u16>) {
        self.rebuild_scheduler.schedule_rebuild(
            pool_ids,
            RebuildReason::NodeJoined {
                node_ids: vec![node_id],
            },
        );
    }

    pub fn on_worker_removed(&self, node_id: u32, pool_ids: Vec<u16>) {
        self.rebuild_scheduler.schedule_rebuild(
            pool_ids,
            RebuildReason::NodeRemoved {
                node_ids: vec![node_id],
            },
        );
    }
}
