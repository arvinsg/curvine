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

use crate::pd::bg::{BGManager, DirtyReason};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::BGUpdateEntry;
use crate::pd::journal::{self, PdEntry};
use crate::pd::node::{NodeEvent, NodeEventType, NodeManager};
use crate::pd::pool::PoolManager;
use curvine_common::raft::RoleState;
use curvine_common::state::{BGLease, NodeType};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::sync::StateCtl;
use std::sync::Arc;
use std::time::Duration;

use super::bg_table_scheduler::BGTableScheduler;
use super::checker::BGPushCommand;
use super::checker_controller::CheckerController;
use super::operator::BGCommands;
use super::operator_controller::OperatorController;

/// Trait for checking PD leader status.
/// Real implementation wires to Raft; tests can mock.
pub trait LeaderChecker: Send + Sync {
    fn is_leader(&self) -> bool;
}

/// Raft-based leader checker: wires to RoleMonitor via StateCtl.
pub struct RaftLeaderChecker {
    role_ctl: StateCtl,
}

impl RaftLeaderChecker {
    pub fn new(role_ctl: StateCtl) -> Self {
        Self { role_ctl }
    }
}

impl LeaderChecker for RaftLeaderChecker {
    fn is_leader(&self) -> bool {
        let state: RoleState = self.role_ctl.state();
        state == RoleState::Leader
    }
}

/// Always-leader checker for tests.
#[cfg(test)]
pub struct AlwaysLeader;

#[cfg(test)]
impl LeaderChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

/// Shared context for schedule (checkers, scheduler, coordinator)
pub struct CoordinatorContext {
    pub node_manager: Arc<NodeManager>,
    pub pool_manager: Arc<PoolManager>,
    pub bg_manager: Arc<BGManager>,
    pub config_manager: Arc<ConfigManager>,
    pub journal_client: Arc<journal::Client>,
    pub leader_checker: Arc<dyn LeaderChecker>,
}

impl CoordinatorContext {
    pub fn is_leader(&self) -> bool {
        self.leader_checker.is_leader()
    }
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

/// Schedule coordinator: the single owner of all event-driven scheduling logic.
/// Runs checkers, operator controller, BGTable scheduler, and handles all
/// NodeEvent side-effects (pool liveness, BG flag refresh, rebuild scheduling).
pub struct Coordinator {
    ctx: Arc<CoordinatorContext>,
    checker_controller: std::sync::Mutex<CheckerController>,
    operator_controller: Arc<OperatorController>,
    bg_table_scheduler: Arc<BGTableScheduler>,
    pending_push_commands: dashmap::DashMap<u32, BGPushCommand>,
}

impl Coordinator {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let checker_controller = std::sync::Mutex::new(CheckerController::new(operator_controller.clone(), ctx.clone()));
        let bg_table_scheduler = Arc::new(BGTableScheduler::new(ctx.clone()));
        Self {
            ctx,
            checker_controller,
            operator_controller,
            bg_table_scheduler,
            pending_push_commands: dashmap::DashMap::new(),
        }
    }

    /// Accessor for OperatorController (used by CompositeDecommissionChecker).
    pub fn operator_controller(&self) -> Arc<OperatorController> {
        self.operator_controller.clone()
    }

    /// Start background loops. Call once after creation.
    pub fn run(self: Arc<Self>, mut event_rx: tokio::sync::broadcast::Receiver<NodeEvent>) {
        // Full reconcile at startup (leader check inside)
        if self.ctx.is_leader() {
            self.full_reconcile();
        }

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

    /// Full reconcile: startup catch-all to ensure consistent state.
    fn full_reconcile(&self) {
        log::info!("Coordinator: running full reconcile");

        // 1. Refresh all BG flags from current cluster state
        self.ctx.bg_manager.refresh_all_bg_flags();

        // 2. Run one patrol cycle to generate initial operators
        let (_ops, push_commands) = self.checker_controller.lock().unwrap().patrol();
        for cmd in push_commands {
            self.pending_push_commands.insert(cmd.worker_id, cmd);
        }

        // 3. Ensure BGTables exist for all active pools
        self.bg_table_scheduler.check_table_initialization();

        log::info!("Coordinator: full reconcile completed");
    }

    // ========== Event handling ==========

    async fn event_loop(&self, rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>) {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    self.handle_event(&event);
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

    /// Central event handler: all worker event side-effects are handled here.
    /// This is the single source of truth for event-driven scheduling.
    fn handle_event(&self, event: &NodeEvent) {
        if event.node_type != NodeType::Worker {
            return;
        }
        match event.event_type {
            NodeEventType::Registered => {
                // BGTableScheduler handles rebuild scheduling via on_event below
            }
            NodeEventType::HeartbeatResumed => {
                log::info!("Worker {} resumed heartbeat", event.node_id);
                self.ctx.pool_manager.mark_allocatable(event.node_id);
                self.ctx.bg_manager.refresh_bg_flags_for_worker(
                    event.node_id,
                    DirtyReason::NodeRecovered,
                );
            }
            NodeEventType::Lost => {
                log::info!("Worker {} lost", event.node_id);
                self.ctx.pool_manager.mark_unallocatable(event.node_id);
                self.ctx
                    .bg_manager
                    .refresh_bg_flags_for_worker(event.node_id, DirtyReason::NodeLost);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::DecommissionStarted => {
                log::info!("Worker {} decommission started", event.node_id);
                self.ctx.pool_manager.mark_unallocatable(event.node_id);
                self.ctx.bg_manager.refresh_bg_flags_for_worker(
                    event.node_id,
                    DirtyReason::NodeDecommission,
                );
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::Offline => {
                log::info!("Worker {} offline", event.node_id);
                if let Err(e) = self.ctx.pool_manager.remove_worker_from_pools(event.node_id) {
                    log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                }
                self.ctx
                    .bg_manager
                    .refresh_bg_flags_for_worker(event.node_id, DirtyReason::NodeOffline);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
                // BGTableScheduler handles rebuild scheduling via on_event below
            }
            NodeEventType::DecommissionFinished => {
                log::info!("Worker {} decommission finished", event.node_id);
                if let Err(e) = self.ctx.pool_manager.remove_worker_from_pools(event.node_id) {
                    log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                }
                self.ctx
                    .bg_manager
                    .refresh_bg_flags_for_worker(event.node_id, DirtyReason::NodeOffline);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
                // BGTableScheduler handles rebuild scheduling via on_event below
            }
            _ => {}
        }
        // Forward to BGTableScheduler for rebuild scheduling (Registered, Offline, DecommissionFinished)
        use super::checker::Scheduler;
        self.bg_table_scheduler.on_event(event);
    }

    // ========== Background loops ==========

    async fn patrol_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            if !self.ctx.is_leader() {
                continue;
            }
            let (_ops, push_commands) = self.checker_controller.lock().unwrap().patrol();
            for cmd in push_commands {
                self.pending_push_commands.insert(cmd.worker_id, cmd);
            }
        }
    }

    async fn operator_check_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            if !self.ctx.is_leader() {
                continue;
            }
            let now = orpc::common::LocalTime::mills();
            self.operator_controller.check_progress(now);
            self.operator_controller.dispatch_next();
        }
    }

    async fn rebuild_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(5000)).await;
            if !self.ctx.is_leader() {
                continue;
            }
            self.bg_table_scheduler.check_and_rebuild().await;
        }
    }

    // ========== Heartbeat dispatch ==========

    pub fn dispatch_operators(&self, worker_id: u32) -> BGCommands {
        let mut commands = self.operator_controller.dispatch_to_worker(worker_id);

        if let Some((_, push_cmd)) = self.pending_push_commands.remove(&worker_id) {
            commands.add_bgs.extend(push_cmd.add_bgs);
            commands.remove_bgs.extend(push_cmd.remove_bgs);
        }
        commands
    }
}
