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
use crate::pd::node::{NodeEvent, NodeEventType, NodeManager};
use crate::pd::pool::PoolManager;
use curvine_common::raft::RoleState;
use curvine_common::state::{BGLease, NodeType};
use curvine_common::FsResult;
use orpc::common::LocalTime;
use orpc::sync::StateCtl;
use std::sync::Arc;
use std::time::Duration;

use super::checker::BGPushCommand;
use super::checker_controller::CheckerController;
use super::operator::BGCommands;
use super::operator_controller::OperatorController;
use super::scheduler_controller::SchedulerController;

/// Trait for checking PD leader status.
pub trait LeaderChecker: Send + Sync {
    fn is_leader(&self) -> bool;
}

/// Raft-based leader checker.
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

#[cfg(test)]
pub struct AlwaysLeader;

#[cfg(test)]
impl LeaderChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

/// Shared context for all scheduling components.
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
            bg_epoch: None,
        };
        self.journal_client.propose(PdEntry::UpdateBG(entry))
    }

    pub fn propose_bg_lease(&self, bg_id: u32, lease: BGLease) -> FsResult<()> {
        let entry = BGUpdateEntry {
            op_ms: LocalTime::mills(),
            bg_id,
            state: None,
            replica_set: None,
            lease_owner: Some(lease),
            bg_epoch: None,
        };
        self.journal_client.propose(PdEntry::UpdateBG(entry))
    }
}

/// Central orchestrator: runs checkers, schedulers,
pub struct Coordinator {
    ctx: Arc<CoordinatorContext>,
    checker_controller: CheckerController,
    scheduler_controller: SchedulerController,
    operator_controller: Arc<OperatorController>,
    pending_push_commands: dashmap::DashMap<u32, BGPushCommand>,
}

impl Coordinator {
    pub fn new(ctx: Arc<CoordinatorContext>) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let checker_controller = CheckerController::new(operator_controller.clone(), ctx.clone());
        let scheduler_controller =
            SchedulerController::new(ctx.clone(), operator_controller.clone());
        Self {
            ctx,
            checker_controller,
            scheduler_controller,
            operator_controller,
            pending_push_commands: dashmap::DashMap::new(),
        }
    }

    pub fn operator_controller(&self) -> Arc<OperatorController> {
        self.operator_controller.clone()
    }

    pub fn scheduler_controller(&self) -> &SchedulerController {
        &self.scheduler_controller
    }

    /// Start 3 background loops. Call once after creation.
    pub fn run(self: Arc<Self>, mut event_rx: tokio::sync::broadcast::Receiver<NodeEvent>) {
        if self.ctx.is_leader() {
            self.full_reconcile();
        }

        let coord = self.clone();
        tokio::spawn(async move {
            coord.patrol_loop().await;
        });
        let coord = self.clone();
        tokio::spawn(async move {
            coord.schedule_loop().await;
        });
        let coord = self.clone();
        tokio::spawn(async move {
            coord.event_loop(&mut event_rx).await;
        });
    }

    fn full_reconcile(&self) {
        log::info!("Coordinator: running full reconcile");

        // Ensure default pools exist (via Raft, leader-only).
        if let Err(e) = self.ctx.pool_manager.ensure_default_pools() {
            log::error!("Failed to ensure default pools: {}", e);
        }

        let (_ops, push_commands) = self.checker_controller.patrol();
        for cmd in push_commands {
            self.pending_push_commands.insert(cmd.worker_id, cmd);
        }
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
                    log::info!("Coordinator event channel closed");
                    break;
                }
            }
        }
    }

    fn handle_event(&self, event: &NodeEvent) {
        if event.node_type != NodeType::Worker {
            return;
        }
        match event.event_type {
            NodeEventType::Registered => {}
            NodeEventType::HeartbeatResumed => {
                log::info!("Worker {} resumed heartbeat", event.node_id);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::Lost => {
                log::info!("Worker {} lost", event.node_id);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::DecommissionStarted => {
                log::info!("Worker {} decommission started", event.node_id);
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::Offline => {
                log::info!("Worker {} offline", event.node_id);
                if let Err(e) = self
                    .ctx
                    .pool_manager
                    .remove_worker_from_pools(event.node_id)
                {
                    log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                }
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            NodeEventType::DecommissionFinished => {
                log::info!("Worker {} decommission finished", event.node_id);
                if let Err(e) = self
                    .ctx
                    .pool_manager
                    .remove_worker_from_pools(event.node_id)
                {
                    log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                }
                self.ctx.bg_manager.mark_worker_bgs_suspect(event.node_id);
            }
            _ => {}
        }
        // Forward to scheduler controller (BGTableScheduler handles rebuild scheduling)
        self.scheduler_controller.on_event(event);
    }

    // ========== Background loops ==========

    /// Patrol loop: runs checkers for correctness (every 1s).
    async fn patrol_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            if !self.ctx.is_leader() {
                continue;
            }
            let (_ops, push_commands) = self.checker_controller.patrol();
            for cmd in push_commands {
                self.pending_push_commands.insert(cmd.worker_id, cmd);
            }
            self.check_decommission_complete();
        }
    }

    /// Schedule loop: runs schedulers for optimization + operator lifecycle (every 100ms).
    async fn schedule_loop(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !self.ctx.is_leader() {
                continue;
            }

            // Run schedulers (balance, bg_table, stats)
            let ops = self.scheduler_controller.schedule_tick();
            for mut op in ops {
                op.id = self.operator_controller.next_operator_id();
                self.operator_controller.add_operator(op);
            }

            // Operator lifecycle: check progress + dispatch next
            let now = orpc::common::LocalTime::mills();
            self.operator_controller.check_progress(now);
            self.operator_controller.dispatch_next();
        }
    }

    fn check_decommission_complete(&self) {
        let decommission_nodes = self
            .ctx
            .node_manager
            .get_nodes_by_state(curvine_common::state::NodeState::Decommission);
        for node in decommission_nodes {
            let node_id = node.base.node_id;
            let has_bgs = !self.ctx.bg_manager.get_bgs_on_worker(node_id).is_empty();
            let has_ops = self
                .operator_controller
                .has_running_operators_for_node(node_id);
            if !has_bgs && !has_ops {
                log::info!(
                    "Node {} decommission complete (no BGs, no operators), deleting",
                    node_id
                );
                if let Err(e) = self.ctx.node_manager.finish_decommission(node_id) {
                    log::error!("Failed to finish decommission for node {}: {}", node_id, e);
                }
            }
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
