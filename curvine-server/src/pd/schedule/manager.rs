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
use crate::pd::node::{NodeEvent, NodeEventType, NodeManager};
use crate::pd::pool::PoolManager;
use curvine_common::state::{BlockGroupInfo, NodeType};
use orpc::runtime::RpcRuntime;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::checker_controller::CheckerController;
use super::operator::BGCommands;
use super::operator_controller::OperatorController;
use super::scheduler_controller::SchedulerController;

/// Shared context for all scheduling components.
pub struct ManagerContext {
    pub node_manager: Arc<NodeManager>,
    pub pool_manager: Arc<PoolManager>,
    pub bg_manager: Arc<BGManager>,
    pub config_manager: Arc<ConfigManager>,
    pub operator_controller: Arc<OperatorController>,
    pub runtime: Arc<orpc::runtime::Runtime>,
}

impl ManagerContext {
    /// Pick a lease transfer target from `bg.replica_set`, excluding `leaving` and
    /// preferring the available worker with the fewest current leases (load balance).
    pub fn pick_lease_fallback(
        &self,
        bg: &BlockGroupInfo,
        leaving: u32,
        default_target: u32,
    ) -> u32 {
        let lease_counts = self.bg_manager.get_worker_lease_counts();
        bg.replica_set
            .iter()
            .copied()
            .filter(|&w| w != leaving && self.pool_manager.is_worker_available(w))
            .min_by_key(|w| lease_counts.get(w).copied().unwrap_or(0))
            .unwrap_or(default_target)
    }
}

/// Central orchestrator: manages checker, scheduler, and operator loops.
pub struct Manager {
    ctx: Arc<ManagerContext>,
    checker_controller: Arc<CheckerController>,
    scheduler_controller: Arc<SchedulerController>,
}

impl Manager {
    pub fn new(ctx: Arc<ManagerContext>) -> Self {
        let checker_controller = Arc::new(CheckerController::new(
            ctx.operator_controller.clone(),
            ctx.clone(),
        ));
        let scheduler_controller = Arc::new(SchedulerController::new(ctx.clone()));
        Self {
            ctx,
            checker_controller,
            scheduler_controller,
        }
    }

    pub fn scheduler_controller(&self) -> &SchedulerController {
        &self.scheduler_controller
    }

    /// Start all background tasks, controlled by the given CancellationToken.
    pub fn start(
        self: Arc<Self>,
        mut event_rx: tokio::sync::broadcast::Receiver<NodeEvent>,
        token: CancellationToken,
    ) {
        self.full_reconcile();

        let rt = &self.ctx.runtime;

        let coord = self.clone();
        let t = token.clone();
        rt.spawn(async move { coord.patrol_loop(t).await });

        let coord = self.clone();
        let t = token.clone();
        rt.spawn(async move { coord.operator_loop(t).await });

        self.scheduler_controller.run_all(token.clone());

        let coord = self.clone();
        let t = token.clone();
        rt.spawn(async move { coord.event_loop(&mut event_rx, t).await });
    }

    fn full_reconcile(&self) {
        log::info!("Schedule manager: running initial reconcile");
        if let Err(e) = self.ctx.pool_manager.ensure_default_pools() {
            log::error!("Failed to ensure default pools: {}", e);
        }
        log::info!("Schedule manager: initial reconcile completed");
    }

    async fn patrol_loop(&self, token: CancellationToken) {
        self.checker_controller.patrol();
        loop {
            let interval_ms = self.ctx.config_manager.get_u64(
                crate::pd::config::keys::PD_SCHEDULE_PATROL_INTERVAL_MS,
                crate::pd::config::keys::PD_SCHEDULE_PATROL_INTERVAL_MS_DEFAULT,
            );
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
            }
            self.checker_controller.patrol();
        }
        log::info!("Patrol loop stopped");
    }

    async fn operator_loop(&self, token: CancellationToken) {
        loop {
            let interval_ms = self.ctx.config_manager.get_u64(
                crate::pd::config::keys::PD_SCHEDULE_OPERATOR_TICK_INTERVAL_MS,
                crate::pd::config::keys::PD_SCHEDULE_OPERATOR_TICK_INTERVAL_MS_DEFAULT,
            );
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
            }
            let now = orpc::common::LocalTime::mills();
            self.ctx.operator_controller.tick(now);
            self.ctx.bg_manager.flush_table_epoch_if_dirty();
        }
        log::info!("Operator loop stopped");
    }

    async fn event_loop(
        &self,
        rx: &mut tokio::sync::broadcast::Receiver<NodeEvent>,
        token: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = token.cancelled() => break,
                result = rx.recv() => {
                    match result {
                        Ok(event) => self.handle_event(&event),
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
        }
        log::info!("Event loop stopped");
    }

    fn handle_event(&self, event: &NodeEvent) {
        if event.node_type != NodeType::Worker {
            return;
        }
        match event.event_type {
            NodeEventType::Registered => {}
            NodeEventType::HeartbeatResumed => {
                log::info!("Worker {} resumed heartbeat", event.node_id);
            }
            NodeEventType::Lost => {
                log::info!("Worker {} lost", event.node_id);
                self.ctx.bg_manager.mark_worker_offline(event.node_id);
            }
            NodeEventType::DecommissionStarted => {
                log::info!("Worker {} decommission started", event.node_id);
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
            }
        }
        self.scheduler_controller.on_event(event);
    }

    pub fn dispatch_operators(
        &self,
        worker_id: u32,
        reported_bg_epochs: &std::collections::HashMap<u32, u64>,
    ) -> BGCommands {
        self.ctx
            .operator_controller
            .build_worker_commands(worker_id, reported_bg_epochs)
    }
}
