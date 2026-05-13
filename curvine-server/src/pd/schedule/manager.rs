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
use curvine_common::state::{BlockGroupInfo, NodePayload, NodeState, NodeType};
use orpc::runtime::RpcRuntime;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::checker_controller::CheckerController;
use super::operator::BGCommands;
use super::operator_controller::OperatorController;
use super::scheduler_controller::SchedulerController;
use super::ScheduleEvent;

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
    /// preferring the available worker with the fewest current leases.
    pub fn pick_lease_fallback(
        &self,
        bg: &BlockGroupInfo,
        leaving: u32,
        default_target: u32,
    ) -> u32 {
        self.pick_lease_fallback_excluding(bg, &[leaving], default_target)
    }

    /// Pick a lease transfer target from `bg.replica_set`, excluding every
    /// worker in `excluded`.
    pub fn pick_lease_fallback_excluding(
        &self,
        bg: &BlockGroupInfo,
        excluded: &[u32],
        default_target: u32,
    ) -> u32 {
        let lease_counts = self.bg_manager.get_worker_lease_counts();
        bg.replica_set
            .iter()
            .copied()
            .filter(|w| !excluded.contains(w) && self.pool_manager.is_worker_available(*w))
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
        log::info!("Schedule manager: running leader full reconcile");
        if let Err(e) = self.ctx.pool_manager.ensure_default_pools() {
            log::error!("Failed to ensure default pools: {}", e);
        }
        self.reconcile_worker_pool_membership();
        self.reconcile_observed_replica_states();
        self.scheduler_controller.on_leader_start();
        log::info!("Schedule manager: leader full reconcile completed");
    }

    fn reconcile_worker_pool_membership(&self) {
        let workers = self.ctx.node_manager.get_nodes_by_type(NodeType::Worker);
        match self
            .ctx
            .pool_manager
            .reconcile_worker_pool_membership(&workers)
        {
            Ok(result) => log::info!(
                "Schedule manager: worker pool reconcile done, workers={}, changed_pool_ids={:?}",
                workers.len(),
                result.changed_pool_ids
            ),
            Err(e) => log::warn!(
                "Schedule manager: worker pool reconcile failed, workers={}, err={}",
                workers.len(),
                e
            ),
        }
    }

    fn reconcile_observed_replica_states(&self) {
        let workers = self.ctx.node_manager.get_nodes_by_type(NodeType::Worker);
        let mut worker_states = Vec::new();
        let mut lost_workers = 0usize;
        let mut offline_workers = 0usize;

        for node in workers {
            match node.state {
                NodeState::Lost => {
                    lost_workers += 1;
                    worker_states
                        .push((node.base.node_id, curvine_common::state::ReplicaState::Lost));
                }
                NodeState::Offline | NodeState::Decommission | NodeState::Blacklist => {
                    offline_workers += 1;
                    worker_states.push((
                        node.base.node_id,
                        curvine_common::state::ReplicaState::Offline,
                    ));
                }
                // Deliberately do not seed Live/Starting replicas as Active here.
                // Scheduler/operator view remains conservative after failover;
                // worker bg_reports and checker patrol converge it later.
                NodeState::Starting | NodeState::Live => {}
            }
        }

        match self.ctx.bg_manager.mark_workers_replicas(&worker_states) {
            Ok(changed) => log::info!(
                "Schedule manager: observed replica reconcile done, lost_workers={}, offline_workers={}, changed_replicas={}",
                lost_workers,
                offline_workers,
                changed
            ),
            Err(e) => log::warn!(
                "Schedule manager: observed replica reconcile failed, lost_workers={}, offline_workers={}, err={}",
                lost_workers,
                offline_workers,
                e
            ),
        }
    }

    async fn patrol_loop(&self, token: CancellationToken) {
        self.checker_controller.patrol();
        loop {
            let interval_ms = self
                .ctx
                .config_manager
                .get_u64(crate::pd::config::keys::PD_SCHEDULE_PATROL_INTERVAL_MS);
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
            let interval_ms = self
                .ctx
                .config_manager
                .get_u64(crate::pd::config::keys::PD_SCHEDULE_OPERATOR_TICK_INTERVAL_MS);
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
            }
            let now = orpc::common::LocalTime::mills();
            self.ctx.operator_controller.tick(now);
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

        let schedule_event = match event.event_type {
            NodeEventType::Registered => self.handle_worker_registered_event(event),
            NodeEventType::HeartbeatResumed => {
                if self.fence_worker_event(event).is_none() {
                    None
                } else {
                    log::info!("Worker {} resumed heartbeat", event.node_id);
                    Some(ScheduleEvent::WorkerHeartbeatResumed {
                        worker_id: event.node_id,
                        node_epoch: event.epoch,
                        event_time_ms: event.event_time_ms,
                    })
                }
            }
            NodeEventType::Lost => {
                if self.fence_worker_event(event).is_none() {
                    None
                } else {
                    log::info!("Worker {} lost", event.node_id);
                    if let Err(e) = self.ctx.bg_manager.mark_replicas_lost(event.node_id) {
                        log::error!("mark_replicas_lost {} failed: {}", event.node_id, e);
                    }
                    Some(ScheduleEvent::WorkerLost {
                        worker_id: event.node_id,
                        node_epoch: event.epoch,
                        event_time_ms: event.event_time_ms,
                    })
                }
            }
            NodeEventType::DecommissionStarted => {
                if self.fence_worker_event(event).is_none() {
                    None
                } else {
                    log::info!("Worker {} decommission started", event.node_id);
                    if let Err(e) = self
                        .ctx
                        .pool_manager
                        .remove_worker_from_pools(event.node_id)
                    {
                        log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                    }
                    Some(ScheduleEvent::WorkerDecommissionStarted {
                        worker_id: event.node_id,
                        node_epoch: event.epoch,
                        event_time_ms: event.event_time_ms,
                    })
                }
            }
            NodeEventType::Offline => {
                if self.fence_worker_event(event).is_none() {
                    None
                } else {
                    log::info!("Worker {} offline", event.node_id);
                    if let Err(e) = self.ctx.bg_manager.mark_replicas_offline(event.node_id) {
                        log::error!("mark_replicas_offline {} failed: {}", event.node_id, e);
                    }
                    if let Err(e) = self
                        .ctx
                        .pool_manager
                        .remove_worker_from_pools(event.node_id)
                    {
                        log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                    }
                    Some(ScheduleEvent::WorkerOffline {
                        worker_id: event.node_id,
                        node_epoch: event.epoch,
                        event_time_ms: event.event_time_ms,
                    })
                }
            }
            NodeEventType::DecommissionFinished => {
                if !self.fence_deleted_worker_event(event) {
                    None
                } else {
                    log::info!("Worker {} decommission finished", event.node_id);
                    if let Err(e) = self
                        .ctx
                        .pool_manager
                        .remove_worker_from_pools(event.node_id)
                    {
                        log::error!("remove_worker_from_pools {} failed: {}", event.node_id, e);
                    }
                    Some(ScheduleEvent::WorkerDecommissionFinished {
                        worker_id: event.node_id,
                        node_epoch: event.epoch,
                        event_time_ms: event.event_time_ms,
                    })
                }
            }
        };

        if let Some(event) = schedule_event {
            self.scheduler_controller.on_event(&event);
        }
    }

    fn fence_deleted_worker_event(&self, event: &NodeEvent) -> bool {
        if event.node_type != NodeType::Worker {
            log::warn!(
                "drop deleted-worker event {:?} for non-worker node_id={}, node_type={:?}",
                event.event_type,
                event.node_id,
                event.node_type
            );
            return false;
        }
        match self.ctx.node_manager.get_node(event.node_id) {
            None => true,
            Some(current) if current.epoch > event.epoch => {
                log::warn!(
                    "drop stale deleted-worker event {:?} node_id={}, event_epoch={}, current_epoch={}, current_state={:?}",
                    event.event_type,
                    event.node_id,
                    event.epoch,
                    current.epoch,
                    current.state
                );
                false
            }
            Some(current) => {
                log::warn!(
                    "drop deleted-worker event {:?} because node still exists node_id={}, event_epoch={}, current_epoch={}, current_state={:?}",
                    event.event_type,
                    event.node_id,
                    event.epoch,
                    current.epoch,
                    current.state
                );
                false
            }
        }
    }

    fn fence_worker_event(&self, event: &NodeEvent) -> Option<curvine_common::state::NodeInfo> {
        let Some(node) = self.ctx.node_manager.get_node(event.node_id) else {
            log::warn!(
                "drop stale worker event {:?} for missing node {} epoch {}",
                event.event_type,
                event.node_id,
                event.epoch
            );
            return None;
        };
        if node.base.node_type != NodeType::Worker || node.epoch != event.epoch {
            log::warn!(
                "drop stale worker event {:?} node_id={}, event_epoch={}, current_epoch={}, node_type={:?}",
                event.event_type,
                event.node_id,
                event.epoch,
                node.epoch,
                node.base.node_type
            );
            return None;
        }
        if let Some(expected_state) = event.new_state {
            if node.state != expected_state {
                log::warn!(
                    "drop stale worker event {:?} node_id={}, epoch={}, expected_state={:?}, current_state={:?}",
                    event.event_type,
                    event.node_id,
                    event.epoch,
                    expected_state,
                    node.state
                );
                return None;
            }
        }
        Some(node)
    }

    fn handle_worker_registered_event(&self, event: &NodeEvent) -> Option<ScheduleEvent> {
        let node = self.fence_worker_event(event)?;
        let NodePayload::Worker(payload) = &node.payload else {
            log::warn!(
                "drop registered worker event for node {} because payload is not Worker",
                event.node_id
            );
            return None;
        };
        let result = match self
            .ctx
            .pool_manager
            .assign_worker_to_pools(event.node_id, &payload.storage_specs)
        {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "assign_worker_to_pools {} failed while handling Registered event: {}",
                    event.node_id,
                    e
                );
                return None;
            }
        };
        if result.changed_pool_ids.is_empty() {
            log::info!(
                "Worker {} registered, target pools {:?}, no pool membership changed",
                event.node_id,
                result.target_pool_ids
            );
            return None;
        }
        Some(ScheduleEvent::WorkerJoinedPools {
            worker_id: event.node_id,
            node_epoch: event.epoch,
            target_pool_ids: result.target_pool_ids,
            changed_pool_ids: result.changed_pool_ids,
            event_time_ms: event.event_time_ms,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::tests_common::Fixture;

    fn worker_event(node_id: u32, epoch: u64, new_state: Option<NodeState>) -> NodeEvent {
        NodeEvent {
            event_type: NodeEventType::Lost,
            node_id,
            node_type: NodeType::Worker,
            old_state: None,
            new_state,
            epoch,
            event_time_ms: 12345,
        }
    }

    #[test]
    fn fence_worker_event_accepts_current_worker_event() {
        let f = Fixture::new();
        f.add_worker(100, crate::pd::pool::POOL_ID_SSD, &[]);
        let manager = Manager::new(f.ctx.clone());
        let event = worker_event(100, 0, Some(NodeState::Live));

        let fenced = manager.fence_worker_event(&event);

        assert!(fenced.is_some());
    }

    #[test]
    fn fence_worker_event_rejects_stale_epoch_single_side() {
        let f = Fixture::new();
        f.add_worker(100, crate::pd::pool::POOL_ID_SSD, &[]);
        let manager = Manager::new(f.ctx.clone());
        let event = worker_event(100, 1, Some(NodeState::Live));

        let fenced = manager.fence_worker_event(&event);

        assert!(fenced.is_none());
    }

    #[test]
    fn fence_worker_event_rejects_state_mismatch_single_side() {
        let f = Fixture::new();
        f.add_worker(100, crate::pd::pool::POOL_ID_SSD, &[]);
        let manager = Manager::new(f.ctx.clone());
        let event = worker_event(100, 0, Some(NodeState::Lost));

        let fenced = manager.fence_worker_event(&event);

        assert!(fenced.is_none());
    }

    #[test]
    fn fence_deleted_worker_event_accepts_missing_node() {
        let f = Fixture::new();
        let manager = Manager::new(f.ctx.clone());
        let mut event = worker_event(100, 1, None);
        event.event_type = NodeEventType::DecommissionFinished;

        assert!(manager.fence_deleted_worker_event(&event));
    }

    #[test]
    fn fence_deleted_worker_event_rejects_newer_incarnation() {
        let f = Fixture::new();
        f.add_worker(100, crate::pd::pool::POOL_ID_SSD, &[]);
        let mut node = f.ctx.node_manager.get_node(100).unwrap();
        node.epoch = 2;
        f.ctx.node_manager.test_insert_node(node);
        let manager = Manager::new(f.ctx.clone());
        let mut event = worker_event(100, 1, None);
        event.event_type = NodeEventType::DecommissionFinished;

        assert!(!manager.fence_deleted_worker_event(&event));
    }
}
