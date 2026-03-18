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
use crate::pd::journal;
use crate::pd::meta::MetaManager;
use crate::pd::mount::MountManager;
use crate::pd::node::{NodeEvent, NodeManager};
use crate::pd::pool::PoolManager;
use crate::pd::schedule::{Coordinator, CoordinatorContext};
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload, MetaHeartbeatResponse,
    NodePayload, NodeState, NodeType, RegisterRequest, WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
use log::info;
use std::sync::Arc;

/// Cluster manager: ties node, pool, bg, config, mount, meta (MetaNode Federation) and the schedule coordinator.
pub struct ClusterManager {
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    meta_manager: Arc<MetaManager>,
    journal_client: Arc<journal::Client>,
    coordinator: Arc<Coordinator>,
}

impl ClusterManager {
    pub fn new(
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        meta_manager: Option<Arc<MetaManager>>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        let event_rx = node_manager.subscribe();
        let ctx = Arc::new(CoordinatorContext {
            node_manager: node_manager.clone(),
            pool_manager: pool_manager.clone(),
            bg_manager: bg_manager.clone(),
            config_manager: config_manager.clone(),
            journal_client: journal_client.clone(),
        });
        let coordinator = Arc::new(Coordinator::new(ctx));
        coordinator.clone().run(event_rx);

        // Spawn event listener for ClusterManager-level dispatching
        let cm_event_rx = node_manager.subscribe();
        let pm = pool_manager.clone();
        let bgm = bg_manager.clone();
        let coord2 = coordinator.clone();
        let jc = journal_client.clone();
        tokio::spawn(async move {
            Self::event_loop(cm_event_rx, pm, bgm, coord2, jc).await;
        });

        let meta_manager = meta_manager.expect("MetaManager is required");
        Self {
            node_manager,
            pool_manager,
            bg_manager,
            config_manager,
            mount_manager,
            meta_manager,
            journal_client,
            coordinator,
        }
    }

    async fn event_loop(
        mut rx: tokio::sync::broadcast::Receiver<NodeEvent>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        coordinator: Arc<Coordinator>,
        journal_client: Arc<journal::Client>,
    ) {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    Self::handle_event(&event, &pool_manager, &bg_manager, &coordinator, &journal_client);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    log::warn!("ClusterManager event loop lagged {} events", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    info!("ClusterManager event channel closed");
                    break;
                }
            }
        }
    }

    fn handle_event(
        event: &NodeEvent,
        pool_manager: &PoolManager,
        bg_manager: &BGManager,
        coordinator: &Coordinator,
        _journal_client: &journal::Client,
    ) {
        match event {
            NodeEvent::Registered {
                node_id,
                node_type: NodeType::Worker,
                ..
            } => {
                coordinator.on_worker_joined(*node_id, pool_manager.get_pools_by_worker(*node_id));
            }
            NodeEvent::StateChanged {
                node_id,
                node_type: NodeType::Worker,
                old_state,
                new_state,
            } => {
                match (old_state, new_state) {
                    (NodeState::Starting, NodeState::Live)
                    | (NodeState::Lost, NodeState::Live) => {
                        // Worker came alive — assign to pools is done during register/heartbeat
                        info!("Worker {} became Live", node_id);
                    }
                    (NodeState::Live, NodeState::Lost) => {
                        let affected = bg_manager.get_bgs_on_worker(*node_id);
                        for bg in &affected {
                            if let Err(e) = bg_manager.propose_update_bg(
                                crate::pd::journal::entry::BGUpdateEntry {
                                    op_ms: orpc::common::LocalTime::mills(),
                                    bg_id: bg.bg_id,
                                    state: Some(curvine_common::state::BGState::Degraded),
                                    replica_set: None,
                                    lease_owner: None,
                                },
                            ) {
                                log::warn!("Failed to degrade BG {}: {}", bg.bg_id, e);
                            }
                        }
                        if !affected.is_empty() {
                            log::warn!(
                                "Worker {} lost, {} BGs degraded via event",
                                node_id,
                                affected.len()
                            );
                        }
                    }
                    (_, NodeState::Offline) => {
                        let pool_ids = pool_manager.get_pools_by_worker(*node_id);
                        if let Err(e) = pool_manager.remove_worker_from_pools(*node_id) {
                            log::error!("remove_worker_from_pools {} failed: {}", node_id, e);
                        }
                        coordinator.on_worker_removed(*node_id, pool_ids);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    // ========== Worker registration & heartbeat ==========

    pub fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        let worker_payload = match &req.payload {
            NodePayload::Worker(p) => p.clone(),
            _ => return Err(FsError::common("expected Worker payload")),
        };

        // NodeManager.register() validates, builds NodeInfo, and proposes via Raft.
        let (node_info, new_epoch) = self.node_manager.register(req)?;

        let pool_ids = self
            .pool_manager
            .assign_worker_to_pools(node_info.base.node_id, &worker_payload.storage_specs)?;

        self.coordinator
            .on_worker_joined(node_info.base.node_id, pool_ids);

        let assigned_bgs = self.bg_manager.list_bgs();
        let worker_bgs: Vec<_> = assigned_bgs
            .into_iter()
            .filter(|bg| bg.replica_set.contains(&node_info.base.node_id))
            .collect();

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            config_version: 0,
            mount_version: 0,
            bg_version: 0,
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: worker_bgs,
                remove_bgs: vec![],
                update_bgs: vec![],
            }),
        })
    }

    pub fn handle_worker_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let mut resp = self.node_manager.handle_heartbeat(req.clone())?;

        let commands = self.coordinator.dispatch_operators(req.node_id);
        if let HeartbeatResponsePayload::Worker(ref mut w) = resp.payload {
            w.add_bgs.extend(commands.add_bgs);
            w.remove_bgs.extend(commands.remove_bgs);
        }

        Ok(resp)
    }

    // ========== MetaNode registration & heartbeat ==========

    pub fn handle_meta_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        if !matches!(&req.payload, NodePayload::Meta(_)) {
            return Err(FsError::common("expected Meta payload"));
        }

        let (_node_info, new_epoch) = self.node_manager.register(req)?;

        let mut meta_resp = MetaHeartbeatResponse {
            path_route_update: None,
            node_group_update: None,
        };
        meta_resp.path_route_update = self.meta_manager.get_path_route_update();
        meta_resp.node_group_update = self.meta_manager.get_node_group_update();

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            config_version: 0,
            mount_version: 0,
            bg_version: 0,
            payload: HeartbeatResponsePayload::Meta(meta_resp),
        })
    }

    pub fn handle_meta_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        if let HeartbeatResponsePayload::Meta(ref mut meta) = resp.payload {
            meta.path_route_update = self.meta_manager.get_path_route_update();
            meta.node_group_update = self.meta_manager.get_node_group_update();
        }
        Ok(resp)
    }

    // ========== Worker offline ==========

    pub fn on_worker_offline(&self, worker_id: u32) -> FsResult<()> {
        let pool_ids = self.pool_manager.get_pools_by_worker(worker_id);
        self.pool_manager.remove_worker_from_pools(worker_id)?;
        self.coordinator.on_worker_removed(worker_id, pool_ids);
        Ok(())
    }

    pub fn handle_decommission(&self, node_id: u32, wait_migration: bool) -> FsResult<NodeState> {
        let node = self
            .node_manager
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;

        let target_state = if wait_migration {
            NodeState::Decommission
        } else {
            NodeState::Offline
        };

        self.node_manager
            .update_state_and_persist(node_id, target_state)?;

        if !wait_migration && node.base.node_type == curvine_common::state::NodeType::Worker {
            self.on_worker_offline(node_id)?;
        }

        Ok(target_state)
    }

    // ========== Accessors ==========

    pub fn node_manager(&self) -> Arc<NodeManager> {
        self.node_manager.clone()
    }

    pub fn pool_manager(&self) -> Arc<PoolManager> {
        self.pool_manager.clone()
    }

    pub fn bg_manager(&self) -> Arc<BGManager> {
        self.bg_manager.clone()
    }

    pub fn config_manager(&self) -> Arc<ConfigManager> {
        self.config_manager.clone()
    }

    pub fn route_path(&self, path: &str) -> Option<u64> {
        self.meta_manager.route(path).ok()
    }

    pub fn meta_manager(&self) -> Arc<MetaManager> {
        self.meta_manager.clone()
    }
}
