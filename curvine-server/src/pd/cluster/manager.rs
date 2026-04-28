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
use crate::pd::meta::MetaManager;
use crate::pd::mount::MountManager;
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use crate::pd::schedule::{Manager, ManagerContext, OperatorController};
use curvine_common::raft::RoleState;
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload, MetaHeartbeatResponse,
    NodePayload, NodeState, RegisterRequest, WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
use orpc::runtime::RpcRuntime;
use orpc::sync::StateCtl;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

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

/// Cluster manager: ties node, pool, bg and schedule manager.
/// Owns the leader lifecycle: starts/stops schedule+liveness loops on leader change.
pub struct ClusterManager {
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    config_manager: Arc<ConfigManager>,
    mount_manager: Arc<MountManager>,
    meta_manager: Arc<MetaManager>,
    schedule_manager: Arc<Manager>,
    leader_checker: Arc<dyn LeaderChecker>,
    runtime: Arc<orpc::runtime::Runtime>,
    leader_token: Mutex<Option<CancellationToken>>,
}

impl ClusterManager {
    pub fn new(
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        meta_manager: Option<Arc<MetaManager>>,
        leader_checker: Arc<dyn LeaderChecker>,
        runtime: Arc<orpc::runtime::Runtime>,
    ) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            config_manager.clone(),
            bg_manager.clone(),
        ));
        let ctx = Arc::new(ManagerContext {
            node_manager: node_manager.clone(),
            pool_manager: pool_manager.clone(),
            bg_manager: bg_manager.clone(),
            config_manager: config_manager.clone(),
            operator_controller,
            runtime: runtime.clone(),
        });
        let schedule_manager = Arc::new(Manager::new(ctx));

        let meta_manager = meta_manager.expect("MetaManager is required");
        Self {
            node_manager,
            pool_manager,
            bg_manager,
            config_manager,
            mount_manager,
            meta_manager,
            schedule_manager,
            leader_checker,
            runtime,
            leader_token: Mutex::new(None),
        }
    }

    /// Start the leader lifecycle monitor. Polls leadership status and
    /// starts/stops schedule+liveness loops accordingly.
    pub fn start_leader_monitor(self: &Arc<Self>) {
        let mgr = self.clone();
        self.runtime.spawn(async move {
            mgr.leader_lifecycle_loop().await;
        });
    }

    async fn leader_lifecycle_loop(&self) {
        let mut was_leader = false;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let is_leader = self.leader_checker.is_leader();
            if is_leader && !was_leader {
                self.on_leader_start();
                was_leader = true;
            } else if !is_leader && was_leader {
                self.on_leader_stop();
                was_leader = false;
            }
        }
    }

    fn on_leader_start(&self) {
        let mut guard = self.leader_token.lock().unwrap();
        if guard.is_some() {
            return;
        }
        log::info!("PD became leader, starting schedule and liveness loops");
        let token = CancellationToken::new();
        let event_rx = self.node_manager.subscribe();
        self.schedule_manager.clone().start(event_rx, token.clone());
        self.node_manager
            .clone()
            .start_liveness_loop(self.runtime.clone(), token.clone());
        *guard = Some(token);
    }

    fn on_leader_stop(&self) {
        let mut guard = self.leader_token.lock().unwrap();
        if let Some(token) = guard.take() {
            log::info!("PD lost leadership, stopping schedule and liveness loops");
            token.cancel();
        }
    }

    pub fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        let worker_payload = match &req.payload {
            NodePayload::Worker(p) => p.clone(),
            _ => return Err(FsError::common("expected Worker payload")),
        };

        let (node_info, new_epoch) = self.node_manager.register(req)?;

        let _pool_ids = self
            .pool_manager
            .assign_worker_to_pools(node_info.base.node_id, &worker_payload.storage_specs)?;

        let worker_bgs = self.bg_manager.get_bgs_on_worker(node_info.base.node_id);

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            mount_version: self.mount_manager.version(),
            table_epochs: self.bg_manager.get_table_epochs(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: worker_bgs,
                remove_bgs: vec![],
                update_bgs: vec![],
            }),
        })
    }

    pub fn handle_worker_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let reported_bg_epochs: std::collections::HashMap<u32, u64> =
            if let curvine_common::state::HeartbeatPayload::Worker(ref w) = req.payload {
                if !w.bg_reports.is_empty() {
                    self.bg_manager
                        .apply_replica_reports(req.node_id, &w.bg_reports);
                } else if !w.bg_epochs.is_empty() {
                    let bg_ids: Vec<u32> = w.bg_epochs.keys().copied().collect();
                    self.bg_manager
                        .promote_pending_replicas(req.node_id, &bg_ids);
                }
                w.bg_epochs.clone()
            } else {
                std::collections::HashMap::new()
            };

        let mut resp = self.node_manager.handle_heartbeat(req.clone())?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bg_manager.get_table_epochs();

        let commands = self
            .schedule_manager
            .dispatch_operators(req.node_id, &reported_bg_epochs);
        if let HeartbeatResponsePayload::Worker(ref mut w) = resp.payload {
            w.add_bgs.extend(commands.add_bgs);
            w.remove_bgs.extend(commands.remove_bgs);
            w.update_bgs.extend(commands.update_bgs);
        }

        Ok(resp)
    }

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
            mount_version: self.mount_manager.version(),
            table_epochs: self.bg_manager.get_table_epochs(),
            payload: HeartbeatResponsePayload::Meta(meta_resp),
        })
    }

    pub fn handle_meta_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bg_manager.get_table_epochs();
        if let HeartbeatResponsePayload::Meta(ref mut meta) = resp.payload {
            meta.path_route_update = self.meta_manager.get_path_route_update();
            meta.node_group_update = self.meta_manager.get_node_group_update();
        }
        Ok(resp)
    }

    pub fn handle_decommission(&self, node_id: u32) -> FsResult<NodeState> {
        self.node_manager.start_decommission(node_id)
    }

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
