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
use curvine_common::state::{
    HeartbeatPayload, HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload,
    MetaHeartbeatResponse, NodePayload, NodeState, NodeType, RegisterRequest,
    TaskHeartbeatResponse, WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
use orpc::runtime::RpcRuntime;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

// LeaderChecker / RaftLeaderChecker / AlwaysLeader were moved to
// `pd/journal/leader.rs` to avoid a circular dependency with `journal::Client`.
// Re-exports keep existing call sites (`crate::pd::cluster::manager::...`) working.
#[cfg(test)]
pub use crate::pd::journal::AlwaysLeader;
pub use crate::pd::journal::{LeaderChecker, RaftLeaderChecker};

/// Cluster manager: ties node, pool, bg and schedule manager.
/// Owns the leader lifecycle: starts/stops schedule+liveness loops on leader change.
pub struct ClusterManager {
    cluster_id: String,
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
        cluster_id: String,
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        meta_manager: Arc<MetaManager>,
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

        Self {
            cluster_id,
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
                was_leader = self.on_leader_start();
            } else if !is_leader && was_leader {
                self.on_leader_stop();
                was_leader = false;
            }
        }
    }

    fn on_leader_start(&self) -> bool {
        let mut guard = self.leader_token.lock().unwrap();
        if guard.is_some() {
            return true;
        }
        log::info!(
            "PD became leader, rebuilding runtime route view and starting schedule/liveness loops"
        );
        if let Err(e) = self.bg_manager.on_leader_start() {
            log::warn!(
                "BG leader-start route epoch bump failed, will retry leader start: {}",
                e
            );
            return false;
        }
        let token = CancellationToken::new();
        let event_rx = self.node_manager.subscribe();
        self.schedule_manager.clone().start(event_rx, token.clone());
        self.node_manager
            .clone()
            .start_liveness_loop(self.runtime.clone(), token.clone());
        *guard = Some(token);
        true
    }

    fn on_leader_stop(&self) {
        let mut guard = self.leader_token.lock().unwrap();
        if let Some(token) = guard.take() {
            log::info!("PD lost leadership, stopping schedule and liveness loops");
            token.cancel();
        }
    }

    pub fn is_leader(&self) -> bool {
        self.leader_checker.is_leader()
    }

    pub fn ensure_leader(&self) -> FsResult<()> {
        if !self.is_leader() {
            return Err(FsError::not_leader(
                "PD node RPC rejected: this PD node is not the raft leader",
            ));
        }
        Ok(())
    }

    fn validate_cluster_id(&self, actual: &str) -> FsResult<()> {
        if actual != self.cluster_id {
            return Err(FsError::common(format!(
                "cluster_id mismatch: expected {} got {}",
                self.cluster_id, actual
            )));
        }
        Ok(())
    }

    fn validate_worker_register(&self, req: &RegisterRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.base.node_type != NodeType::Worker || !matches!(&req.payload, NodePayload::Worker(_))
        {
            return Err(FsError::common(format!(
                "expected Worker register payload, got node_type={:?}",
                req.base.node_type
            )));
        }
        Ok(())
    }

    fn validate_meta_register(&self, req: &RegisterRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.base.node_type != NodeType::Meta || !matches!(&req.payload, NodePayload::Meta(_)) {
            return Err(FsError::common(format!(
                "expected Meta register payload, got node_type={:?}",
                req.base.node_type
            )));
        }
        Ok(())
    }

    fn validate_task_register(&self, req: &RegisterRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.base.node_type != NodeType::Task || !matches!(&req.payload, NodePayload::Task(_)) {
            return Err(FsError::common(format!(
                "expected Task register payload, got node_type={:?}",
                req.base.node_type
            )));
        }
        Ok(())
    }

    fn validate_worker_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.node_type != NodeType::Worker || !matches!(&req.payload, HeartbeatPayload::Worker(_))
        {
            return Err(FsError::common(format!(
                "expected Worker heartbeat payload, got node_type={:?}",
                req.node_type
            )));
        }
        Ok(())
    }

    fn validate_meta_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.node_type != NodeType::Meta || !matches!(&req.payload, HeartbeatPayload::Meta(_)) {
            return Err(FsError::common(format!(
                "expected Meta heartbeat payload, got node_type={:?}",
                req.node_type
            )));
        }
        Ok(())
    }

    fn validate_task_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        self.validate_cluster_id(&req.cluster_id)?;
        if req.node_type != NodeType::Task || !matches!(&req.payload, HeartbeatPayload::Task(_)) {
            return Err(FsError::common(format!(
                "expected Task heartbeat payload, got node_type={:?}",
                req.node_type
            )));
        }
        Ok(())
    }

    pub fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        self.validate_worker_register(&req)?;

        let (node_info, new_epoch) = self.node_manager.register(req)?;

        let worker_bgs = self.bg_manager.get_bgs_on_worker(node_info.base.node_id);
        // Wire format expects Vec<BlockGroupInfo>; deref-clone Arc-wrapped values.
        let worker_bgs: Vec<curvine_common::state::BlockGroupInfo> =
            worker_bgs.into_iter().map(|arc| (*arc).clone()).collect();

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
        self.validate_worker_heartbeat(&req)?;
        let mut resp = self.node_manager.handle_heartbeat(req.clone())?;

        let reported_bg_epochs: std::collections::HashMap<u32, u64> =
            if let HeartbeatPayload::Worker(ref w) = req.payload {
                if !w.bg_reports.is_empty() {
                    if let Err(e) = self
                        .bg_manager
                        .apply_replica_reports(req.node_id, &w.bg_reports)
                    {
                        log::warn!(
                            "worker heartbeat bg_reports soft error worker_id={}, err={}",
                            req.node_id,
                            e
                        );
                        resp.error = Some(format!("apply bg_reports failed: {}", e));
                    }
                } else if !w.bg_epochs.is_empty() {
                    let bg_ids: Vec<u32> = w.bg_epochs.keys().copied().collect();
                    if let Err(e) = self
                        .bg_manager
                        .promote_pending_replicas(req.node_id, &bg_ids)
                    {
                        log::warn!(
                            "worker heartbeat promote_pending_replicas soft error worker_id={}, err={}",
                            req.node_id,
                            e
                        );
                        resp.error = Some(format!("promote pending replicas failed: {}", e));
                    }
                }
                w.bg_epochs.clone()
            } else {
                std::collections::HashMap::new()
            };

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
        self.validate_meta_register(&req)?;

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
        self.validate_meta_heartbeat(&req)?;
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bg_manager.get_table_epochs();
        if let HeartbeatResponsePayload::Meta(ref mut meta) = resp.payload {
            meta.path_route_update = self.meta_manager.get_path_route_update();
            meta.node_group_update = self.meta_manager.get_node_group_update();
        }
        Ok(resp)
    }

    pub fn handle_task_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        self.validate_task_register(&req)?;

        let (_node_info, new_epoch) = self.node_manager.register(req)?;

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            mount_version: self.mount_manager.version(),
            table_epochs: self.bg_manager.get_table_epochs(),
            payload: HeartbeatResponsePayload::Task(TaskHeartbeatResponse::default()),
        })
    }

    pub fn handle_task_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        self.validate_task_heartbeat(&req)?;
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bg_manager.get_table_epochs();
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
