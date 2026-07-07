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

use crate::pd::bgtable::BGTableManager;
use crate::pd::config::ConfigManager;
use crate::pd::coordinator::{Coordinator, CoordinatorContext, OperatorController};
use crate::pd::metaroute::MetaRouteManager;
use crate::pd::mount::MountManager;
use crate::pd::namespace::NamespaceManager;
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use curvine_common::state::{
    BGKind, HeartbeatPayload, HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload,
    MetaHeartbeatResponse, NodePayload, NodeState, NodeType, RegisterRequest,
    TaskHeartbeatResponse, WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
use orpc::runtime::RpcRuntime;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

// LeaderChecker / RaftLeaderChecker / AlwaysLeader live in the leaf module.
#[cfg(test)]
pub use crate::pd::leader::AlwaysLeader;
pub use crate::pd::leader::{LeaderChecker, RaftLeaderChecker};

/// Cluster manager: ties node, pool, bg and coordinator.
/// Owns the leader lifecycle: starts/stops schedule+liveness loops on leader change.
pub struct ClusterManager {
    pub(crate) cluster_id: String,
    pub(crate) node_manager: Arc<NodeManager>,
    pub(crate) pool_manager: Arc<PoolManager>,
    pub(crate) bgtable_manager: Arc<BGTableManager>,
    pub(crate) config_manager: Arc<ConfigManager>,
    pub(crate) mount_manager: Arc<MountManager>,
    pub(crate) namespace_manager: Arc<NamespaceManager>,
    pub(crate) metaroute_manager: Arc<MetaRouteManager>,
    coordinator: Arc<Coordinator>,
    leader_checker: Arc<dyn LeaderChecker>,
    runtime: Arc<orpc::runtime::Runtime>,
    leader_token: Mutex<Option<CancellationToken>>,
}

impl ClusterManager {
    pub fn new(
        cluster_id: String,
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bgtable_manager: Arc<BGTableManager>,
        config_manager: Arc<ConfigManager>,
        mount_manager: Arc<MountManager>,
        namespace_manager: Arc<NamespaceManager>,
        metaroute_manager: Arc<MetaRouteManager>,
        leader_checker: Arc<dyn LeaderChecker>,
        runtime: Arc<orpc::runtime::Runtime>,
    ) -> Self {
        let operator_controller = Arc::new(OperatorController::new(
            config_manager.clone(),
            bgtable_manager.clone(),
        ));
        let ctx = Arc::new(CoordinatorContext {
            node_manager: node_manager.clone(),
            pool_manager: pool_manager.clone(),
            bgtable_manager: bgtable_manager.clone(),
            config_manager: config_manager.clone(),
            operator_controller,
            runtime: runtime.clone(),
        });
        let coordinator = Arc::new(Coordinator::new(ctx));

        Self {
            cluster_id,
            node_manager,
            pool_manager,
            bgtable_manager,
            config_manager,
            mount_manager,
            namespace_manager,
            metaroute_manager,
            coordinator,
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
            "PD became leader, resetting BG replica runtime states and starting coordinator/liveness loops"
        );
        self.bgtable_manager.reset_replica_states();
        let token = CancellationToken::new();
        let event_rx = self.node_manager.subscribe();
        self.coordinator.clone().start(event_rx, token.clone());
        self.node_manager
            .clone()
            .start_liveness_loop(self.runtime.clone(), token.clone());
        *guard = Some(token);
        true
    }

    fn on_leader_stop(&self) {
        let mut guard = self.leader_token.lock().unwrap();
        if let Some(token) = guard.take() {
            log::info!("PD lost leadership, stopping coordinator and liveness loops");
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

    /// Validate cluster_id, node type, and that the payload variant matches the
    /// expected node type. `payload_matches` is computed by the caller because
    /// register and heartbeat carry different payload enums. `what` names the
    /// message context (e.g. "register" / "heartbeat").
    fn validate_node(
        &self,
        cluster_id: &str,
        actual: NodeType,
        expected: NodeType,
        payload_matches: bool,
        what: &str,
    ) -> FsResult<()> {
        self.validate_cluster_id(cluster_id)?;
        if actual != expected || !payload_matches {
            return Err(FsError::common(format!(
                "expected {:?} {} payload, got node_type={:?}",
                expected, what, actual
            )));
        }
        Ok(())
    }

    fn validate_worker_register(&self, req: &RegisterRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, NodePayload::Worker(_));
        self.validate_node(
            &req.cluster_id,
            req.base.node_type,
            NodeType::Worker,
            ok,
            "register",
        )
    }

    fn validate_meta_register(&self, req: &RegisterRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, NodePayload::Meta(_));
        self.validate_node(
            &req.cluster_id,
            req.base.node_type,
            NodeType::Meta,
            ok,
            "register",
        )
    }

    fn validate_task_register(&self, req: &RegisterRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, NodePayload::Task(_));
        self.validate_node(
            &req.cluster_id,
            req.base.node_type,
            NodeType::Task,
            ok,
            "register",
        )
    }

    fn validate_worker_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, HeartbeatPayload::Worker(_));
        self.validate_node(
            &req.cluster_id,
            req.node_type,
            NodeType::Worker,
            ok,
            "heartbeat",
        )
    }

    fn validate_meta_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, HeartbeatPayload::Meta(_));
        self.validate_node(
            &req.cluster_id,
            req.node_type,
            NodeType::Meta,
            ok,
            "heartbeat",
        )
    }

    fn validate_task_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<()> {
        let ok = matches!(&req.payload, HeartbeatPayload::Task(_));
        self.validate_node(
            &req.cluster_id,
            req.node_type,
            NodeType::Task,
            ok,
            "heartbeat",
        )
    }

    pub fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        self.validate_worker_register(&req)?;

        let (node_info, new_epoch) = self.node_manager.register(req)?;

        let worker_bgs =
            self.bgtable_manager
                .bg()
                .bgs_on_worker(BGKind::Hash, node_info.base.node_id, None);
        // Wire format expects Vec<BlockGroupInfo>; deref-clone Arc-wrapped values.
        let worker_bgs: Vec<curvine_common::state::BlockGroupInfo> =
            worker_bgs.into_iter().map(|arc| (*arc).clone()).collect();

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            mount_version: self.mount_manager.version(),
            table_epochs: self.bgtable_manager.get_table_epochs(),
            simple_cluster_view_hint: self.build_simple_cluster_view_hint(),
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

        let reported_bg_epoch_by_id: std::collections::HashMap<curvine_common::state::BgId, u64> =
            if let HeartbeatPayload::Worker(ref w) = req.payload {
                if !w.bg_reports.is_empty() {
                    if let Err(e) = self
                        .bgtable_manager
                        .bg()
                        .apply_replica_reports(req.node_id, &w.bg_reports)
                    {
                        log::warn!(
                            "worker heartbeat bg_reports soft error worker_id={}, err={}",
                            req.node_id,
                            e
                        );
                        resp.error = Some(format!("apply bg_reports failed: {}", e));
                    }
                    if let Err(e) = self
                        .bgtable_manager
                        .reconcile_replicas(req.node_id, &w.bg_reports)
                    {
                        log::warn!(
                            "worker heartbeat bg table reconcile soft error worker_id={}, err={}",
                            req.node_id,
                            e
                        );
                        resp.error = Some(format!("reconcile bg_reports failed: {}", e));
                    }
                }
                w.bg_reports
                    .iter()
                    .map(|report| (report.bg_id, report.bg_epoch))
                    .collect()
            } else {
                std::collections::HashMap::new()
            };

        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bgtable_manager.get_table_epochs();
        resp.simple_cluster_view_hint = self.build_simple_cluster_view_hint();

        let commands = self
            .coordinator
            .dispatch_operators(req.node_id, &reported_bg_epoch_by_id);
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
        meta_resp.path_route_update = self.metaroute_manager.get_path_route_update();
        meta_resp.node_group_update = self.metaroute_manager.get_node_group_update();

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            mount_version: self.mount_manager.version(),
            table_epochs: self.bgtable_manager.get_table_epochs(),
            simple_cluster_view_hint: self.build_simple_cluster_view_hint(),
            payload: HeartbeatResponsePayload::Meta(meta_resp),
        })
    }

    pub fn handle_meta_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        self.validate_meta_heartbeat(&req)?;
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bgtable_manager.get_table_epochs();
        resp.simple_cluster_view_hint = self.build_simple_cluster_view_hint();
        if let HeartbeatResponsePayload::Meta(ref mut meta) = resp.payload {
            meta.path_route_update = self.metaroute_manager.get_path_route_update();
            meta.node_group_update = self.metaroute_manager.get_node_group_update();
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
            table_epochs: self.bgtable_manager.get_table_epochs(),
            simple_cluster_view_hint: self.build_simple_cluster_view_hint(),
            payload: HeartbeatResponsePayload::Task(TaskHeartbeatResponse::default()),
        })
    }

    pub fn handle_task_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        self.validate_task_heartbeat(&req)?;
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        resp.table_epochs = self.bgtable_manager.get_table_epochs();
        resp.simple_cluster_view_hint = self.build_simple_cluster_view_hint();
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

    pub fn bgtable_manager(&self) -> Arc<BGTableManager> {
        self.bgtable_manager.clone()
    }

    pub fn config_manager(&self) -> Arc<ConfigManager> {
        self.config_manager.clone()
    }

    pub fn namespace_manager(&self) -> Arc<NamespaceManager> {
        self.namespace_manager.clone()
    }

    pub fn metaroute_manager(&self) -> Arc<MetaRouteManager> {
        self.metaroute_manager.clone()
    }
}
