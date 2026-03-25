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
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use crate::pd::schedule::coordinator::LeaderChecker;
use crate::pd::schedule::{Coordinator, CoordinatorContext};
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload,
    MetaHeartbeatResponse, NodePayload, NodeState, RegisterRequest,
    WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
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
        leader_checker: Arc<dyn LeaderChecker>,
    ) -> Self {
        let event_rx = node_manager.subscribe();
        let ctx = Arc::new(CoordinatorContext {
            node_manager: node_manager.clone(),
            pool_manager: pool_manager.clone(),
            bg_manager: bg_manager.clone(),
            config_manager: config_manager.clone(),
            journal_client: journal_client.clone(),
            leader_checker,
        });
        let coordinator = Arc::new(Coordinator::new(ctx));
        coordinator.clone().run(event_rx);

        node_manager.clone().start_liveness_loop();

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

    // ========== Worker registration & heartbeat ==========

    pub fn handle_worker_register(&self, req: RegisterRequest) -> FsResult<HeartbeatResponse> {
        let worker_payload = match &req.payload {
            NodePayload::Worker(p) => p.clone(),
            _ => return Err(FsError::common("expected Worker payload")),
        };

        // NodeManager.register() validates, builds NodeInfo, and proposes via Raft.
        let (node_info, new_epoch) = self.node_manager.register(req)?;

        let _pool_ids = self
            .pool_manager
            .assign_worker_to_pools(node_info.base.node_id, &worker_payload.storage_specs)?;

        // Coordinator event_loop handles rebuild scheduling via NodeEvent::Registered

        let worker_bgs = self.bg_manager.get_bgs_on_worker(node_info.base.node_id);

        Ok(HeartbeatResponse {
            error: None,
            epoch: new_epoch,
            mount_version: self.mount_manager.version(),
            bg_version: self.bg_manager.max_table_epoch(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: worker_bgs,
                remove_bgs: vec![],
                update_bgs: vec![],
            }),
        })
    }

    pub fn handle_worker_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let mut resp = self.node_manager.handle_heartbeat(req.clone())?;
        resp.mount_version = self.mount_manager.version();

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
            mount_version: self.mount_manager.version(),
            bg_version: self.bg_manager.max_table_epoch(),
            payload: HeartbeatResponsePayload::Meta(meta_resp),
        })
    }

    pub fn handle_meta_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let mut resp = self.node_manager.handle_heartbeat(req)?;
        resp.mount_version = self.mount_manager.version();
        if let HeartbeatResponsePayload::Meta(ref mut meta) = resp.payload {
            meta.path_route_update = self.meta_manager.get_path_route_update();
            meta.node_group_update = self.meta_manager.get_node_group_update();
        }
        Ok(resp)
    }

    // ========== Decommission ==========

    pub fn handle_decommission(&self, node_id: u32) -> FsResult<NodeState> {
        self.node_manager.start_decommission(node_id)
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
