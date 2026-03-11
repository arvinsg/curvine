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

use super::{HandlerRegistry, HeartbeatHandler, MetaHeartbeatHandler, WorkerHeartbeatHandler};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::{NodeEntry, NodeStateEntry};
use curvine_common::state::{
    BlockGroupInfo, BlockGroupInfoView, HeartbeatPayload, HeartbeatRequest, HeartbeatResponse,
    NodeInfo, NodePayload, NodeState, NodeType, ReplicaInfo, RegisterRequest,
};
use curvine_common::{FsError, FsResult};
use std::sync::Arc;
use std::sync::RwLock;

use super::index::NodeIndex;
use super::store::NodeStore;

pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    handler_registry: HandlerRegistry,
    #[allow(dead_code)] // TODO: used for pd.node.heartbeat_timeout_ms etc.
    config_manager: Arc<ConfigManager>,
}

impl NodeManager {
    pub fn new(store: Arc<NodeStore>, config_manager: Arc<ConfigManager>) -> Self {
        let mut registry = HandlerRegistry::new();
        registry.register(Arc::new(WorkerHeartbeatHandler::new()));
        registry.register(Arc::new(MetaHeartbeatHandler::new()));
        Self {
            index: Arc::new(RwLock::new(NodeIndex::new())),
            store,
            handler_registry: registry,
            config_manager,
        }
    }

    pub fn register_handler(&mut self, handler: Arc<dyn HeartbeatHandler>) {
        self.handler_registry.register(handler);
    }

    fn get_handler(&self, node_type: NodeType) -> FsResult<Arc<dyn HeartbeatHandler>> {
        self.handler_registry.get(node_type).ok_or_else(|| {
            FsError::common(format!("unsupported node type: {:?}", node_type))
        })
    }

    /// Builds NodeInfo and new_epoch for registration. Caller is responsible for proposing PdEntry::RegisterNode.
    pub fn prepare_register(&self, req: RegisterRequest) -> FsResult<(NodeInfo, u64)> {
        let handler = self.get_handler(req.base.node_type)?;
        let new_epoch = {
            let index = self.index.read().unwrap();
            if let Some(existing) = index.get_by_id(req.base.node_id) {
                if existing.state == NodeState::Lost || existing.state == NodeState::Offline {
                    existing.epoch + 1
                } else {
                    return Err(FsError::common(format!(
                        "node {} already registered and state {:?}",
                        req.base.node_id, existing.state
                    )));
                }
            } else {
                1
            }
        };
        let mut info = handler.handle_register(req)?;
        info.epoch = new_epoch;
        Ok((info, new_epoch))
    }

    /// Handles heartbeat: validates, updates last_heartbeat_ms, returns response.
    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let handler = self.get_handler(req.node_type)?;
        let mut index = self.index.write().unwrap();
        let node = index.get_by_id_mut(req.node_id).ok_or_else(|| {
            FsError::common(format!("node {} not found", req.node_id))
        })?;
        handler.validate_consistency(node, &req)?;
        node.last_heartbeat_ms = req.timestamp_ms;
        match &req.payload {
            HeartbeatPayload::Worker(w) => {
                node.sys_stats = w.sys_stats.clone();
                if let NodePayload::Worker(ref mut p) = &mut node.payload {
                    p.storage_stats = w.storage_stats.clone();
                }
            }
            HeartbeatPayload::Meta(m) => {
                node.sys_stats = m.sys_stats.clone();
                if let NodePayload::Meta(ref mut p) = &mut node.payload {
                    p.stats = m.inodes_stats.clone();
                }
            }
        }
        drop(index);
        let mut resp = handler.handle_heartbeat(req)?;
        // Fill versions when config/mount managers expose get_version()
        resp.config_version = 0;
        resp.mount_version = 0;
        Ok(resp)
    }

    /// Apply RegisterNode entry (called from PdAppStorage when Raft applies).
    pub fn apply_register_node(&self, entry: &NodeEntry) -> FsResult<()> {
        let mut info = entry.info.clone();
        info.last_heartbeat_ms = entry.op_ms;
        self.store.put(&info)?;
        let mut index = self.index.write().unwrap();
        index.insert(info);
        Ok(())
    }

    /// Apply UpdateNodeState entry (called from PdAppStorage when Raft applies).
    pub fn apply_update_state(&self, entry: &NodeStateEntry) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        if !index.update_state(entry.node_id, entry.new_state) {
            return Ok(());
        }
        if let Some(node) = index.get_by_id(entry.node_id).cloned() {
            drop(index);
            let mut node_for_store = node;
            node_for_store.epoch = entry.new_epoch.unwrap_or(node_for_store.epoch);
            self.store.put(&node_for_store)?;
        }
        Ok(())
    }

    /// Restore in-memory index from store (call on startup).
    pub fn restore(&self) -> FsResult<()> {
        let nodes = self.store.list_all()?;
        let mut index = self.index.write().unwrap();
        for node in nodes {
            index.insert(node);
        }
        Ok(())
    }

    /// Get node by id.
    pub fn get_node(&self, node_id: u32) -> Option<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_id(node_id).cloned()
    }

    /// Get nodes by type.
    pub fn get_nodes_by_type(&self, node_type: NodeType) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_type(node_type).into_iter().cloned().collect()
    }

    /// Get nodes by state.
    pub fn get_nodes_by_state(&self, state: NodeState) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_state(state).into_iter().cloned().collect()
    }

    /// Mark nodes that have not heartbeaten within timeout as Lost (in-memory only).
    /// Call periodically; actual Raft propose for UpdateNodeState can be done by caller.
    /// TODO:
    pub fn check_heartbeat_timeout(&self, now_ms: u64, timeout_ms: u64) -> Vec<u32> {
        let mut index = self.index.write().unwrap();
        let mut marked = Vec::new();
        for node_id in index.all_node_ids() {
            if let Some(node) = index.get_by_id(node_id) {
                if node.state == NodeState::Live
                    && node.last_heartbeat_ms > 0
                    && now_ms.saturating_sub(node.last_heartbeat_ms) > timeout_ms
                {
                    index.update_state(node_id, NodeState::Lost);
                    marked.push(node_id);
                }
            }
        }
        marked
    }

    /// Returns heartbeat timeout in ms (from config or default).
    #[allow(dead_code)] // TODO: used when check_heartbeat_timeout is wired to a timer
    fn heartbeat_timeout_ms(&self) -> u64 {
        // TODO: read from config_manager pd.node.heartbeat_timeout_ms
        60_000
    }
}
