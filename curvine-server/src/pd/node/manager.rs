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

use super::event::NodeEvent;
use super::{HandlerRegistry, HeartbeatHandler, MetaHeartbeatHandler, WorkerHeartbeatHandler};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::NodeEntry;
use crate::pd::journal::{self, PdEntry};
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, NodeInfo, NodeState, NodeType, RegisterRequest,
};
use curvine_common::{FsError, FsResult};
use log::info;
use orpc::common::LocalTime;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::broadcast;

use super::index::NodeIndex;
use super::store::NodeStore;

const EVENT_CHANNEL_CAPACITY: usize = 256;

pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    handler_registry: HandlerRegistry,
    config_manager: Arc<ConfigManager>,
    journal_client: Arc<journal::Client>,
    event_tx: broadcast::Sender<NodeEvent>,
}

impl NodeManager {
    pub fn new(
        store: Arc<NodeStore>,
        config_manager: Arc<ConfigManager>,
        journal_client: Arc<journal::Client>,
    ) -> Self {
        let mut registry = HandlerRegistry::new();
        registry.register(Arc::new(WorkerHeartbeatHandler::new()));
        registry.register(Arc::new(MetaHeartbeatHandler::new()));
        let (event_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            index: Arc::new(RwLock::new(NodeIndex::new())),
            store,
            handler_registry: registry,
            config_manager,
            journal_client,
            event_tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NodeEvent> {
        self.event_tx.subscribe()
    }

    fn emit_event(&self, event: NodeEvent) {
        let _ = self.event_tx.send(event);
    }

    pub fn register_handler(&mut self, handler: Arc<dyn HeartbeatHandler>) {
        self.handler_registry.register(handler);
    }

    fn get_handler(&self, node_type: NodeType) -> FsResult<&(dyn HeartbeatHandler + 'static)> {
        self.handler_registry
            .get(node_type)
            .ok_or_else(|| FsError::common(format!("unsupported node type: {:?}", node_type)))
    }

    // ========== Registration ==========

    pub fn register(&self, req: RegisterRequest) -> FsResult<(NodeInfo, u64)> {
        let handler = self.get_handler(req.base.node_type)?;
        let now = LocalTime::mills();

        let new_epoch = {
            let index = self.index.read().unwrap();
            if let Some(existing) = index.get_by_id(req.base.node_id) {
                if existing.state == NodeState::Lost || existing.state == NodeState::Offline {
                    if existing.base.address != req.base.address {
                        info!(
                            "Node {} address changed: {:?} -> {:?}, re-registering",
                            req.base.node_id, existing.base.address, req.base.address
                        );
                    }
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

        let mut node = handler.build_node_info(&req)?;
        node.epoch = new_epoch;
        node.last_heartbeat_ms = now;

        let entry = NodeEntry {
            op_ms: now,
            info: node.clone(),
        };
        self.journal_client
            .propose(PdEntry::RegisterNode(entry))?;

        self.emit_event(NodeEvent::Registered {
            node_id: node.base.node_id,
            node_type: node.base.node_type,
            pool_ids: vec![],
        });

        Ok((node, new_epoch))
    }

    // ========== Heartbeat ==========

    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        let handler = self.get_handler(req.node_type)?;
        let now = LocalTime::mills();
        let persist_interval = self.persist_interval_ms();

        let (node_snapshot, need_persist) = {
            let mut index = self.index.write().unwrap();
            let node = index
                .get_by_id_mut(req.node_id)
                .ok_or_else(|| FsError::common(format!("node {} not found", req.node_id)))?;

            if node.epoch != req.epoch {
                return Err(FsError::common(format!(
                    "epoch mismatch for node {}: expected {} got {}",
                    req.node_id, node.epoch, req.epoch
                )));
            }

            // Reject heartbeats from terminal states
            match node.state {
                NodeState::Offline | NodeState::Blacklist => {
                    return Err(FsError::common(format!(
                        "node {} is {:?}, must re-register",
                        req.node_id, node.state
                    )));
                }
                _ => {}
            }

            // Delegate payload processing to handler
            let critical_changed = handler.process_heartbeat(node, &req)?;
            node.last_heartbeat_ms = now;

            // State transition: Starting/Lost → Live
            let old_state = node.state;
            let state_changed = matches!(old_state, NodeState::Starting | NodeState::Lost);

            let need_persist = critical_changed
                || state_changed
                || node.need_persist(now, persist_interval);

            let node_id = req.node_id;
            let node_type = node.base.node_type;
            if state_changed {
                index.update_state(node_id, NodeState::Live);
            }

            let snapshot = index.get_by_id(node_id).unwrap().clone();
            if state_changed {
                self.emit_event(NodeEvent::StateChanged {
                    node_id,
                    node_type,
                    old_state,
                    new_state: NodeState::Live,
                });
            }
            (snapshot, need_persist)
        };

        let response_payload = handler.build_heartbeat_response(&node_snapshot, &req)?;

        if need_persist {
            let entry = NodeEntry {
                op_ms: now,
                info: node_snapshot.clone(),
            };
            self.journal_client.propose(PdEntry::SaveNode(entry))?;

            let mut index = self.index.write().unwrap();
            if let Some(n) = index.get_by_id_mut(req.node_id) {
                n.last_persist_ms = now;
            }
        }

        Ok(HeartbeatResponse {
            error: None,
            epoch: node_snapshot.epoch,
            config_version: 0,
            mount_version: 0,
            bg_version: 0,
            payload: response_payload,
        })
    }

    // ========== Raft apply callbacks ==========

    /// Apply RegisterNode entry from Raft.
    pub fn apply_register_node(&self, entry: &NodeEntry) -> FsResult<()> {
        let mut node = entry.info.clone();
        node.last_persist_ms = entry.op_ms;
        self.store.put(&node)?;
        let mut index = self.index.write().unwrap();
        index.insert(node);
        Ok(())
    }

    /// Apply SaveNode entry from Raft — persists full NodeInfo.
    pub fn apply_save_node(&self, entry: &NodeEntry) -> FsResult<()> {
        self.store.put(&entry.info)?;
        let mut index = self.index.write().unwrap();
        let mut updated = entry.info.clone();
        if let Some(existing) = index.get_by_id(entry.info.base.node_id) {
            updated.preserve_memory_fields(existing);
        }
        updated.last_persist_ms = entry.op_ms;
        index.insert(updated);
        Ok(())
    }

    // ========== State management ==========

    /// Persist the current in-memory state of a node to Raft.
    pub fn persist_node(&self, node_id: u32) -> FsResult<()> {
        let node = {
            let index = self.index.read().unwrap();
            index.get_by_id(node_id).cloned()
        };
        let Some(node) = node else { return Ok(()) };
        let now = LocalTime::mills();
        let entry = NodeEntry {
            op_ms: now,
            info: node,
        };
        self.journal_client.propose(PdEntry::SaveNode(entry))?;
        let mut index = self.index.write().unwrap();
        if let Some(n) = index.get_by_id_mut(node_id) {
            n.last_persist_ms = now;
        }
        Ok(())
    }

    /// Update state in-memory and immediately persist via Raft.
    pub fn update_state_and_persist(&self, node_id: u32, new_state: NodeState) -> FsResult<()> {
        let old_info = {
            let mut index = self.index.write().unwrap();
            let old_info = index.get_by_id(node_id).map(|n| (n.state, n.base.node_type));
            index.update_state(node_id, new_state);
            old_info
        };
        if let Some((old_state, node_type)) = old_info {
            if old_state != new_state {
                self.emit_event(NodeEvent::StateChanged {
                    node_id,
                    node_type,
                    old_state,
                    new_state,
                });
            }
        }
        self.persist_node(node_id)
    }

    // ========== Restore & queries ==========

    /// Restore in-memory index from store (call on startup).
    pub fn restore(&self) -> FsResult<()> {
        let nodes = self.store.list_all()?;
        let mut index = self.index.write().unwrap();
        for mut node in nodes {
            // Treat persisted last_heartbeat_ms as the initial last_persist_ms
            // so that need_persist() won't fire on every first heartbeat.
            node.last_persist_ms = node.last_heartbeat_ms;
            index.insert(node);
        }
        Ok(())
    }

    pub fn get_node(&self, node_id: u32) -> Option<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_id(node_id).cloned()
    }

    pub fn get_nodes_by_type(&self, node_type: NodeType) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_type(node_type).into_iter().cloned().collect()
    }

    pub fn get_nodes_by_state(&self, state: NodeState) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index.get_by_state(state).into_iter().cloned().collect()
    }

    /// Detect nodes that have exceeded heartbeat timeout.
    /// Transitions Live → Lost in-memory and returns the timed-out node IDs.
    pub fn detect_heartbeat_timeout(&self, now_ms: u64, timeout_ms: u64) -> Vec<u32> {
        let mut index = self.index.write().unwrap();
        let all_ids = index.all_node_ids();
        let mut timed_out = Vec::new();
        for node_id in all_ids {
            let timeout_info = index.get_by_id(node_id).and_then(|n| {
                if n.state == NodeState::Live
                    && n.last_heartbeat_ms > 0
                    && now_ms.saturating_sub(n.last_heartbeat_ms) > timeout_ms
                {
                    Some(n.base.node_type)
                } else {
                    None
                }
            });
            if let Some(node_type) = timeout_info {
                index.update_state(node_id, NodeState::Lost);
                timed_out.push(node_id);
                self.emit_event(NodeEvent::StateChanged {
                    node_id,
                    node_type,
                    old_state: NodeState::Live,
                    new_state: NodeState::Lost,
                });
            }
        }
        timed_out
    }

    pub fn heartbeat_timeout_ms(&self) -> u64 {
        self.config_manager.get_u64(
            crate::pd::config::keys::PD_NODE_HEARTBEAT_TIMEOUT_MS,
            crate::pd::config::keys::PD_NODE_HEARTBEAT_TIMEOUT_MS_DEFAULT,
        )
    }

    fn persist_interval_ms(&self) -> u64 {
        self.config_manager.get_u64(
            crate::pd::config::keys::PD_NODE_PERSIST_INTERVAL_MS,
            crate::pd::config::keys::PD_NODE_PERSIST_INTERVAL_MS_DEFAULT,
        )
    }
}
