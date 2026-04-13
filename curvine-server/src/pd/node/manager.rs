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

use super::event::{NodeEvent, NodeEventType};
use super::{HandlerRegistry, HeartbeatHandler, MetaHeartbeatHandler, WorkerHeartbeatHandler};
use crate::pd::config::ConfigManager;
use crate::pd::journal::entry::NodeEntry;
use crate::pd::journal::{self, PdEntry};
use crate::pd::pd_server::Pd;
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, NodeInfo, NodeState, NodeType, RegisterRequest,
};
use curvine_common::{FsError, FsResult};
use dashmap::DashMap;
use log::info;
use orpc::common::LocalTime;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::broadcast;

use super::index::NodeIndex;
use super::store::NodeStore;

const EVENT_CHANNEL_CAPACITY: usize = 2048;

pub struct NodeManager {
    index: Arc<RwLock<NodeIndex>>,
    store: Arc<NodeStore>,
    handler_registry: HandlerRegistry,
    config_manager: Arc<ConfigManager>,
    journal_client: Arc<journal::Client>,
    event_tx: broadcast::Sender<NodeEvent>,
    /// Tracks when each node entered Lost state (node_id -> lost_time_ms)
    lost_since: Arc<DashMap<u32, u64>>,
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
            lost_since: Arc::new(DashMap::new()),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<NodeEvent> {
        self.event_tx.subscribe()
    }

    fn emit_event(&self, event: NodeEvent) {
        Pd::get_metrics()
            .node_event_total
            .with_label_values(&[event.event_type.as_str()])
            .inc();
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
        self.journal_client.propose(PdEntry::RegisterNode(entry))?;

        self.emit_event(NodeEvent {
            event_type: NodeEventType::Registered,
            node_id: node.base.node_id,
            node_type: node.base.node_type,
            old_state: None,
            new_state: Some(node.state),
            epoch: node.epoch,
            event_time_ms: now,
        });

        Ok((node, new_epoch))
    }

    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        Pd::get_metrics()
            .heartbeat_total
            .with_label_values(&[req.node_type.as_str()])
            .inc();

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

            // Reject heartbeats from terminal/decommissioning states
            match node.state {
                NodeState::Offline | NodeState::Blacklist | NodeState::Decommission => {
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

            let need_persist = critical_changed || node.need_persist(now, persist_interval);

            let node_id = req.node_id;
            let node_type = node.base.node_type;
            if state_changed {
                index.update_state(node_id, NodeState::Live);
            }

            let snapshot = index.get_by_id(node_id).unwrap().clone();
            if state_changed {
                let event_type = match old_state {
                    NodeState::Starting => NodeEventType::HeartbeatResumed,
                    NodeState::Lost => NodeEventType::HeartbeatResumed,
                    _ => NodeEventType::HeartbeatResumed,
                };
                self.emit_event(NodeEvent {
                    event_type,
                    node_id,
                    node_type,
                    old_state: Some(old_state),
                    new_state: Some(NodeState::Live),
                    epoch: snapshot.epoch,
                    event_time_ms: now,
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
            mount_version: 0,
            table_epochs: Default::default(),
            payload: response_payload,
        })
    }

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
        let old_state = index.get_by_id(entry.info.base.node_id).map(|n| n.state);
        if let Some(existing) = index.get_by_id(entry.info.base.node_id) {
            updated.preserve_memory_fields(existing);
        }
        updated.last_persist_ms = entry.op_ms;
        index.insert(updated);
        drop(index);

        // Emit event on state change
        let new_state = entry.info.state;
        if old_state.is_some() && old_state != Some(new_state) {
            let event_type = match new_state {
                NodeState::Offline => NodeEventType::Offline,
                NodeState::Decommission => NodeEventType::DecommissionStarted,
                NodeState::Live => NodeEventType::HeartbeatResumed,
                NodeState::Lost => NodeEventType::Lost,
                _ => return Ok(()),
            };
            self.emit_event(NodeEvent {
                event_type,
                node_id: entry.info.base.node_id,
                node_type: entry.info.base.node_type,
                old_state,
                new_state: Some(new_state),
                epoch: entry.info.epoch,
                event_time_ms: entry.op_ms,
            });
        }
        Ok(())
    }

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
        self.journal_client.propose(PdEntry::SaveNode(entry))
    }

    /// Start decommissioning a node.
    pub fn start_decommission(&self, node_id: u32) -> FsResult<NodeState> {
        let node = self
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;

        let mut updated = node.clone();
        updated.state = NodeState::Decommission;
        let now = LocalTime::mills();
        self.journal_client.propose(PdEntry::SaveNode(NodeEntry {
            op_ms: now,
            info: updated,
        }))?;
        Ok(NodeState::Decommission)
    }

    /// Update state in-memory and emit the corresponding event. Does NOT persist.
    pub fn update_state(&self, node_id: u32, new_state: NodeState) {
        let old_info = {
            let mut index = self.index.write().unwrap();
            let old_info = index
                .get_by_id(node_id)
                .map(|n| (n.state, n.base.node_type, n.epoch));
            index.update_state(node_id, new_state);
            old_info
        };
        if let Some((old_state, node_type, epoch)) = old_info {
            if old_state != new_state {
                let event_type = match new_state {
                    NodeState::Offline => NodeEventType::Offline,
                    NodeState::Decommission => NodeEventType::DecommissionStarted,
                    NodeState::Live => NodeEventType::HeartbeatResumed,
                    NodeState::Lost => NodeEventType::Lost,
                    _ => return,
                };
                self.emit_event(NodeEvent {
                    event_type,
                    node_id,
                    node_type,
                    old_state: Some(old_state),
                    new_state: Some(new_state),
                    epoch,
                    event_time_ms: orpc::common::LocalTime::mills(),
                });
            }
        }
    }

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
        let mut timeout_nodes = Vec::new();
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
                timeout_nodes.push(node_id);
                let epoch = index.get_by_id(node_id).map(|n| n.epoch).unwrap_or(0);
                self.emit_event(NodeEvent {
                    event_type: NodeEventType::Lost,
                    node_id,
                    node_type,
                    old_state: Some(NodeState::Live),
                    new_state: Some(NodeState::Lost),
                    epoch,
                    event_time_ms: now_ms,
                });
            }
        }
        timeout_nodes
    }

    fn heartbeat_timeout_ms(&self) -> u64 {
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

    fn liveness_check_interval_ms(&self) -> u64 {
        self.config_manager.get_u64(
            crate::pd::config::keys::PD_NODE_LIVENESS_CHECK_INTERVAL_MS,
            crate::pd::config::keys::PD_NODE_LIVENESS_CHECK_INTERVAL_MS_DEFAULT,
        )
    }

    fn recovery_window_ms(&self) -> u64 {
        self.config_manager.get_u64(
            crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS,
            crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS_DEFAULT,
        )
    }

    // TODO: 是否使用统一的 async
    // ========== Internal liveness loop ==========

    /// Start the internal liveness detection loop.
    /// This loop handles:
    /// - Live→Lost detection (heartbeat timeout, memory-only)
    /// - Lost→Offline promotion (recovery window exceeded, persisted via Raft)
    pub fn start_liveness_loop(self: Arc<Self>) {
        let mgr = self.clone();
        tokio::spawn(async move {
            mgr.liveness_loop().await;
        });
    }

    async fn liveness_loop(&self) {
        loop {
            let check_interval = self.liveness_check_interval_ms();
            tokio::time::sleep(std::time::Duration::from_millis(check_interval)).await;

            let now = LocalTime::mills();

            // 1. Detect heartbeat timeouts: Live→Lost
            let timeout = self.heartbeat_timeout_ms();
            let newly_lost = self.detect_heartbeat_timeout(now, timeout);
            for &node_id in &newly_lost {
                self.lost_since.insert(node_id, now);
                log::warn!("Node {} marked Lost (heartbeat timeout)", node_id);
            }

            // 2. Promote Lost→Offline after recovery window
            let recovery_window = self.recovery_window_ms();
            let to_offline: Vec<u32> = self
                .lost_since
                .iter()
                .filter(|entry| now.saturating_sub(*entry.value()) > recovery_window)
                .map(|entry| *entry.key())
                .collect();

            for node_id in to_offline {
                log::error!(
                    "Node {} exceeded recovery window ({}ms), promoting to Offline",
                    node_id,
                    recovery_window
                );
                self.update_state(node_id, NodeState::Offline);
                self.lost_since.remove(&node_id);
            }

            // 3. Cleanup recovered nodes (no longer Lost)
            let recovered: Vec<u32> = self
                .lost_since
                .iter()
                .filter(|entry| {
                    self.get_node(*entry.key())
                        .map(|n| n.state != NodeState::Lost)
                        .unwrap_or(true)
                })
                .map(|entry| *entry.key())
                .collect();

            for node_id in recovered {
                self.lost_since.remove(&node_id);
                log::info!("Node {} recovered from Lost state", node_id);
            }
        }
    }

    /// Finish decommission: emit event and propose DeleteNode via Raft.
    pub fn finish_decommission(&self, node_id: u32) -> FsResult<()> {
        let node = self
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;

        if node.state != NodeState::Decommission {
            return Err(FsError::common(format!(
                "node {} is {:?}, expected Decommission",
                node_id, node.state
            )));
        }

        log::info!(
            "Node {} decommission complete, deleting from cluster",
            node_id
        );

        self.emit_event(NodeEvent {
            event_type: NodeEventType::DecommissionFinished,
            node_id,
            node_type: node.base.node_type,
            old_state: Some(NodeState::Decommission),
            new_state: None,
            epoch: node.epoch,
            event_time_ms: LocalTime::mills(),
        });

        self.journal_client.propose(PdEntry::DeleteNode(node_id))
    }

    /// Apply DeleteNode entry from Raft — remove from store and in-memory index.
    pub fn apply_delete_node(&self, node_id: u32) -> FsResult<()> {
        self.store.delete(node_id)?;
        let mut index = self.index.write().unwrap();
        index.remove(node_id);
        Ok(())
    }
}

#[cfg(test)]
impl NodeManager {
    /// Insert a node directly into the in-memory index, bypassing store serialization.
    pub fn test_insert_node(&self, node: NodeInfo) {
        let mut index = self.index.write().unwrap();
        index.insert(node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_common::state::{
        MetaNodePayload, NodeAddress, NodeBase, NodeInfo, NodePayload, NodeState, NodeType,
        WorkerNodePayload,
    };
    use std::sync::Arc;

    fn test_store() -> Arc<dyn crate::pd::store::KvStore> {
        Arc::new(crate::pd::store::memory_kv_engine::MemoryKvEngine::new())
    }

    fn test_manager_with_store(store: Arc<dyn crate::pd::store::KvStore>) -> NodeManager {
        let node_store = Arc::new(super::super::store::NodeStore::new(store.clone()));
        let raft = curvine_common::raft::RaftClient::from_conf(
            curvine_common::conf::JournalConf::default().create_runtime(),
            &curvine_common::conf::JournalConf::default(),
        );
        let jc = Arc::new(crate::pd::journal::Client::new(raft));
        let config = Arc::new(crate::pd::config::ConfigManager::new(
            store,
            jc.clone(),
            std::collections::HashMap::new(),
        ));
        NodeManager::new(node_store, config, jc)
    }

    fn test_manager() -> NodeManager {
        test_manager_with_store(test_store())
    }

    fn make_node(id: u32, node_type: NodeType, state: NodeState) -> NodeInfo {
        let payload = match node_type {
            NodeType::Worker => NodePayload::Worker(WorkerNodePayload::default()),
            NodeType::Meta => NodePayload::Meta(MetaNodePayload::default()),
        };
        NodeInfo {
            base: NodeBase {
                node_id: id,
                node_type,
                address: NodeAddress {
                    hostname: format!("host-{}", id),
                    ip: format!("10.0.0.{}", id),
                    rpc_port: 8000 + id as u16,
                    web_port: 9000 + id as u16,
                },
                ..Default::default()
            },
            state,
            epoch: 1,
            last_heartbeat_ms: orpc::common::LocalTime::mills(),
            last_persist_ms: 0,
            sys_stats: Default::default(),
            payload,
        }
    }

    /// Insert a node directly into the in-memory index, bypassing store serialization.
    fn insert_node(mgr: &NodeManager, node: &NodeInfo) {
        let mut index = mgr.index.write().unwrap();
        index.insert(node.clone());
    }

    /// Insert a node into both the in-memory index and the underlying KvStore.
    /// Writes serialized bytes directly to the KvStore using the same key layout
    /// as NodeStore, so that NodeStore::list_all can later deserialize them.
    fn insert_node_persisted(
        kv: &Arc<dyn crate::pd::store::KvStore>,
        mgr: &NodeManager,
        node: &NodeInfo,
    ) {
        let bytes = curvine_common::utils::SerdeUtils::serialize(node).expect("serialize NodeInfo");
        let mut key = [0u8; 5];
        key[0] = 0x20;
        key[1..5].copy_from_slice(&node.base.node_id.to_be_bytes());
        kv.put("meta", &key, &bytes).expect("KvStore put");
        let mut index = mgr.index.write().unwrap();
        index.insert(node.clone());
    }

    #[test]
    fn apply_register_and_get_node() {
        let mgr = test_manager();
        let node = make_node(1, NodeType::Worker, NodeState::Starting);
        insert_node(&mgr, &node);

        let fetched = mgr.get_node(1).expect("node should exist");
        assert_eq!(fetched.base.node_id, 1);
        assert_eq!(fetched.state, NodeState::Starting);
        assert_eq!(fetched.base.node_type, NodeType::Worker);
    }

    #[test]
    fn get_nodes_by_type() {
        let mgr = test_manager();
        let w1 = make_node(1, NodeType::Worker, NodeState::Live);
        let w2 = make_node(2, NodeType::Worker, NodeState::Live);
        let m1 = make_node(3, NodeType::Meta, NodeState::Live);
        insert_node(&mgr, &w1);
        insert_node(&mgr, &w2);
        insert_node(&mgr, &m1);

        let workers = mgr.get_nodes_by_type(NodeType::Worker);
        assert_eq!(workers.len(), 2);
        assert!(workers.iter().all(|n| n.base.node_type == NodeType::Worker));

        let metas = mgr.get_nodes_by_type(NodeType::Meta);
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].base.node_id, 3);
    }

    #[test]
    fn get_nodes_by_state() {
        let mgr = test_manager();
        let n1 = make_node(1, NodeType::Worker, NodeState::Live);
        let n2 = make_node(2, NodeType::Worker, NodeState::Lost);
        let n3 = make_node(3, NodeType::Worker, NodeState::Live);
        insert_node(&mgr, &n1);
        insert_node(&mgr, &n2);
        insert_node(&mgr, &n3);

        let live = mgr.get_nodes_by_state(NodeState::Live);
        assert_eq!(live.len(), 2);
        assert!(live.iter().all(|n| n.state == NodeState::Live));

        let lost = mgr.get_nodes_by_state(NodeState::Lost);
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].base.node_id, 2);
    }

    #[test]
    fn detect_heartbeat_timeout_marks_lost() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Live);
        node.last_heartbeat_ms = 1000;
        insert_node(&mgr, &node);

        let timed_out = mgr.detect_heartbeat_timeout(20_000, 5_000);
        assert_eq!(timed_out, vec![1]);

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.state, NodeState::Lost);
    }

    #[test]
    fn detect_heartbeat_timeout_ignores_non_live() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Lost);
        node.last_heartbeat_ms = 1000;
        insert_node(&mgr, &node);

        let timed_out = mgr.detect_heartbeat_timeout(20_000, 5_000);
        assert!(timed_out.is_empty());

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.state, NodeState::Lost);
    }

    #[test]
    fn update_state_and_events() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Live);
        node.last_heartbeat_ms = 1000;
        insert_node(&mgr, &node);

        // Subscribe before the state change so we capture the event.
        let mut rx = mgr.subscribe();

        // detect_heartbeat_timeout transitions Live -> Lost and emits an event
        let timed_out = mgr.detect_heartbeat_timeout(20_000, 5_000);
        assert_eq!(timed_out, vec![1]);

        let event = rx.try_recv().expect("should have received an event");
        assert_eq!(event.event_type, NodeEventType::Lost);
        assert_eq!(event.node_id, 1);
        assert_eq!(event.node_type, NodeType::Worker);
        assert_eq!(event.old_state, Some(NodeState::Live));
        assert_eq!(event.new_state, Some(NodeState::Lost));
    }

    #[test]
    fn restore_loads_from_store() {
        let kv = test_store();

        // Persist a node to the KvStore via the first manager
        let mgr1 = test_manager_with_store(kv.clone());
        let node = make_node(1, NodeType::Worker, NodeState::Live);
        insert_node_persisted(&kv, &mgr1, &node);
        drop(mgr1);

        // Create a fresh manager with the same backing store
        let mgr2 = test_manager_with_store(kv);
        assert!(mgr2.get_node(1).is_none(), "should be empty before restore");

        mgr2.restore().unwrap();

        let restored = mgr2.get_node(1).expect("node should be restored");
        assert_eq!(restored.base.node_id, 1);
        assert_eq!(restored.base.node_type, NodeType::Worker);
    }
}
