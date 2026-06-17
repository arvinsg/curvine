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
use crate::pd::journal::entry::{
    BatchUpdateNodeStateEntry, DeleteNodeEntry, HeartbeatCheckpointEntry, NodeEntry,
    NodePayloadUpdate, UpdateNodeStateEntry,
};
use crate::pd::journal::{self, PdEntry};
use crate::pd::pd_server::Pd;
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, NodeInfo, NodePayload, NodeState, NodeType,
    RegisterRequest,
};
use curvine_common::{FsError, FsResult};

use log::{info, warn};
use orpc::common::LocalTime;
use orpc::runtime::RpcRuntime;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::broadcast;

use super::index::NodeIndex;
use super::store::NodeStore;

const EVENT_CHANNEL_CAPACITY: usize = 2048;
const MAX_BATCH_UPDATE_NODE_STATE: usize = 256;

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
        // #2: leader fence at RPC entry. register reads self.index but doesn't
        // mutate before propose, so leader fence here mainly avoids unnecessary
        // work on followers; propose_as_leader below would also reject.
        if !self.journal_client.is_leader() {
            return Err(FsError::not_leader(
                "register rejected: this PD node is not the raft leader",
            ));
        }
        if req.base.node_id == 0 {
            return Err(FsError::common("node_id must not be 0"));
        }
        if !Self::payload_matches_node_type(&req.payload, req.base.node_type) {
            return Err(FsError::common(format!(
                "register payload type mismatch for node_id={}, node_type={:?}",
                req.base.node_id, req.base.node_type
            )));
        }
        let handler = self.get_handler(req.base.node_type)?;
        let now = LocalTime::mills();

        let new_epoch = {
            let index = self.index.read().unwrap();
            if let Some(existing) = index.get_by_id(req.base.node_id) {
                let allow_replace = match existing.state {
                    NodeState::Lost | NodeState::Offline => true,
                    NodeState::Starting | NodeState::Live => {
                        req.base.startup_time_ms > existing.base.startup_time_ms
                    }
                    NodeState::Decommission | NodeState::Blacklist => false,
                };
                if !allow_replace {
                    return Err(FsError::common(format!(
                        "node {} already registered and state {:?}, startup_time_ms current={} requested={}",
                        req.base.node_id,
                        existing.state,
                        existing.base.startup_time_ms,
                        req.base.startup_time_ms
                    )));
                }
                if existing.base.address != req.base.address {
                    info!(
                        "Node {} address changed: {:?} -> {:?}, re-registering",
                        req.base.node_id, existing.base.address, req.base.address
                    );
                }
                existing.epoch + 1
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
            .propose_as_leader(PdEntry::RegisterNode(entry))?;

        let Some(applied) = self.get_node(node.base.node_id) else {
            warn!(
                "RegisterNode propose returned but node {} is missing; skip event",
                node.base.node_id
            );
            return Err(FsError::common(format!(
                "register node {} was not applied",
                node.base.node_id
            )));
        };
        if !Self::register_apply_matches(&applied, &node, new_epoch) {
            warn!(
                "RegisterNode stale/skip node_id={}, expected_epoch={}, current_epoch={}, current_state={:?}, expected_address={:?}, current_address={:?}; skip event",
                node.base.node_id,
                new_epoch,
                applied.epoch,
                applied.state,
                node.base.address,
                applied.base.address
            );
            return Err(FsError::common(format!(
                "register node {} was skipped by CAS or overwritten by another register",
                node.base.node_id
            )));
        }

        info!(
            "Registered node {} type {:?} epoch {} state {:?}",
            node.base.node_id, node.base.node_type, node.epoch, node.state
        );
        self.emit_event(NodeEvent {
            event_type: NodeEventType::Registered,
            node_id: node.base.node_id,
            node_type: node.base.node_type,
            old_state: None,
            new_state: Some(node.state),
            epoch: node.epoch,
            event_time_ms: now,
        });

        Ok((applied, new_epoch))
    }

    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        // #2 fix: leader fence at RPC entry. handle_heartbeat MUTATES the
        // in-memory NodeIndex (last_heartbeat_ms, payload fields) before
        // propose. Pre-#2 fix, a follower receiving heartbeat RPC would
        // silently mutate its local index, then propose_as_leader fails
        // — leaving runtime state diverged from the leader's. Reject early.
        if !self.journal_client.is_leader() {
            return Err(FsError::not_leader(
                "heartbeat rejected: this PD node is not the raft leader",
            ));
        }
        Pd::get_metrics()
            .heartbeat_total
            .with_label_values(&[req.node_type.as_str()])
            .inc();

        let handler = self.get_handler(req.node_type)?;
        let now = LocalTime::mills();
        let persist_interval = self.persist_interval_ms();

        let (
            node_snapshot,
            old_state,
            state_changed,
            critical_changed,
            need_checkpoint,
            payload_update,
        ) = {
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
            if node.base.node_type != req.node_type {
                return Err(FsError::common(format!(
                    "node_type mismatch for node {}: expected {:?} got {:?}",
                    req.node_id, node.base.node_type, req.node_type
                )));
            }
            if node.base.address != req.address {
                warn!(
                    "node {} endpoint changed in heartbeat: registered={:?}, reported={:?}; keep registered endpoint until raft endpoint update is implemented",
                    req.node_id,
                    node.base.address,
                    req.address
                );
            }

            match node.state {
                NodeState::Offline | NodeState::Blacklist | NodeState::Decommission => {
                    return Err(FsError::common(format!(
                        "node {} is {:?}, must re-register",
                        req.node_id, node.state
                    )));
                }
                _ => {}
            }

            let old_payload = node.payload.clone();
            let critical_changed = handler.process_heartbeat(node, &req)?;
            let payload_update = if critical_changed {
                let update = node.payload.clone();
                // Persistent payload fields (currently Meta group view) must be changed by apply only.
                // Keep runtime-only fields updated in memory, but roll persistent fields back until
                // the UpdateNodeState entry is committed and applied locally.
                Self::restore_persistent_payload_fields(&mut node.payload, &old_payload);
                Some(NodePayloadUpdate::Replace(update))
            } else {
                None
            };
            node.last_heartbeat_ms = now;

            let old_state = node.state;
            let state_changed = matches!(old_state, NodeState::Starting | NodeState::Lost);
            let need_checkpoint =
                !state_changed && !critical_changed && node.need_persist(now, persist_interval);
            let snapshot = node.clone();
            (
                snapshot,
                old_state,
                state_changed,
                critical_changed,
                need_checkpoint,
                payload_update,
            )
        };

        if state_changed || critical_changed {
            let new_state = if state_changed {
                NodeState::Live
            } else {
                old_state
            };
            let state_since_ms = if state_changed {
                now
            } else {
                node_snapshot.state_since_ms
            };
            let entry = UpdateNodeStateEntry {
                op_ms: now,
                node_id: node_snapshot.base.node_id,
                expected_epoch: node_snapshot.epoch,
                expected_state: Some(old_state),
                new_state,
                state_since_ms,
                last_heartbeat_ms: Some(now),
                payload_update,
            };
            self.journal_client
                .propose_as_leader(PdEntry::UpdateNodeState(entry))?;
            if state_changed
                && self.emit_state_event_if_current(
                    node_snapshot.base.node_id,
                    node_snapshot.epoch,
                    Some(old_state),
                    NodeState::Live,
                    NodeEventType::HeartbeatResumed,
                    now,
                )
            {
                info!(
                    "Node {} heartbeat resumed {:?} -> Live epoch {}",
                    node_snapshot.base.node_id, old_state, node_snapshot.epoch
                );
            } else if critical_changed {
                info!(
                    "Node {} heartbeat committed critical payload update epoch {} state {:?}",
                    node_snapshot.base.node_id, node_snapshot.epoch, old_state
                );
            }
        } else if need_checkpoint {
            let entry = HeartbeatCheckpointEntry {
                op_ms: now,
                node_id: node_snapshot.base.node_id,
                expected_epoch: node_snapshot.epoch,
                expected_state: Some(node_snapshot.state),
                last_heartbeat_ms: now,
            };
            self.journal_client
                .propose_as_leader(PdEntry::HeartbeatCheckpoint(entry))?;
        }

        let latest = self.get_node(req.node_id).unwrap_or(node_snapshot.clone());
        let response_payload = handler.build_heartbeat_response(&latest, &req)?;

        Ok(HeartbeatResponse {
            error: None,
            epoch: latest.epoch,
            mount_version: 0,
            table_epochs: Default::default(),
            payload: response_payload,
        })
    }

    /// Apply RegisterNode entry from Raft.
    pub fn apply_register_node(&self, entry: &NodeEntry) -> FsResult<()> {
        let mut node = entry.info.clone();
        node.last_persist_ms = entry.op_ms;

        let mut index = self.index.write().unwrap();
        if node.base.node_id == 0 {
            warn!("RegisterNode with invalid node_id=0; skip");
            return Ok(());
        }
        if !Self::payload_matches_node_type(&node.payload, node.base.node_type) {
            warn!(
                "RegisterNode payload type mismatch node_id={}, node_type={:?}; skip",
                node.base.node_id, node.base.node_type
            );
            return Ok(());
        }
        if let Some(existing) = index.get_by_id(node.base.node_id) {
            if node.epoch != existing.epoch.saturating_add(1) {
                warn!(
                    "stale/non-contiguous RegisterNode node_id={}, entry_epoch={}, current_epoch={}, current_state={:?}; skip",
                    node.base.node_id, node.epoch, existing.epoch, existing.state
                );
                return Ok(());
            }
            let allow_replace = match existing.state {
                NodeState::Lost | NodeState::Offline => true,
                NodeState::Starting | NodeState::Live => {
                    node.base.startup_time_ms > existing.base.startup_time_ms
                }
                NodeState::Decommission | NodeState::Blacklist => false,
            };
            if !allow_replace {
                warn!(
                    "RegisterNode rejected by current state/incarnation node_id={}, entry_epoch={}, current_epoch={}, current_state={:?}, current_startup_time_ms={}, entry_startup_time_ms={}; skip",
                    node.base.node_id,
                    node.epoch,
                    existing.epoch,
                    existing.state,
                    existing.base.startup_time_ms,
                    node.base.startup_time_ms
                );
                return Ok(());
            }
        } else if node.epoch == 0 {
            warn!(
                "RegisterNode for missing node with invalid epoch=0 node_id={}; skip",
                node.base.node_id
            );
            return Ok(());
        } else if node.epoch > 1 {
            warn!(
                "RegisterNode for missing node with epoch>1 node_id={}, entry_epoch={}; accept for Raft replay/snapshot recovery",
                node.base.node_id, node.epoch
            );
        }

        self.store.put(&node)?;
        index.insert(node.clone());
        info!(
            "RegisterNode applied node_id={}, epoch={}, state={:?}",
            node.base.node_id, node.epoch, node.state
        );
        Ok(())
    }

    /// Apply legacy SaveNode entry from Raft — persists full NodeInfo.
    pub fn apply_save_node(&self, entry: &NodeEntry) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(existing) = index.get_by_id(entry.info.base.node_id).cloned() else {
            warn!(
                "legacy SaveNode for unknown/deleted node_id={}, epoch={}, state={:?}; skip",
                entry.info.base.node_id, entry.info.epoch, entry.info.state
            );
            return Ok(());
        };
        if entry.info.epoch <= existing.epoch {
            warn!(
                "legacy SaveNode stale/non-newer node_id={}, entry_epoch={}, current_epoch={}; skip",
                entry.info.base.node_id, entry.info.epoch, existing.epoch
            );
            return Ok(());
        }

        let mut updated = entry.info.clone();
        updated.preserve_memory_fields(&existing);
        updated.last_persist_ms = entry.op_ms;
        self.store.put(&updated)?;
        index.insert(updated.clone());
        info!(
            "legacy SaveNode applied node_id={}, epoch={}, state={:?}",
            updated.base.node_id, updated.epoch, updated.state
        );
        Ok(())
    }

    pub fn apply_update_node_state(&self, entry: &UpdateNodeStateEntry) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(existing) = index.get_by_id(entry.node_id).cloned() else {
            warn!(
                "UpdateNodeState for unknown node_id={}, expected_epoch={}, target_state={:?}; skip",
                entry.node_id, entry.expected_epoch, entry.new_state
            );
            return Ok(());
        };
        if existing.epoch != entry.expected_epoch {
            warn!(
                "stale UpdateNodeState epoch node_id={}, expected_epoch={}, current_epoch={}, current_state={:?}, target_state={:?}; skip",
                entry.node_id, entry.expected_epoch, existing.epoch, existing.state, entry.new_state
            );
            return Ok(());
        }
        if let Some(expected_state) = entry.expected_state {
            if existing.state != expected_state {
                warn!(
                    "stale UpdateNodeState state node_id={}, expected_state={:?}, current_state={:?}, epoch={}, target_state={:?}; skip",
                    entry.node_id, expected_state, existing.state, existing.epoch, entry.new_state
                );
                return Ok(());
            }
        }
        if !Self::is_valid_state_transition(existing.state, entry.new_state) {
            warn!(
                "invalid UpdateNodeState transition node_id={}, current_state={:?}, target_state={:?}, epoch={}; skip",
                entry.node_id, existing.state, entry.new_state, existing.epoch
            );
            return Ok(());
        }

        let mut updated = existing.clone();
        updated.state = entry.new_state;
        updated.state_since_ms = entry.state_since_ms;
        if let Some(ms) = entry.last_heartbeat_ms {
            updated.last_heartbeat_ms = ms;
        }
        if let Some(NodePayloadUpdate::Replace(payload)) = &entry.payload_update {
            if !Self::payload_matches_node_type(payload, existing.base.node_type) {
                warn!(
                    "UpdateNodeState payload type mismatch node_id={}, node_type={:?}; skip",
                    entry.node_id, existing.base.node_type
                );
                return Ok(());
            }
            updated.payload = payload.clone();
            Self::preserve_runtime_fields(&mut updated, &existing);
        }
        updated.last_persist_ms = entry.op_ms;
        self.store.put(&updated)?;
        index.insert(updated.clone());
        info!(
            "UpdateNodeState applied node_id={}, epoch={}, {:?}->{:?}",
            entry.node_id, entry.expected_epoch, existing.state, entry.new_state
        );
        Ok(())
    }

    pub fn apply_batch_update_node_state(&self, entry: &BatchUpdateNodeStateEntry) -> FsResult<()> {
        for update in &entry.entries {
            self.apply_update_node_state(update)?;
        }
        info!(
            "BatchUpdateNodeState applied entries={}",
            entry.entries.len()
        );
        Ok(())
    }

    pub fn apply_heartbeat_checkpoint(&self, entry: &HeartbeatCheckpointEntry) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(existing) = index.get_by_id(entry.node_id).cloned() else {
            warn!(
                "HeartbeatCheckpoint for unknown node_id={}, expected_epoch={}; skip",
                entry.node_id, entry.expected_epoch
            );
            return Ok(());
        };
        if existing.epoch != entry.expected_epoch {
            warn!(
                "stale HeartbeatCheckpoint node_id={}, expected_epoch={}, current_epoch={}; skip",
                entry.node_id, entry.expected_epoch, existing.epoch
            );
            return Ok(());
        }
        if let Some(expected_state) = entry.expected_state {
            if existing.state != expected_state {
                warn!(
                    "stale HeartbeatCheckpoint state node_id={}, expected_state={:?}, current_state={:?}, epoch={}; skip",
                    entry.node_id, expected_state, existing.state, existing.epoch
                );
                return Ok(());
            }
        }
        if !matches!(existing.state, NodeState::Starting | NodeState::Live) {
            warn!(
                "HeartbeatCheckpoint for non-live node_id={}, state={:?}, epoch={}; skip",
                entry.node_id, existing.state, existing.epoch
            );
            return Ok(());
        }
        let mut updated = existing.clone();
        updated.last_heartbeat_ms = entry.last_heartbeat_ms;
        updated.last_persist_ms = entry.op_ms;
        self.store.put(&updated)?;
        index.insert(updated);
        Ok(())
    }

    /// Persist the current heartbeat timestamp checkpoint via Raft.
    pub fn persist_node(&self, node_id: u32) -> FsResult<()> {
        let node = {
            let index = self.index.read().unwrap();
            index.get_by_id(node_id).cloned()
        };
        let Some(node) = node else { return Ok(()) };
        if !matches!(node.state, NodeState::Starting | NodeState::Live) {
            warn!(
                "skip HeartbeatCheckpoint for non-live node_id={}, state={:?}, epoch={}",
                node_id, node.state, node.epoch
            );
            return Ok(());
        }
        let now = LocalTime::mills();
        self.journal_client
            .propose_as_leader(PdEntry::HeartbeatCheckpoint(HeartbeatCheckpointEntry {
                op_ms: now,
                node_id,
                expected_epoch: node.epoch,
                expected_state: Some(node.state),
                last_heartbeat_ms: node.last_heartbeat_ms,
            }))
    }

    /// Start decommissioning a node.
    pub fn start_decommission(&self, node_id: u32) -> FsResult<NodeState> {
        let node = self
            .get_node(node_id)
            .ok_or_else(|| FsError::common(format!("node {} not found", node_id)))?;

        if node.state == NodeState::Decommission {
            return Ok(NodeState::Decommission);
        }
        if matches!(node.state, NodeState::Blacklist) {
            return Err(FsError::common(format!(
                "node {} is {:?}, cannot decommission",
                node_id, node.state
            )));
        }

        let now = LocalTime::mills();
        self.journal_client
            .propose_as_leader(PdEntry::UpdateNodeState(UpdateNodeStateEntry {
                op_ms: now,
                node_id,
                expected_epoch: node.epoch,
                expected_state: Some(node.state),
                new_state: NodeState::Decommission,
                state_since_ms: now,
                last_heartbeat_ms: None,
                payload_update: None,
            }))?;

        if self.emit_state_event_if_current(
            node_id,
            node.epoch,
            Some(node.state),
            NodeState::Decommission,
            NodeEventType::DecommissionStarted,
            now,
        ) {
            info!(
                "Node {} decommission started {:?} -> Decommission epoch {}",
                node_id, node.state, node.epoch
            );
        }
        Ok(NodeState::Decommission)
    }

    /// Deprecated: strong semantic state changes must go through Raft.
    #[cfg(test)]
    pub fn update_state(&self, node_id: u32, new_state: NodeState) {
        let mut index = self.index.write().unwrap();
        index.update_state(node_id, new_state);
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
    /// Proposes Starting/Live → Lost through Raft and returns successfully transitioned IDs.
    pub fn detect_heartbeat_timeout(&self, now_ms: u64, timeout_ms: u64) -> Vec<u32> {
        let timed_out: Vec<NodeInfo> = {
            let index = self.index.read().unwrap();
            index
                .all_node_ids()
                .into_iter()
                .filter_map(|node_id| index.get_by_id(node_id).cloned())
                .filter(|n| {
                    matches!(n.state, NodeState::Starting | NodeState::Live)
                        && n.last_heartbeat_ms > 0
                        && now_ms.saturating_sub(n.last_heartbeat_ms) > timeout_ms
                })
                .collect()
        };

        let mut changed = Vec::new();
        for chunk in timed_out.chunks(MAX_BATCH_UPDATE_NODE_STATE) {
            let entries: Vec<UpdateNodeStateEntry> = chunk
                .iter()
                .map(|node| UpdateNodeStateEntry {
                    op_ms: now_ms,
                    node_id: node.base.node_id,
                    expected_epoch: node.epoch,
                    expected_state: Some(node.state),
                    new_state: NodeState::Lost,
                    state_since_ms: now_ms,
                    last_heartbeat_ms: None,
                    payload_update: None,
                })
                .collect();
            if let Err(e) = self
                .journal_client
                .propose_as_leader(PdEntry::BatchUpdateNodeState(BatchUpdateNodeStateEntry {
                    op_ms: now_ms,
                    entries,
                }))
            {
                warn!(
                    "propose timeout BatchUpdateNodeState failed batch_size={}, err={}",
                    chunk.len(),
                    e
                );
                continue;
            }
            for node in chunk {
                if self.emit_state_event_if_current(
                    node.base.node_id,
                    node.epoch,
                    Some(node.state),
                    NodeState::Lost,
                    NodeEventType::Lost,
                    now_ms,
                ) {
                    changed.push(node.base.node_id);
                }
            }
        }
        changed
    }

    fn heartbeat_timeout_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_HEARTBEAT_TIMEOUT_MS)
    }

    fn persist_interval_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_PERSIST_INTERVAL_MS)
    }

    fn liveness_check_interval_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_LIVENESS_CHECK_INTERVAL_MS)
    }

    fn recovery_window_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS)
    }

    /// Start the internal liveness detection loop.
    pub fn start_liveness_loop(
        self: Arc<Self>,
        runtime: Arc<orpc::runtime::Runtime>,
        token: tokio_util::sync::CancellationToken,
    ) {
        let mgr = self.clone();
        runtime.spawn(async move {
            mgr.liveness_loop(token).await;
        });
    }

    async fn liveness_loop(&self, token: tokio_util::sync::CancellationToken) {
        let leader_start_ms = LocalTime::mills();
        loop {
            let check_interval = self.liveness_check_interval_ms();
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(check_interval)) => {}
            }

            let now = LocalTime::mills();

            // Grace period after leader switch; persisted heartbeat checkpoints may lag.
            let timeout = self.heartbeat_timeout_ms();
            if now.saturating_sub(leader_start_ms) > timeout {
                for &node_id in &self.detect_heartbeat_timeout(now, timeout) {
                    log::warn!("Node {} marked Lost (heartbeat timeout)", node_id);
                }
            }

            let recovery_window = self.recovery_window_ms();
            let to_offline: Vec<NodeInfo> = self
                .get_nodes_by_state(NodeState::Lost)
                .into_iter()
                .filter(|n| now.saturating_sub(n.state_since_ms) > recovery_window)
                .collect();

            for chunk in to_offline.chunks(MAX_BATCH_UPDATE_NODE_STATE) {
                let entries: Vec<UpdateNodeStateEntry> = chunk
                    .iter()
                    .map(|node| UpdateNodeStateEntry {
                        op_ms: now,
                        node_id: node.base.node_id,
                        expected_epoch: node.epoch,
                        expected_state: Some(NodeState::Lost),
                        new_state: NodeState::Offline,
                        state_since_ms: now,
                        last_heartbeat_ms: None,
                        payload_update: None,
                    })
                    .collect();
                log::error!(
                    "{} Lost nodes exceeded recovery window ({}ms), promoting to Offline",
                    entries.len(),
                    recovery_window
                );
                if let Err(e) =
                    self.journal_client
                        .propose_as_leader(PdEntry::BatchUpdateNodeState(
                            BatchUpdateNodeStateEntry {
                                op_ms: now,
                                entries,
                            },
                        ))
                {
                    warn!(
                        "propose Lost->Offline BatchUpdateNodeState failed batch_size={}, err={}",
                        chunk.len(),
                        e
                    );
                    continue;
                }
                for node in chunk {
                    self.emit_state_event_if_current(
                        node.base.node_id,
                        node.epoch,
                        Some(NodeState::Lost),
                        NodeState::Offline,
                        NodeEventType::Offline,
                        now,
                    );
                }
            }
        }
        log::info!("Liveness loop stopped");
    }

    fn register_apply_matches(
        applied: &NodeInfo,
        expected: &NodeInfo,
        expected_epoch: u64,
    ) -> bool {
        applied.epoch == expected_epoch
            && applied.state == expected.state
            && applied.base.node_id == expected.base.node_id
            && applied.base.node_type == expected.base.node_type
            && applied.base.address == expected.base.address
            && applied.base.startup_time_ms == expected.base.startup_time_ms
    }

    fn payload_matches_node_type(payload: &NodePayload, node_type: NodeType) -> bool {
        matches!(
            (payload, node_type),
            (NodePayload::Worker(_), NodeType::Worker) | (NodePayload::Meta(_), NodeType::Meta)
        )
    }

    fn preserve_runtime_fields(target: &mut NodeInfo, source: &NodeInfo) {
        target.sys_stats = source.sys_stats.clone();
        match (&mut target.payload, &source.payload) {
            (NodePayload::Worker(ref mut dst), NodePayload::Worker(ref src)) => {
                dst.storage_stats = src.storage_stats.clone();
                dst.bg_epochs = src.bg_epochs.clone();
                dst.bg_reports = src.bg_reports.clone();
            }
            (NodePayload::Meta(ref mut dst), NodePayload::Meta(ref src)) => {
                dst.stats = src.stats.clone();
            }
            _ => {}
        }
    }

    fn restore_persistent_payload_fields(current: &mut NodePayload, persisted: &NodePayload) {
        match (current, persisted) {
            (NodePayload::Worker(cur), NodePayload::Worker(old)) => {
                cur.storage_specs = old.storage_specs.clone();
            }
            (NodePayload::Meta(cur), NodePayload::Meta(old)) => {
                cur.group_id = old.group_id;
                cur.peers = old.peers.clone();
                cur.rw_policy = old.rw_policy;
                cur.group_epoch = old.group_epoch;
            }
            (cur, old) => {
                *cur = old.clone();
            }
        }
    }

    fn is_valid_state_transition(from: NodeState, to: NodeState) -> bool {
        if from == to {
            return true;
        }
        matches!(
            (from, to),
            (NodeState::Starting, NodeState::Live)
                | (NodeState::Starting, NodeState::Lost)
                | (NodeState::Live, NodeState::Lost)
                | (NodeState::Lost, NodeState::Live)
                | (NodeState::Lost, NodeState::Offline)
                | (NodeState::Starting, NodeState::Decommission)
                | (NodeState::Live, NodeState::Decommission)
                | (NodeState::Lost, NodeState::Decommission)
                | (NodeState::Offline, NodeState::Decommission)
                | (NodeState::Starting, NodeState::Blacklist)
                | (NodeState::Live, NodeState::Blacklist)
                | (NodeState::Lost, NodeState::Blacklist)
                | (NodeState::Offline, NodeState::Blacklist)
                | (NodeState::Decommission, NodeState::Blacklist)
        )
    }

    fn emit_state_event_if_current(
        &self,
        node_id: u32,
        expected_epoch: u64,
        old_state: Option<NodeState>,
        new_state: NodeState,
        event_type: NodeEventType,
        event_time_ms: u64,
    ) -> bool {
        let Some(current) = self.get_node(node_id) else {
            warn!(
                "skip {:?} event for missing node_id={}, expected_epoch={}, target_state={:?}",
                event_type, node_id, expected_epoch, new_state
            );
            return false;
        };
        if current.epoch != expected_epoch || current.state != new_state {
            warn!(
                "skip {:?} event due to fencing node_id={}, expected_epoch={}, current_epoch={}, current_state={:?}, target_state={:?}",
                event_type, node_id, expected_epoch, current.epoch, current.state, new_state
            );
            return false;
        }
        self.emit_event(NodeEvent {
            event_type,
            node_id,
            node_type: current.base.node_type,
            old_state,
            new_state: Some(new_state),
            epoch: current.epoch,
            event_time_ms,
        });
        true
    }

    /// Finish decommission: propose DeleteNode via Raft and emit after apply.
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

        let now = LocalTime::mills();
        log::info!(
            "Node {} decommission complete, deleting from cluster",
            node_id
        );

        self.journal_client
            .propose_as_leader(PdEntry::DeleteNode(DeleteNodeEntry {
                op_ms: now,
                node_id,
                expected_epoch: node.epoch,
                expected_state: Some(NodeState::Decommission),
            }))?;

        if self.get_node(node_id).is_none() {
            self.emit_event(NodeEvent {
                event_type: NodeEventType::DecommissionFinished,
                node_id,
                node_type: node.base.node_type,
                old_state: Some(NodeState::Decommission),
                new_state: None,
                epoch: node.epoch,
                event_time_ms: now,
            });
            info!(
                "Node {} decommission finished epoch {}",
                node_id, node.epoch
            );
        } else {
            warn!(
                "DeleteNode propose returned but node still exists node_id={}, epoch={}; skip DecommissionFinished event",
                node_id, node.epoch
            );
        }
        Ok(())
    }

    /// Apply DeleteNode entry from Raft — remove from store and in-memory index.
    pub fn apply_delete_node(&self, entry: &DeleteNodeEntry) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(existing) = index.get_by_id(entry.node_id).cloned() else {
            warn!(
                "DeleteNode for missing node_id={}, expected_epoch={}; skip",
                entry.node_id, entry.expected_epoch
            );
            return Ok(());
        };
        if existing.epoch != entry.expected_epoch {
            warn!(
                "stale DeleteNode epoch node_id={}, expected_epoch={}, current_epoch={}; skip",
                entry.node_id, entry.expected_epoch, existing.epoch
            );
            return Ok(());
        }
        if let Some(expected_state) = entry.expected_state {
            if existing.state != expected_state {
                warn!(
                    "DeleteNode rejected node_id={}, expected_state={:?}, current_state={:?}, epoch={}; skip",
                    entry.node_id, expected_state, existing.state, existing.epoch
                );
                return Ok(());
            }
        }
        self.store.delete(entry.node_id)?;
        index.remove(entry.node_id);
        info!(
            "DeleteNode applied node_id={}, epoch={}, old_state={:?}",
            entry.node_id, existing.epoch, existing.state
        );
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
        crate::pd::pd_server::init_metrics_for_test();
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
            state_since_ms: orpc::common::LocalTime::mills(),
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
    fn apply_register_node_allows_lost_without_newer_startup_time() {
        let mgr = test_manager();
        let mut existing = make_node(1, NodeType::Worker, NodeState::Lost);
        existing.base.startup_time_ms = 100;
        insert_node(&mgr, &existing);

        let mut replacement = make_node(1, NodeType::Worker, NodeState::Starting);
        replacement.epoch = 2;
        replacement.base.startup_time_ms = 100;

        mgr.apply_register_node(&NodeEntry {
            op_ms: 200,
            info: replacement.clone(),
        })
        .expect("apply register");

        let applied = mgr.get_node(1).expect("node exists");
        assert_eq!(applied.epoch, 2);
        assert_eq!(applied.state, NodeState::Starting);
        assert_eq!(applied.base.startup_time_ms, 100);
    }

    #[test]
    fn apply_register_node_rejects_starting_with_equal_startup_time() {
        let mgr = test_manager();
        let mut existing = make_node(1, NodeType::Worker, NodeState::Starting);
        existing.base.startup_time_ms = 100;
        insert_node(&mgr, &existing);

        let mut stale_replacement = make_node(1, NodeType::Worker, NodeState::Starting);
        stale_replacement.epoch = 2;
        stale_replacement.base.startup_time_ms = 100;

        mgr.apply_register_node(&NodeEntry {
            op_ms: 200,
            info: stale_replacement,
        })
        .expect("apply register should skip equal startup_time for Starting");

        let current = mgr.get_node(1).expect("node exists");
        assert_eq!(current.epoch, 1);
        assert_eq!(current.state, NodeState::Starting);
    }

    #[test]
    fn apply_register_node_rejects_live_without_newer_startup_time() {
        let mgr = test_manager();
        let mut existing = make_node(1, NodeType::Worker, NodeState::Live);
        existing.base.startup_time_ms = 100;
        insert_node(&mgr, &existing);

        let mut stale_replacement = make_node(1, NodeType::Worker, NodeState::Starting);
        stale_replacement.epoch = 2;
        stale_replacement.base.startup_time_ms = 100;

        mgr.apply_register_node(&NodeEntry {
            op_ms: 200,
            info: stale_replacement,
        })
        .expect("apply register should skip stale incarnation");

        let current = mgr.get_node(1).expect("node exists");
        assert_eq!(current.epoch, 1);
        assert_eq!(current.state, NodeState::Live);
    }

    #[test]
    fn apply_register_node_rejects_invalid_identity() {
        let mgr = test_manager();

        let mut invalid_id = make_node(0, NodeType::Worker, NodeState::Starting);
        invalid_id.epoch = 1;
        mgr.apply_register_node(&NodeEntry {
            op_ms: 100,
            info: invalid_id,
        })
        .expect("invalid id should be skipped");
        assert!(mgr.get_node(0).is_none());

        let mut mismatch = make_node(2, NodeType::Worker, NodeState::Starting);
        mismatch.payload = NodePayload::Meta(MetaNodePayload::default());
        mgr.apply_register_node(&NodeEntry {
            op_ms: 100,
            info: mismatch,
        })
        .expect("payload mismatch should be skipped");
        assert!(mgr.get_node(2).is_none());
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

        mgr.apply_update_node_state(&UpdateNodeStateEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            new_state: NodeState::Lost,
            state_since_ms: 20_000,
            last_heartbeat_ms: None,
            payload_update: None,
        })
        .unwrap();

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

        mgr.apply_update_node_state(&UpdateNodeStateEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            new_state: NodeState::Lost,
            state_since_ms: 20_000,
            last_heartbeat_ms: None,
            payload_update: None,
        })
        .unwrap();
        assert!(mgr.emit_state_event_if_current(
            1,
            1,
            Some(NodeState::Live),
            NodeState::Lost,
            NodeEventType::Lost,
            20_000,
        ));

        let event = rx.try_recv().expect("should have received an event");
        assert_eq!(event.event_type, NodeEventType::Lost);
        assert_eq!(event.node_id, 1);
        assert_eq!(event.node_type, NodeType::Worker);
        assert_eq!(event.old_state, Some(NodeState::Live));
        assert_eq!(event.new_state, Some(NodeState::Lost));
    }

    #[test]
    fn apply_update_node_state_rejects_stale_epoch() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Live);
        node.epoch = 2;
        insert_node(&mgr, &node);

        mgr.apply_update_node_state(&UpdateNodeStateEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            new_state: NodeState::Lost,
            state_since_ms: 20_000,
            last_heartbeat_ms: None,
            payload_update: None,
        })
        .unwrap();

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.epoch, 2);
        assert_eq!(updated.state, NodeState::Live);
    }

    #[test]
    fn apply_heartbeat_checkpoint_rejects_stale_epoch() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Live);
        node.epoch = 2;
        node.last_heartbeat_ms = 10;
        insert_node(&mgr, &node);

        mgr.apply_heartbeat_checkpoint(&HeartbeatCheckpointEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            last_heartbeat_ms: 999,
        })
        .unwrap();

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.epoch, 2);
        assert_eq!(updated.last_heartbeat_ms, 10);
    }

    #[test]
    fn apply_delete_node_rejects_stale_epoch() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Worker, NodeState::Decommission);
        node.epoch = 2;
        insert_node(&mgr, &node);

        mgr.apply_delete_node(&DeleteNodeEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Decommission),
        })
        .unwrap();

        assert!(mgr.get_node(1).is_some());
    }

    #[test]
    fn apply_update_node_state_payload_update_preserves_meta_runtime_stats() {
        let mgr = test_manager();
        let mut node = make_node(1, NodeType::Meta, NodeState::Live);
        if let NodePayload::Meta(ref mut payload) = node.payload {
            payload.group_id = 10;
            payload.group_epoch = 1;
            payload.stats.inode_count = 42;
        }
        insert_node(&mgr, &node);

        let mut new_payload = match node.payload.clone() {
            NodePayload::Meta(payload) => payload,
            _ => unreachable!(),
        };
        new_payload.group_epoch = 2;
        new_payload.stats.inode_count = 0;

        mgr.apply_update_node_state(&UpdateNodeStateEntry {
            op_ms: 20_000,
            node_id: 1,
            expected_epoch: 1,
            expected_state: Some(NodeState::Live),
            new_state: NodeState::Live,
            state_since_ms: node.state_since_ms,
            last_heartbeat_ms: Some(20_000),
            payload_update: Some(NodePayloadUpdate::Replace(NodePayload::Meta(new_payload))),
        })
        .unwrap();

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.state, NodeState::Live);
        assert_eq!(updated.last_heartbeat_ms, 20_000);
        match updated.payload {
            NodePayload::Meta(payload) => {
                assert_eq!(payload.group_epoch, 2);
                assert_eq!(payload.stats.inode_count, 42);
            }
            _ => panic!("expected meta payload"),
        }
    }

    #[test]
    fn apply_register_allows_missing_meta_with_non_initial_epoch() {
        let mgr = test_manager();
        let mut node = make_node(3, NodeType::Meta, NodeState::Starting);
        node.epoch = 4;

        mgr.apply_register_node(&NodeEntry {
            op_ms: 20_000,
            info: node.clone(),
        })
        .unwrap();

        let updated = mgr.get_node(3).unwrap();
        assert_eq!(updated.base.node_id, 3);
        assert_eq!(updated.base.node_type, NodeType::Meta);
        assert_eq!(updated.epoch, 4);
        assert_eq!(updated.state, NodeState::Starting);
        assert_eq!(updated.last_persist_ms, 20_000);
    }

    #[test]
    fn apply_register_rejects_existing_decommission_node() {
        let mgr = test_manager();
        let existing = make_node(1, NodeType::Worker, NodeState::Decommission);
        insert_node(&mgr, &existing);

        let mut replacement = make_node(1, NodeType::Worker, NodeState::Starting);
        replacement.epoch = 2;
        mgr.apply_register_node(&NodeEntry {
            op_ms: 20_000,
            info: replacement,
        })
        .unwrap();

        let updated = mgr.get_node(1).unwrap();
        assert_eq!(updated.epoch, 1);
        assert_eq!(updated.state, NodeState::Decommission);
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
