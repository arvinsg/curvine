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

use super::NodeManager;
use crate::pd::journal::entry::{
    MetaNodePayloadPatch, NodePayloadPatch, NodeStatusUpdate, UpdateNodePayloadEntry,
    UpdateNodeStatusEntry,
};
use crate::pd::journal::{ApplyOutcome, PdEntry};
use crate::pd::node::event::NodeEventType;
use crate::pd::pd_server::Pd;
use curvine_common::state::{
    HeartbeatRequest, HeartbeatResponse, HeartbeatResponsePayload, MetaHeartbeatResponse, NodeInfo,
    NodePayload, NodeState, NodeType, TaskHeartbeatResponse, WorkerHeartbeatResponse,
};
use curvine_common::{FsError, FsResult};
use log::{info, warn};
use orpc::common::LocalTime;

struct HeartbeatUpdate {
    now_ms: u64,
    old: NodeInfo,
    updated: NodeInfo,
    state_changed: bool,
    need_checkpoint: bool,
    payload_patch: Option<NodePayloadPatch>,
}

impl NodeManager {
    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        Self::record_heartbeat_metric(&req);

        let update = self.build_heartbeat_update(&req)?;
        self.persist_heartbeat_update(&update)?;
        self.apply_runtime_heartbeat(&update.updated)?;
        self.build_heartbeat_response(req.node_id, update.updated)
    }

    fn record_heartbeat_metric(req: &HeartbeatRequest) {
        Pd::get_metrics()
            .heartbeat_total
            .with_label_values(&[req.node_type.as_str()])
            .inc();
    }

    fn build_heartbeat_update(&self, req: &HeartbeatRequest) -> FsResult<HeartbeatUpdate> {
        let now = LocalTime::mills();
        let old = self.get_heartbeat_snapshot(req)?;
        let mut updated = old.clone();
        let critical_changed = self
            .get_handler(req.node_type)?
            .process_heartbeat(&mut updated, req)?;
        updated.last_heartbeat_ms = now;

        let old_state = old.state;
        let state_changed = matches!(old_state, NodeState::Starting | NodeState::Lost);
        let payload_patch = critical_changed
            .then(|| Self::build_payload_patch(&updated))
            .flatten();
        let need_checkpoint = !state_changed
            && !critical_changed
            && old.need_persist(now, self.persist_interval_ms());

        Ok(HeartbeatUpdate {
            now_ms: now,
            old,
            updated,
            state_changed,
            need_checkpoint,
            payload_patch,
        })
    }

    fn persist_heartbeat_update(&self, update: &HeartbeatUpdate) -> FsResult<()> {
        if update.state_changed || update.need_checkpoint {
            let applied = self.persist_heartbeat_status(update)?;
            self.emit_resume_event(update, applied);
        }
        if let Some(patch) = &update.payload_patch {
            self.persist_heartbeat_payload(update, patch.clone())?;
        }
        Ok(())
    }

    fn persist_heartbeat_status(&self, update: &HeartbeatUpdate) -> FsResult<bool> {
        let outcome = self
            .journal_client
            .propose(PdEntry::UpdateNodeStatus(Self::build_status_entry(update)))?;
        let applied = matches!(outcome, ApplyOutcome::Applied);
        if update.state_changed {
            Self::apply_outcome_to_result(outcome, "heartbeat_status", update.old.base.node_id)?;
        } else if !outcome.is_success() {
            warn!(
                "heartbeat status checkpoint skipped node_id={}, epoch={}, outcome={:?}",
                update.old.base.node_id, update.old.epoch, outcome
            );
        }
        Ok(applied)
    }

    fn build_status_entry(update: &HeartbeatUpdate) -> UpdateNodeStatusEntry {
        UpdateNodeStatusEntry {
            op_ms: update.now_ms,
            update: NodeStatusUpdate {
                node_id: update.old.base.node_id,
                expected_epoch: update.old.epoch,
                expected_state: update.old.state,
                target_state: update.state_changed.then_some(NodeState::Live),
                heartbeat_ms: Some(update.now_ms),
            },
        }
    }

    fn persist_heartbeat_payload(
        &self,
        update: &HeartbeatUpdate,
        patch: NodePayloadPatch,
    ) -> FsResult<()> {
        let expected_state = if update.state_changed {
            NodeState::Live
        } else {
            update.old.state
        };
        match self
            .journal_client
            .propose(PdEntry::UpdateNodePayload(UpdateNodePayloadEntry {
                op_ms: update.now_ms,
                node_id: update.old.base.node_id,
                expected_epoch: update.old.epoch,
                expected_state,
                patch,
            })) {
            Ok(outcome) if outcome.is_success() => {
                if matches!(outcome, ApplyOutcome::Applied) {
                    info!(
                        "Node {} heartbeat committed persistent payload update epoch {} state {:?}",
                        update.old.base.node_id, update.old.epoch, expected_state
                    );
                }
                Ok(())
            }
            Ok(outcome) => {
                warn!(
                    "heartbeat payload update skipped node_id={}, epoch={}, outcome={:?}",
                    update.old.base.node_id, update.old.epoch, outcome
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn emit_resume_event(&self, update: &HeartbeatUpdate, applied: bool) {
        if !applied || !update.state_changed {
            return;
        }
        if self.emit_fenced_state_event(
            update.old.base.node_id,
            update.old.epoch,
            Some(update.old.state),
            NodeState::Live,
            NodeEventType::HeartbeatResumed,
            update.now_ms,
        ) {
            info!(
                "Node {} heartbeat resumed {:?} -> Live epoch {}",
                update.old.base.node_id, update.old.state, update.old.epoch
            );
        }
    }

    fn build_heartbeat_response(
        &self,
        node_id: u32,
        fallback: NodeInfo,
    ) -> FsResult<HeartbeatResponse> {
        let latest = self.get_node(node_id).unwrap_or(fallback);
        Ok(HeartbeatResponse {
            error: None,
            epoch: latest.epoch,
            mount_version: 0,
            table_epochs: Default::default(),
            simple_cluster_view_hint: Default::default(),
            payload: Self::default_heartbeat_response(latest.base.node_type),
        })
    }

    fn get_heartbeat_snapshot(&self, req: &HeartbeatRequest) -> FsResult<NodeInfo> {
        let index = self.index.read().unwrap();
        let node = index
            .get_by_id(req.node_id)
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
                req.node_id, node.base.address, req.address
            );
        }
        if matches!(
            node.state,
            NodeState::Offline | NodeState::Blacklist | NodeState::Decommission
        ) {
            return Err(FsError::common(format!(
                "node {} is {:?}, must re-register",
                req.node_id, node.state
            )));
        }

        Ok(node.clone())
    }

    fn apply_runtime_heartbeat(&self, updated: &NodeInfo) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(node) = index.get_by_id_mut(updated.base.node_id) else {
            return Err(FsError::common(format!(
                "node {} not found while applying runtime heartbeat",
                updated.base.node_id
            )));
        };
        if node.epoch != updated.epoch {
            return Err(FsError::common(format!(
                "epoch mismatch while applying runtime heartbeat for node {}: expected {} got {}",
                updated.base.node_id, node.epoch, updated.epoch
            )));
        }
        node.last_heartbeat_ms = node.last_heartbeat_ms.max(updated.last_heartbeat_ms);
        Self::preserve_runtime_fields(node, updated);
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
                "skip heartbeat status checkpoint for non-live node_id={}, state={:?}, epoch={}",
                node_id, node.state, node.epoch
            );
            return Ok(());
        }
        let now = LocalTime::mills();
        let outcome =
            self.journal_client
                .propose(PdEntry::UpdateNodeStatus(UpdateNodeStatusEntry {
                    op_ms: now,
                    update: NodeStatusUpdate {
                        node_id,
                        expected_epoch: node.epoch,
                        expected_state: node.state,
                        target_state: None,
                        heartbeat_ms: Some(node.last_heartbeat_ms),
                    },
                }))?;
        Self::apply_outcome_to_result(outcome, "heartbeat_status", node_id)
    }

    fn build_payload_patch(node: &NodeInfo) -> Option<NodePayloadPatch> {
        match &node.payload {
            NodePayload::Meta(payload) => Some(NodePayloadPatch::Meta(MetaNodePayloadPatch {
                group_id: payload.group_id,
                group_epoch: payload.group_epoch,
                peers: payload.peers.clone(),
                rw_policy: payload.rw_policy,
            })),
            _ => None,
        }
    }

    fn default_heartbeat_response(node_type: NodeType) -> HeartbeatResponsePayload {
        match node_type {
            NodeType::Worker => {
                HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse::default())
            }
            NodeType::Meta => HeartbeatResponsePayload::Meta(MetaHeartbeatResponse {
                path_route_update: None,
                node_group_update: None,
            }),
            NodeType::Task => HeartbeatResponsePayload::Task(TaskHeartbeatResponse::default()),
        }
    }
}
