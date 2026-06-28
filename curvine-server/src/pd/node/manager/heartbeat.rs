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

use super::{HeartbeatPlan, NodeManager};
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

impl NodeManager {
    pub fn handle_heartbeat(&self, req: HeartbeatRequest) -> FsResult<HeartbeatResponse> {
        self.ensure_leader("heartbeat rejected: this PD node is not the raft leader")?;
        Pd::get_metrics()
            .heartbeat_total
            .with_label_values(&[req.node_type.as_str()])
            .inc();

        let plan = self.plan_heartbeat(&req)?;
        self.commit_heartbeat_plan(&plan)?;
        self.apply_runtime_heartbeat(&plan.planned)?;
        self.build_heartbeat_response(req.node_id, plan.planned)
    }

    fn plan_heartbeat(&self, req: &HeartbeatRequest) -> FsResult<HeartbeatPlan> {
        let now = LocalTime::mills();
        let snapshot = self.get_heartbeat_snapshot(req)?;
        let mut planned = snapshot.clone();
        let critical_changed = self
            .get_handler(req.node_type)?
            .process_heartbeat(&mut planned, req)?;
        planned.last_heartbeat_ms = now;

        let old_state = snapshot.state;
        let state_changed = matches!(old_state, NodeState::Starting | NodeState::Lost);
        let payload_patch = critical_changed
            .then(|| Self::node_payload_patch(&planned))
            .flatten();
        let need_checkpoint = !state_changed
            && !critical_changed
            && snapshot.need_persist(now, self.persist_interval_ms());

        Ok(HeartbeatPlan {
            now_ms: now,
            snapshot,
            planned,
            old_state,
            state_changed,
            need_checkpoint,
            payload_patch,
        })
    }

    fn commit_heartbeat_plan(&self, plan: &HeartbeatPlan) -> FsResult<()> {
        if plan.state_changed || plan.need_checkpoint {
            let applied = self.commit_heartbeat_status(plan)?;
            self.emit_heartbeat_resume_event_if_needed(plan, applied);
        }
        if let Some(patch) = &plan.payload_patch {
            self.commit_heartbeat_payload(plan, patch.clone())?;
        }
        Ok(())
    }

    fn commit_heartbeat_status(&self, plan: &HeartbeatPlan) -> FsResult<bool> {
        let outcome = self
            .journal_client
            .propose(PdEntry::UpdateNodeStatus(self.heartbeat_status_entry(plan)))?;
        let applied = matches!(outcome, ApplyOutcome::Applied);
        if plan.state_changed {
            Self::apply_outcome_to_result(outcome, "heartbeat_status", plan.snapshot.base.node_id)?;
        } else if !outcome.is_success() {
            warn!(
                "heartbeat status checkpoint skipped node_id={}, epoch={}, outcome={:?}",
                plan.snapshot.base.node_id, plan.snapshot.epoch, outcome
            );
        }
        Ok(applied)
    }

    fn heartbeat_status_entry(&self, plan: &HeartbeatPlan) -> UpdateNodeStatusEntry {
        UpdateNodeStatusEntry {
            op_ms: plan.now_ms,
            update: NodeStatusUpdate {
                node_id: plan.snapshot.base.node_id,
                expected_epoch: plan.snapshot.epoch,
                expected_state: plan.old_state,
                target_state: plan.state_changed.then_some(NodeState::Live),
                heartbeat_ms: Some(plan.now_ms),
            },
        }
    }

    fn commit_heartbeat_payload(
        &self,
        plan: &HeartbeatPlan,
        patch: NodePayloadPatch,
    ) -> FsResult<()> {
        let expected_state = if plan.state_changed {
            NodeState::Live
        } else {
            plan.old_state
        };
        match self
            .journal_client
            .propose(PdEntry::UpdateNodePayload(UpdateNodePayloadEntry {
                op_ms: plan.now_ms,
                node_id: plan.snapshot.base.node_id,
                expected_epoch: plan.snapshot.epoch,
                expected_state,
                patch,
            })) {
            Ok(outcome) if outcome.is_success() => {
                if matches!(outcome, ApplyOutcome::Applied) {
                    info!(
                        "Node {} heartbeat committed persistent payload update epoch {} state {:?}",
                        plan.snapshot.base.node_id, plan.snapshot.epoch, expected_state
                    );
                }
                Ok(())
            }
            Ok(outcome) => {
                warn!(
                    "heartbeat payload update skipped node_id={}, epoch={}, outcome={:?}",
                    plan.snapshot.base.node_id, plan.snapshot.epoch, outcome
                );
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn emit_heartbeat_resume_event_if_needed(&self, plan: &HeartbeatPlan, applied: bool) {
        if !applied || !plan.state_changed {
            return;
        }
        if self.emit_fenced_state_event(
            plan.snapshot.base.node_id,
            plan.snapshot.epoch,
            Some(plan.old_state),
            NodeState::Live,
            NodeEventType::HeartbeatResumed,
            plan.now_ms,
        ) {
            info!(
                "Node {} heartbeat resumed {:?} -> Live epoch {}",
                plan.snapshot.base.node_id, plan.old_state, plan.snapshot.epoch
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

    fn apply_runtime_heartbeat(&self, planned: &NodeInfo) -> FsResult<()> {
        let mut index = self.index.write().unwrap();
        let Some(node) = index.get_by_id_mut(planned.base.node_id) else {
            return Err(FsError::common(format!(
                "node {} not found while applying runtime heartbeat",
                planned.base.node_id
            )));
        };
        if node.epoch != planned.epoch {
            return Err(FsError::common(format!(
                "epoch mismatch while applying runtime heartbeat for node {}: expected {} got {}",
                planned.base.node_id, node.epoch, planned.epoch
            )));
        }
        node.last_heartbeat_ms = node.last_heartbeat_ms.max(planned.last_heartbeat_ms);
        Self::preserve_runtime_fields(node, planned);
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

    fn node_payload_patch(node: &NodeInfo) -> Option<NodePayloadPatch> {
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
