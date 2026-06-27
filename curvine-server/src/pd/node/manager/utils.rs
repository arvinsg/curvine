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
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::{NodeInfo, NodePayload, NodeState, NodeType};
use curvine_common::{FsError, FsResult};

impl NodeManager {
    pub(super) fn ensure_leader(&self, message: &'static str) -> FsResult<()> {
        if self.journal_client.is_leader() {
            Ok(())
        } else {
            Err(FsError::not_leader(message))
        }
    }

    pub(super) fn heartbeat_timeout_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_HEARTBEAT_TIMEOUT_MS)
    }

    pub(super) fn persist_interval_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_PERSIST_INTERVAL_MS)
    }

    pub(super) fn liveness_check_interval_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_LIVENESS_CHECK_INTERVAL_MS)
    }

    pub(super) fn recovery_window_ms(&self) -> u64 {
        self.config_manager
            .get_u64(crate::pd::config::keys::PD_NODE_LOST_RECOVERY_WINDOW_MS)
    }

    pub(super) fn payload_matches_node_type(payload: &NodePayload, node_type: NodeType) -> bool {
        matches!(
            (payload, node_type),
            (NodePayload::Worker(_), NodeType::Worker)
                | (NodePayload::Meta(_), NodeType::Meta)
                | (NodePayload::Task(_), NodeType::Task)
        )
    }

    pub(super) fn preserve_runtime_fields(target: &mut NodeInfo, source: &NodeInfo) {
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
            (NodePayload::Task(ref mut dst), NodePayload::Task(ref src)) => {
                dst.stats = src.stats.clone();
            }
            _ => {}
        }
    }

    pub(super) fn apply_outcome_to_result(
        outcome: ApplyOutcome,
        op: &str,
        node_id: u32,
    ) -> FsResult<()> {
        match outcome {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                Err(FsError::stale_entry(op, node_id.to_string(), reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }

    // todo
    pub(super) fn same_node_incarnation(existing: &NodeInfo, node: &NodeInfo) -> bool {
        existing.epoch == node.epoch
            && existing.state == node.state
            && Self::same_node_base(&existing.base, &node.base)
            && Self::same_persistent_payload(&existing.payload, &node.payload)
    }

    // todo
    pub(super) fn same_node_base(
        left: &curvine_common::state::NodeBase,
        right: &curvine_common::state::NodeBase,
    ) -> bool {
        left.node_id == right.node_id
            && left.node_type == right.node_type
            && left.address == right.address
            && left.labels == right.labels
            && left.software_version == right.software_version
            && left.startup_time_ms == right.startup_time_ms
    }

    // todo
    pub(super) fn same_persistent_node_state(left: &NodeInfo, right: &NodeInfo) -> bool {
        Self::same_node_base(&left.base, &right.base)
            && left.epoch == right.epoch
            && left.state == right.state
            && left.state_since_ms == right.state_since_ms
            && left.last_heartbeat_ms == right.last_heartbeat_ms
            && Self::same_persistent_payload(&left.payload, &right.payload)
    }

    // todo
    pub(super) fn same_persistent_payload(left: &NodePayload, right: &NodePayload) -> bool {
        match (left, right) {
            (NodePayload::Worker(l), NodePayload::Worker(r)) => {
                l.storage_specs.len() == r.storage_specs.len()
                    && l.storage_specs.iter().all(|(id, left_spec)| {
                        r.storage_specs.get(id).is_some_and(|right_spec| {
                            left_spec.dir_id == right_spec.dir_id
                                && left_spec.storage_id == right_spec.storage_id
                                && left_spec.failed == right_spec.failed
                                && left_spec.storage_type == right_spec.storage_type
                                && left_spec.dir_path == right_spec.dir_path
                        })
                    })
            }
            (NodePayload::Meta(l), NodePayload::Meta(r)) => {
                l.group_id == r.group_id
                    && l.peers.len() == r.peers.len()
                    && l.peers.iter().zip(&r.peers).all(|(a, b)| {
                        a.node_id == b.node_id
                            && a.address == b.address
                            && a.is_leader == b.is_leader
                    })
                    && l.rw_policy == r.rw_policy
                    && l.group_epoch == r.group_epoch
            }
            (NodePayload::Task(_), NodePayload::Task(_)) => true,
            _ => false,
        }
    }

    pub(super) fn is_valid_state_transition(from: NodeState, to: NodeState) -> bool {
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
}
