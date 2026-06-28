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
use crate::pd::config::keys::{
    PD_NODE_HEARTBEAT_TIMEOUT_MS, PD_NODE_LIVENESS_CHECK_INTERVAL_MS,
    PD_NODE_LOST_RECOVERY_WINDOW_MS, PD_NODE_OFFLINE_PROMOTION_MAX_RATIO_BPS,
    PD_NODE_OFFLINE_PROMOTION_MIN_NODES, PD_NODE_PERSIST_INTERVAL_MS,
};
use crate::pd::journal::ApplyOutcome;
use curvine_common::state::{NodeInfo, NodePayload, NodeState, NodeType};
use curvine_common::{FsError, FsResult};

impl NodeManager {
    pub(super) fn heartbeat_timeout_ms(&self) -> u64 {
        self.config_manager.get_u64(PD_NODE_HEARTBEAT_TIMEOUT_MS)
    }

    pub(super) fn persist_interval_ms(&self) -> u64 {
        self.config_manager.get_u64(PD_NODE_PERSIST_INTERVAL_MS)
    }

    pub(super) fn liveness_check_interval_ms(&self) -> u64 {
        self.config_manager
            .get_u64(PD_NODE_LIVENESS_CHECK_INTERVAL_MS)
    }

    pub(super) fn recovery_window_ms(&self) -> u64 {
        self.config_manager.get_u64(PD_NODE_LOST_RECOVERY_WINDOW_MS)
    }

    pub(super) fn offline_promotion_min_nodes(&self) -> usize {
        self.config_manager
            .get_u32(PD_NODE_OFFLINE_PROMOTION_MIN_NODES) as usize
    }

    pub(super) fn offline_promotion_max_ratio_bps(&self) -> u64 {
        self.config_manager
            .get_u64(PD_NODE_OFFLINE_PROMOTION_MAX_RATIO_BPS)
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

    #[cfg(test)]
    pub fn test_insert_node(&self, node: NodeInfo) {
        let mut index = self.index.write().unwrap();
        index.insert(node);
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
