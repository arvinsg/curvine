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
use crate::pd::journal::entry::{BatchUpdateNodeStatusEntry, NodeStatusUpdate};
use crate::pd::journal::PdEntry;
use crate::pd::node::event::NodeEventType;
use curvine_common::state::{NodeInfo, NodeState};
use log::{error, info, warn};
use orpc::common::LocalTime;
use orpc::runtime::{RpcRuntime, Runtime};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

impl NodeManager {
    /// Start the internal liveness detection loop.
    pub fn start_liveness_loop(self: Arc<Self>, runtime: Arc<Runtime>, token: CancellationToken) {
        let mgr = self.clone();
        runtime.spawn(async move {
            mgr.liveness_loop(token).await;
        });
    }

    async fn liveness_loop(&self, token: CancellationToken) {
        let start_ms = LocalTime::mills();
        loop {
            let check_interval = self.liveness_check_interval_ms();
            tokio::select! {
                _ = token.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(check_interval)) => {}
            }
            self.run_liveness_tick(LocalTime::mills(), start_ms);
        }
        info!("Liveness loop stopped");
    }

    fn run_liveness_tick(&self, now_ms: u64, start_ms: u64) {
        self.mark_timeout_nodes_lost(now_ms, start_ms);
        self.promote_expired_lost_nodes_to_offline(now_ms);
    }

    fn mark_timeout_nodes_lost(&self, now_ms: u64, start_ms: u64) {
        let timeout = self.heartbeat_timeout_ms();
        if now_ms.saturating_sub(start_ms) <= timeout {
            return;
        }
        for node_id in self.detect_heartbeat_timeout(now_ms, timeout) {
            warn!("Node {} marked Lost (heartbeat timeout)", node_id);
        }
    }

    /// Detect nodes that have exceeded heartbeat timeout.
    /// Proposes Starting/Live → Lost through Raft and returns successfully IDs.
    pub fn detect_heartbeat_timeout(&self, now_ms: u64, timeout_ms: u64) -> Vec<u32> {
        let timed_out = self.collect_timed_out_nodes(now_ms, timeout_ms);
        if timed_out.is_empty() {
            return Vec::new();
        }

        let updates = Self::lost_node_updates(&timed_out);
        if !self.propose_node_status_updates(now_ms, updates, "timeout BatchUpdateNodeStatus") {
            return Vec::new();
        }
        self.emit_transition_events(&timed_out, NodeState::Lost, NodeEventType::Lost, now_ms)
    }

    fn collect_timed_out_nodes(&self, now_ms: u64, timeout_ms: u64) -> Vec<NodeInfo> {
        let index = self.index.read().unwrap();
        index
            .all_node_ids()
            .into_iter()
            .filter_map(|node_id| index.get_by_id(node_id).cloned())
            .filter(|node| Self::is_node_timed_out(node, now_ms, timeout_ms))
            .collect()
    }

    fn is_node_timed_out(node: &NodeInfo, now_ms: u64, timeout_ms: u64) -> bool {
        matches!(node.state, NodeState::Starting | NodeState::Live)
            && node.last_heartbeat_ms > 0
            && now_ms.saturating_sub(node.last_heartbeat_ms) > timeout_ms
    }

    fn lost_node_updates(nodes: &[NodeInfo]) -> Vec<NodeStatusUpdate> {
        nodes
            .iter()
            .map(|node| NodeStatusUpdate {
                node_id: node.base.node_id,
                expected_epoch: node.epoch,
                expected_state: node.state,
                target_state: Some(NodeState::Lost),
                heartbeat_ms: None,
            })
            .collect()
    }

    fn offline_node_updates(nodes: &[NodeInfo]) -> Vec<NodeStatusUpdate> {
        nodes
            .iter()
            .map(|node| NodeStatusUpdate {
                node_id: node.base.node_id,
                expected_epoch: node.epoch,
                expected_state: NodeState::Lost,
                target_state: Some(NodeState::Offline),
                heartbeat_ms: None,
            })
            .collect()
    }

    fn propose_node_status_updates(
        &self,
        op_ms: u64,
        updates: Vec<NodeStatusUpdate>,
        context: &str,
    ) -> bool {
        let batch_size = updates.len();
        match self.journal_client.propose(PdEntry::BatchUpdateNodeStatus(
            BatchUpdateNodeStatusEntry { op_ms, updates },
        )) {
            Ok(outcome) if outcome.is_success() => true,
            Ok(outcome) => {
                warn!(
                    "{} skipped batch_size={}, outcome={:?}",
                    context, batch_size, outcome
                );
                false
            }
            Err(e) => {
                warn!(
                    "propose {} failed batch_size={}, err={}",
                    context, batch_size, e
                );
                false
            }
        }
    }

    fn emit_transition_events(
        &self,
        nodes: &[NodeInfo],
        new_state: NodeState,
        event_type: NodeEventType,
        now_ms: u64,
    ) -> Vec<u32> {
        nodes
            .iter()
            .filter_map(|node| {
                self.emit_fenced_state_event(
                    node.base.node_id,
                    node.epoch,
                    Some(node.state),
                    new_state,
                    event_type,
                    now_ms,
                )
                .then_some(node.base.node_id)
            })
            .collect()
    }

    fn promote_expired_lost_nodes_to_offline(&self, now_ms: u64) {
        let recovery_window = self.recovery_window_ms();
        let expired = self.expired_lost_nodes(now_ms, recovery_window);
        if expired.is_empty() {
            return;
        }

        if self.should_skip_offline_promotion(expired.len()) {
            warn!(
                "skip Lost->Offline promotion for {} nodes: offline promotion guard triggered",
                expired.len()
            );
            return;
        }

        let updates = Self::offline_node_updates(&expired);
        error!(
            "{} Lost nodes exceeded recovery window ({}ms), promoting to Offline",
            updates.len(),
            recovery_window
        );
        if self.propose_node_status_updates(now_ms, updates, "Lost->Offline BatchUpdateNodeStatus")
        {
            self.emit_transition_events(
                &expired,
                NodeState::Offline,
                NodeEventType::Offline,
                now_ms,
            );
        }
    }

    pub(super) fn should_skip_offline_promotion(&self, expired_count: usize) -> bool {
        let recoverable_count = self.recoverable_node_count();
        let min_nodes = self.offline_promotion_min_nodes();
        if recoverable_count < min_nodes || recoverable_count == 0 {
            return false;
        }

        let max_ratio_bps = self.offline_promotion_max_ratio_bps();
        (expired_count as u128) * 10_000 > (recoverable_count as u128) * (max_ratio_bps as u128)
    }

    fn recoverable_node_count(&self) -> usize {
        let index = self.index.read().unwrap();
        index
            .all_node_ids()
            .into_iter()
            .filter_map(|node_id| index.get_by_id(node_id))
            .filter(|node| {
                matches!(
                    node.state,
                    NodeState::Starting | NodeState::Live | NodeState::Lost
                )
            })
            .count()
    }

    fn expired_lost_nodes(&self, now_ms: u64, recovery_window_ms: u64) -> Vec<NodeInfo> {
        self.get_nodes_by_state(NodeState::Lost)
            .into_iter()
            .filter(|node| now_ms.saturating_sub(node.state_since_ms) > recovery_window_ms)
            .collect()
    }
}
