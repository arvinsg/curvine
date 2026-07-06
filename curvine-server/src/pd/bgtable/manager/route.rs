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

use super::{BGTable, BGTableManager};
use curvine_common::state::{
    BGKind, BGTableSummary, BgId, BlockGroupInfo, BlockGroupRouteView, CapacityBGTableSummary,
    HashBGTableSummary, NodeState, ReplicaInfo, ReplicaState, TableId,
};

impl BGTableManager {
    pub fn build_table_summary(&self, table_id: TableId) -> Option<BGTableSummary> {
        let table = self.get_table(table_id)?;
        self.build_client_summary_for_table(&table)
    }

    fn build_client_summary_for_table(&self, table: &BGTable) -> Option<BGTableSummary> {
        let views = self.build_route_views(table)?;
        // Assemble the kind-specific client summary from the route views. This
        // is a pure projection of table metadata + views, so it lives here with
        // the rest of the route-building logic rather than on the control.
        Some(match table {
            BGTable::Hash(t) => BGTableSummary::Hash(HashBGTableSummary {
                table_id: t.base.table_id,
                epoch: t.base.epoch,
                cache_replica_policy: t.cache_replica_policy.clone(),
                buckets: views,
            }),
            BGTable::Capacity(t) => BGTableSummary::Capacity(CapacityBGTableSummary {
                table_id: t.base.table_id,
                epoch: t.base.epoch,
                active_bgs: views,
            }),
        })
    }

    /// Collect per-BG route views for a table. Hash tables require every bucket
    /// to resolve (a missing BG is an error → None); Capacity tables skip
    /// missing active BGs.
    fn build_route_views(&self, table: &BGTable) -> Option<Vec<BlockGroupRouteView>> {
        match table {
            BGTable::Hash(t) => {
                let mut views = Vec::with_capacity(t.buckets.len());
                for &bg_id in &t.buckets {
                    let Some(view) = self.build_bg_route_view(BGKind::Hash, bg_id) else {
                        log::warn!(
                            "build hash route for table_id={} failed: missing bg_id={}",
                            t.base.table_id,
                            bg_id
                        );
                        return None;
                    };
                    views.push(view);
                }
                Some(views)
            }
            BGTable::Capacity(t) => {
                let mut views = Vec::with_capacity(t.active_bgs.len());
                for &bg_id in &t.active_bgs {
                    let Some(view) = self.build_bg_route_view(BGKind::Capacity, bg_id) else {
                        log::warn!(
                            "build capacity route for table_id={} skipped missing active bg_id={}",
                            t.base.table_id,
                            bg_id
                        );
                        continue;
                    };
                    views.push(view);
                }
                Some(views)
            }
        }
    }

    fn build_bg_route_view(&self, kind: BGKind, bg_id: BgId) -> Option<BlockGroupRouteView> {
        let bg = self.bg_manager.get_bg(kind, bg_id)?;
        let serving: Vec<u32> = bg
            .isr
            .iter()
            .copied()
            .filter(|wid| bg.replica_set.contains(wid))
            .filter(|wid| self.is_serving_replica(&bg, *wid))
            .collect();
        Some(self.block_group_info_to_route_view(&bg, &serving))
    }

    fn block_group_info_to_route_view(
        &self,
        bg: &BlockGroupInfo,
        serving_ids: &[u32],
    ) -> BlockGroupRouteView {
        let serving_replicas: Vec<ReplicaInfo> = serving_ids
            .iter()
            .filter_map(|&node_id| {
                self.pool_manager
                    .get_worker_node(node_id)
                    .map(|node| ReplicaInfo {
                        node_id,
                        address: node.base.address,
                        state: node.state,
                        labels: node.base.labels,
                    })
            })
            .collect();
        BlockGroupRouteView {
            bg_id: bg.bg_id,
            table_id: bg.table_id,
            kind: bg.kind,
            bg_epoch: bg.bg_epoch,
            serving_replicas,
            state: bg.state,
            primary: bg.primary.clone(),
        }
    }

    fn is_serving_replica(&self, bg: &BlockGroupInfo, worker_id: u32) -> bool {
        if bg.replica_state(worker_id) != ReplicaState::Active {
            return false;
        }
        self.pool_manager
            .get_worker_node(worker_id)
            .map(|node| node.state == NodeState::Live)
            .unwrap_or(false)
    }
}
