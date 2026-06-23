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

use crate::pd::bg::BGManager;
use crate::pd::node::NodeManager;
use crate::pd::pool::PoolManager;
use crate::pd::schedule::OperatorController;
use curvine_common::state::{BGState, NodePayload, NodeState, NodeType};
use orpc::common::{CounterVec, Gauge, GaugeVec, HistogramVec, Metrics as m, Metrics};
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct PdMetrics {
    node_manager: Arc<NodeManager>,
    pool_manager: Arc<PoolManager>,
    bg_manager: Arc<BGManager>,
    operator_controller: Option<Arc<OperatorController>>,

    // ---- Node ----
    pub(crate) node_count: GaugeVec,
    pub(crate) heartbeat_total: CounterVec,
    pub(crate) node_event_total: CounterVec,

    // ---- Meta ----
    pub(crate) meta_inode_count: GaugeVec,
    pub(crate) meta_dir_count: GaugeVec,
    pub(crate) meta_file_count: GaugeVec,
    pub(crate) meta_total_size_bytes: GaugeVec,

    // ---- Pool ----
    pub(crate) pool_capacity_bytes: GaugeVec,
    pub(crate) pool_available_bytes: GaugeVec,
    pub(crate) pool_used_bytes: GaugeVec,
    pub(crate) pool_worker_count: GaugeVec,
    pub(crate) pool_block_count: GaugeVec,

    // ---- BG ----
    pub(crate) bg_total: Gauge,
    pub(crate) bg_count: GaugeVec,
    pub(crate) bg_used_bytes: Gauge,
    pub(crate) bg_block_count: Gauge,

    // ---- BGTable ----
    pub(crate) bg_table_count: Gauge,
    pub(crate) bg_table_bucket_count: GaugeVec,
    pub(crate) bg_table_used_bytes: GaugeVec,
    pub(crate) bg_table_available_bytes: GaugeVec,
    pub(crate) bg_table_block_count: GaugeVec,

    // ---- Operator ----
    pub(crate) operator_running: Gauge,
    pub(crate) operator_waiting: Gauge,
    pub(crate) operator_finish_total: CounterVec,

    // ---- Raft ----
    pub(crate) raft_apply_total: CounterVec,

    // ---- RPC ----
    pub(crate) rpc_request_total: CounterVec,
    pub(crate) rpc_request_duration: HistogramVec,

    // ---- System ----
    pub(crate) used_memory_bytes: Gauge,
}

impl PdMetrics {
    pub fn new(
        node_manager: Arc<NodeManager>,
        pool_manager: Arc<PoolManager>,
        bg_manager: Arc<BGManager>,
    ) -> CommonResult<Self> {
        let buckets = vec![
            10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0,
        ];
        Ok(Self {
            node_manager,
            pool_manager,
            bg_manager,
            operator_controller: None,

            // Node
            node_count: m::new_gauge_vec(
                "pd_node_count",
                "Number of nodes by type and state",
                &["type", "state"],
            )?,
            heartbeat_total: m::new_counter_vec(
                "pd_heartbeat_total",
                "Total heartbeat requests",
                &["node_type"],
            )?,
            node_event_total: m::new_counter_vec(
                "pd_node_event_total",
                "Total node events",
                &["event"],
            )?,

            // Meta
            meta_inode_count: m::new_gauge_vec(
                "pd_meta_inode_count",
                "Meta node inode count",
                &["node_id", "group_id"],
            )?,
            meta_dir_count: m::new_gauge_vec(
                "pd_meta_dir_count",
                "Meta node directory count",
                &["node_id", "group_id"],
            )?,
            meta_file_count: m::new_gauge_vec(
                "pd_meta_file_count",
                "Meta node file count",
                &["node_id", "group_id"],
            )?,
            meta_total_size_bytes: m::new_gauge_vec(
                "pd_meta_total_size_bytes",
                "Meta node total metadata size in bytes",
                &["node_id", "group_id"],
            )?,

            // Pool
            pool_capacity_bytes: m::new_gauge_vec(
                "pd_pool_capacity_bytes",
                "Pool total capacity in bytes",
                &["pool_type"],
            )?,
            pool_available_bytes: m::new_gauge_vec(
                "pd_pool_available_bytes",
                "Pool available space in bytes",
                &["pool_type"],
            )?,
            pool_used_bytes: m::new_gauge_vec(
                "pd_pool_used_bytes",
                "Pool used space in bytes",
                &["pool_type"],
            )?,
            pool_worker_count: m::new_gauge_vec(
                "pd_pool_worker_count",
                "Number of workers in pool",
                &["pool_type"],
            )?,
            pool_block_count: m::new_gauge_vec(
                "pd_pool_block_count",
                "Total block count in pool",
                &["pool_type"],
            )?,

            // BG
            bg_total: m::new_gauge("pd_bg_total", "Total number of block groups")?,
            bg_count: m::new_gauge_vec(
                "pd_bg_count",
                "Number of block groups by state",
                &["state"],
            )?,
            bg_used_bytes: m::new_gauge(
                "pd_bg_used_bytes",
                "Total used bytes across all block groups",
            )?,
            bg_block_count: m::new_gauge(
                "pd_bg_block_count",
                "Total block count across all block groups",
            )?,

            // BGTable
            bg_table_count: m::new_gauge("pd_bg_table_count", "Total number of BG tables")?,
            bg_table_bucket_count: m::new_gauge_vec(
                "pd_bg_table_bucket_count",
                "Number of buckets in BG table",
                &["table_id"],
            )?,
            bg_table_used_bytes: m::new_gauge_vec(
                "pd_bg_table_used_bytes",
                "Used bytes in BG table",
                &["table_id"],
            )?,
            bg_table_available_bytes: m::new_gauge_vec(
                "pd_bg_table_available_bytes",
                "Available bytes in BG table",
                &["table_id"],
            )?,
            bg_table_block_count: m::new_gauge_vec(
                "pd_bg_table_block_count",
                "Block count in BG table",
                &["table_id"],
            )?,

            // Operator
            operator_running: m::new_gauge("pd_operator_running", "Number of running operators")?,
            operator_waiting: m::new_gauge("pd_operator_waiting", "Number of waiting operators")?,
            operator_finish_total: m::new_counter_vec(
                "pd_operator_finish_total",
                "Total finished operators by result",
                &["result"],
            )?,

            // Raft
            raft_apply_total: m::new_counter_vec(
                "pd_raft_apply_total",
                "Total raft apply entries by type",
                &["entry_type"],
            )?,

            // RPC
            rpc_request_total: m::new_counter_vec(
                "pd_rpc_request_total",
                "Total RPC requests by operation",
                &["operation"],
            )?,
            rpc_request_duration: m::new_histogram_vec_with_buckets(
                "pd_rpc_request_duration_ms",
                "RPC request duration in milliseconds",
                &["operation"],
                &buckets,
            )?,

            // System
            used_memory_bytes: m::new_gauge("pd_used_memory_bytes", "Total memory used by PD")?,
        })
    }

    pub fn set_operator_controller(&mut self, oc: Arc<OperatorController>) {
        self.operator_controller = Some(oc);
    }

    pub fn text_output(&self) -> CommonResult<String> {
        self.snapshot_node_gauges();
        self.snapshot_meta_gauges();
        self.snapshot_pool_gauges();
        self.snapshot_bg_gauges();
        self.snapshot_bg_table_gauges();
        self.snapshot_operator_gauges();
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        Metrics::text_output()
    }

    fn snapshot_node_gauges(&self) {
        let node_types = [NodeType::Worker, NodeType::Meta, NodeType::Task];

        for nt in &node_types {
            let nodes = self.node_manager.get_nodes_by_type(*nt);
            for ns in &NodeState::ALL {
                let count = nodes.iter().filter(|n| n.state == *ns).count();
                self.node_count
                    .with_label_values(&[nt.as_str(), ns.as_str()])
                    .set(count as i64);
            }
        }
    }

    fn snapshot_meta_gauges(&self) {
        let meta_nodes = self.node_manager.get_nodes_by_type(NodeType::Meta);
        for node in &meta_nodes {
            if let NodePayload::Meta(ref payload) = node.payload {
                let node_id = node.base.node_id.to_string();
                let group_id = payload.group_id.to_string();
                let labels = [node_id.as_str(), group_id.as_str()];
                self.meta_inode_count
                    .with_label_values(&labels)
                    .set(payload.stats.inode_count as i64);
                self.meta_dir_count
                    .with_label_values(&labels)
                    .set(payload.stats.dir_count as i64);
                self.meta_file_count
                    .with_label_values(&labels)
                    .set(payload.stats.file_count as i64);
                self.meta_total_size_bytes
                    .with_label_values(&labels)
                    .set(payload.stats.total_size as i64);
            }
        }
    }

    fn snapshot_pool_gauges(&self) {
        let pools = self.pool_manager.list_active_pools();
        for pool in &pools {
            let pid = pool.pool_type.to_string();
            self.pool_capacity_bytes
                .with_label_values(&[&pid])
                .set(pool.stats.capacity_bytes as i64);
            self.pool_available_bytes
                .with_label_values(&[&pid])
                .set(pool.stats.available_bytes as i64);
            self.pool_used_bytes
                .with_label_values(&[&pid])
                .set(pool.stats.used_bytes as i64);
            self.pool_worker_count
                .with_label_values(&[&pid])
                .set(self.pool_manager.get_workers_in_pool(pool.pool_type).len() as i64);
            self.pool_block_count
                .with_label_values(&[&pid])
                .set(pool.stats.block_count as i64);
        }
    }

    fn snapshot_bg_gauges(&self) {
        let bgs = self.bg_manager.list_bgs();
        self.bg_total.set(bgs.len() as i64);

        for state in &BGState::ALL {
            let count = bgs.iter().filter(|bg| bg.state == *state).count();
            self.bg_count
                .with_label_values(&[state.as_str()])
                .set(count as i64);
        }

        let mut total_used: u64 = 0;
        let mut total_blocks: u64 = 0;
        for bg in &bgs {
            total_used += bg.stats.used_bytes;
            total_blocks += bg.stats.block_count;
        }
        self.bg_used_bytes.set(total_used as i64);
        self.bg_block_count.set(total_blocks as i64);
    }

    fn snapshot_bg_table_gauges(&self) {
        let tables = self.bg_manager.list_tables();
        self.bg_table_count.set(tables.len() as i64);

        for table in &tables {
            let tid = table.table_id.to_string();
            self.bg_table_bucket_count
                .with_label_values(&[&tid])
                .set(table.bucket_count as i64);

            let stats = self.bg_manager.get_table_stats(table.table_id);
            self.bg_table_used_bytes
                .with_label_values(&[&tid])
                .set(stats.used_bytes as i64);
            self.bg_table_available_bytes
                .with_label_values(&[&tid])
                .set(stats.free_bytes as i64);
            self.bg_table_block_count
                .with_label_values(&[&tid])
                .set(stats.block_count as i64);
        }
    }

    fn snapshot_operator_gauges(&self) {
        if let Some(ref oc) = self.operator_controller {
            self.operator_running.set(oc.running_count() as i64);
            self.operator_waiting.set(oc.waiting_count() as i64);
        }
    }
}

impl Debug for PdMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PdMetrics")
    }
}
