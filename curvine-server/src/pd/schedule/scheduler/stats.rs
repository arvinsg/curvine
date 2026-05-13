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

use super::Scheduler;
use crate::pd::schedule::{BGOperator, ManagerContext};
use curvine_common::state::{BGStats, NodePayload, NodeType};
use std::collections::HashMap;
use std::time::Duration;

/// Periodically refresh stats: rolls worker `bg_reports` into `BGManager` (BG-level)
/// and then aggregates them into per-table stats cached on `BGTable.stats`.
/// Produces no operators.
pub struct StatsScheduler;

impl StatsScheduler {
    pub fn new() -> Self {
        Self
    }
}

impl Default for StatsScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler for StatsScheduler {
    fn name(&self) -> &str {
        "stats-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        ctx.pool_manager.refresh_pool_stats();

        for node in ctx.node_manager.get_nodes_by_type(NodeType::Worker) {
            let NodePayload::Worker(ref payload) = node.payload else {
                continue;
            };
            if payload.bg_reports.is_empty() {
                continue;
            }
            let stats_map: HashMap<u32, BGStats> = payload
                .bg_reports
                .iter()
                .map(|r| (r.bg_id, r.stats.clone()))
                .collect();
            ctx.bg_manager.update_bg_stats(&stats_map);
        }

        // Aggregate BG-level stats into each BGTable.stats.
        ctx.bg_manager.refresh_table_stats();

        if let Err(e) = ctx.bg_manager.retry_dirty_route_publish() {
            log::warn!(
                "stats scheduler failed to retry dirty BG route publish: {}",
                e
            );
        }

        Vec::new()
    }

    fn is_schedule_allowed(&self, _ctx: &ManagerContext) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::Fixture;
    use curvine_common::state::{WorkerBGReport, WorkerNodePayload};

    #[test]
    fn name_and_type() {
        let s = StatsScheduler::new();
        assert_eq!(s.name(), "stats-scheduler");
    }

    #[test]
    fn fixed_interval_no_backoff() {
        let s = StatsScheduler::new();
        let interval = s.min_interval();
        assert_eq!(interval, Duration::from_secs(10));
        assert_eq!(s.next_interval(interval), interval);
    }

    #[test]
    fn always_allowed_and_returns_no_ops() {
        let f = Fixture::new();
        let s = StatsScheduler::new();
        assert!(s.is_schedule_allowed(&f.ctx));
        assert!(s.schedule(&f.ctx).is_empty());
    }

    /// Overwrite worker `wid`'s payload so it advertises `bg_reports`.
    fn set_worker_bg_reports(f: &Fixture, wid: u32, reports: Vec<WorkerBGReport>) {
        let mut node = f.ctx.node_manager.get_node(wid).expect("worker");
        node.payload = NodePayload::Worker(WorkerNodePayload {
            bg_reports: reports,
            ..WorkerNodePayload::default()
        });
        f.ctx.node_manager.test_insert_node(node);
    }

    #[test]
    fn schedule_aggregates_bg_reports_into_table_stats() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg1 = 5_000;
        let bg2 = 5_001;
        f.insert_bg(bg1, table_id, vec![100, 101, 102], None);
        f.insert_bg(bg2, table_id, vec![100, 101, 102], None);
        f.set_table_buckets(table_id, &[bg1, bg2]);

        set_worker_bg_reports(
            &f,
            100,
            vec![
                WorkerBGReport {
                    bg_id: bg1,
                    state: curvine_common::state::ReplicaState::Active,
                    stats: BGStats {
                        used_bytes: 100,
                        free_bytes: 0,
                        block_count: 1,
                        last_report_ms: 0,
                    },
                },
                WorkerBGReport {
                    bg_id: bg2,
                    state: curvine_common::state::ReplicaState::Active,
                    stats: BGStats {
                        used_bytes: 200,
                        free_bytes: 0,
                        block_count: 2,
                        last_report_ms: 0,
                    },
                },
            ],
        );

        // Before schedule: table stats are default zeros.
        let before = f.ctx.bg_manager.get_table_stats(table_id);
        assert_eq!(before.used_bytes, 0);
        assert_eq!(before.block_count, 0);

        let ops = StatsScheduler::new().schedule(&f.ctx);
        assert!(ops.is_empty(), "stats scheduler must never produce ops");

        // After schedule: aggregated stats are cached on BGTable.stats.
        let stats = f.ctx.bg_manager.get_table_stats(table_id);
        assert_eq!(stats.used_bytes, 300, "sum of reported used_bytes");
        assert_eq!(stats.block_count, 3, "sum of reported block counts");
    }

    #[test]
    fn multiple_schedule_calls_refresh_table_stats() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg_id = 5_100;
        f.insert_bg(bg_id, table_id, vec![100, 101, 102], None);
        f.set_table_buckets(table_id, &[bg_id]);

        let s = StatsScheduler::new();

        // Round 1.
        set_worker_bg_reports(
            &f,
            100,
            vec![WorkerBGReport {
                bg_id,
                state: curvine_common::state::ReplicaState::Active,
                stats: BGStats {
                    used_bytes: 100,
                    ..BGStats::default()
                },
            }],
        );
        s.schedule(&f.ctx);
        assert_eq!(f.ctx.bg_manager.get_table_stats(table_id).used_bytes, 100);

        // Round 2: fresher number overwrites.
        set_worker_bg_reports(
            &f,
            100,
            vec![WorkerBGReport {
                bg_id,
                state: curvine_common::state::ReplicaState::Active,
                stats: BGStats {
                    used_bytes: 500,
                    ..BGStats::default()
                },
            }],
        );
        s.schedule(&f.ctx);
        assert_eq!(
            f.ctx.bg_manager.get_table_stats(table_id).used_bytes,
            500,
            "table stats must be refreshed on each schedule call"
        );
    }

    #[test]
    fn schedule_without_reports_leaves_table_stats_zero() {
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);

        StatsScheduler::new().schedule(&f.ctx);

        let stats = f.ctx.bg_manager.get_table_stats(table_id);
        assert_eq!(stats.used_bytes, 0);
        assert_eq!(stats.block_count, 0);
    }
}
