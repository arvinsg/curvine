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
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BGStats, NodePayload, NodeType};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

pub struct StatsScheduler {
    table_stats_cache: RwLock<HashMap<u32, BGStats>>,
}

impl StatsScheduler {
    pub fn new() -> Self {
        Self {
            table_stats_cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_table_stats(&self, table_id: u32) -> Option<BGStats> {
        self.table_stats_cache.read().unwrap().get(&table_id).cloned()
    }

    pub fn get_all_table_stats(&self) -> HashMap<u32, BGStats> {
        self.table_stats_cache.read().unwrap().clone()
    }
}

impl Scheduler for StatsScheduler {
    fn name(&self) -> &str {
        "stats-scheduler"
    }

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator> {
        ctx.pool_manager.refresh_pool_stats();

        let workers = ctx.node_manager.get_nodes_by_type(NodeType::Worker);
        for node in workers {
            if let NodePayload::Worker(ref payload) = node.payload {
                if !payload.bg_reports.is_empty() {
                    let stats_map: HashMap<u32, BGStats> = payload
                        .bg_reports
                        .iter()
                        .map(|r| (r.bg_id, r.stats.clone()))
                        .collect();
                    ctx.bg_manager.update_bg_stats(&stats_map);
                }
            }
        }

        let tables = ctx.bg_manager.list_tables();
        let mut cache = self.table_stats_cache.write().unwrap();
        cache.clear();
        for table in &tables {
            let stats = ctx.bg_manager.compute_table_stats(table.table_id);
            cache.insert(table.table_id, stats);
        }

        Vec::new()
    }

    fn is_schedule_allowed(&self, _ctx: &ManagerContext) -> bool {
        true
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_type() {
        let s = StatsScheduler::new();
        assert_eq!(s.name(), "stats-scheduler");
    }

    #[test]
    fn empty_cache_initially() {
        let s = StatsScheduler::new();
        assert!(s.get_all_table_stats().is_empty());
        assert!(s.get_table_stats(1).is_none());
    }

    #[test]
    fn fixed_interval_no_backoff() {
        let s = StatsScheduler::new();
        let interval = s.min_interval();
        assert_eq!(s.next_interval(interval), interval);
    }
}
