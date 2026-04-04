use super::{Scheduler, SchedulerContext};
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::operator_controller::OperatorController;
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::{BGStats, NodePayload, NodeType};
use curvine_common::FsResult;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// StatsScheduler: periodically collects and aggregates Pool / BGTable statistics.
///
/// Replaces the inline stats refresh logic that was embedded in coordinator's patrol_loop.
pub struct StatsScheduler {
    ctx: Arc<CoordinatorContext>,
    _operator_controller: Arc<OperatorController>,
    /// Cached table stats, refreshed periodically.
    table_stats_cache: RwLock<HashMap<u32, BGStats>>,
}

impl StatsScheduler {
    pub fn new(ctx: Arc<CoordinatorContext>, operator_controller: Arc<OperatorController>) -> Self {
        Self {
            ctx,
            _operator_controller: operator_controller,
            table_stats_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Get cached table stats (O(1) lookup).
    pub fn get_table_stats(&self, table_id: u32) -> Option<BGStats> {
        self.table_stats_cache.read().unwrap().get(&table_id).cloned()
    }

    /// Get all cached table stats.
    pub fn get_all_table_stats(&self) -> HashMap<u32, BGStats> {
        self.table_stats_cache.read().unwrap().clone()
    }
}

impl Scheduler for StatsScheduler {
    fn name(&self) -> &str {
        "stats-scheduler"
    }

    fn scheduler_type(&self) -> &str {
        "stats"
    }

    fn schedule(&self, ctx: &SchedulerContext<'_>) -> Vec<BGOperator> {
        // 1. Refresh pool stats
        ctx.pool_manager.refresh_pool_stats();

        // 2. Collect BG stats from worker heartbeat payloads
        let workers = ctx.node_manager.get_nodes_by_type(NodeType::Worker);
        for node in workers {
            if let NodePayload::Worker(ref payload) = node.payload {
                if !payload.bg_stats.is_empty() {
                    ctx.bg_manager.update_bg_stats(&payload.bg_stats);
                }
            }
        }

        // 3. Refresh table stats cache
        let tables = ctx.bg_manager.list_tables();
        let mut cache = self.table_stats_cache.write().unwrap();
        cache.clear();
        for table in &tables {
            let stats = ctx.bg_manager.compute_table_stats(table.table_id);
            cache.insert(table.table_id, stats);
        }

        // StatsScheduler does not produce operators
        Vec::new()
    }

    fn is_schedule_allowed(&self, _ctx: &SchedulerContext<'_>) -> bool {
        true // stats collection is always allowed
    }

    fn min_interval(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn next_interval(&self, current: Duration) -> Duration {
        current // Fixed interval, no backoff
    }

    fn encode_config(&self) -> FsResult<serde_json::Value> {
        let cache_size = self.table_stats_cache.read().unwrap().len();
        Ok(serde_json::json!({
            "type": self.scheduler_type(),
            "interval_ms": self.min_interval().as_millis() as u64,
            "cached_tables": cache_size,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_type() {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let oc = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let s = StatsScheduler::new(ctx, oc);
        assert_eq!(s.name(), "stats-scheduler");
        assert_eq!(s.scheduler_type(), "stats");
    }

    #[test]
    fn empty_cache_initially() {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let oc = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let s = StatsScheduler::new(ctx, oc);
        assert!(s.get_all_table_stats().is_empty());
        assert!(s.get_table_stats(1).is_none());
    }

    #[test]
    fn fixed_interval_no_backoff() {
        let ctx = crate::pd::schedule::checker::tests_common::test_coordinator_context(
            std::collections::HashMap::new(),
        );
        let oc = Arc::new(OperatorController::new(
            ctx.config_manager.clone(),
            ctx.bg_manager.clone(),
        ));
        let s = StatsScheduler::new(ctx, oc);
        let interval = s.min_interval();
        assert_eq!(s.next_interval(interval), interval);
    }
}
