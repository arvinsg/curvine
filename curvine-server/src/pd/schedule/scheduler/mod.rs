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

pub mod bg_balance;
pub mod bg_table;
pub mod lease_balance;
pub mod stats;

use crate::pd::bg::BGManager;
use crate::pd::config::ConfigManager;
use crate::pd::node::{NodeEvent, NodeManager};
use crate::pd::pool::PoolManager;
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::operator_controller::OperatorController;
use curvine_common::state::PoolInfo;
use curvine_common::FsResult;
use std::time::Duration;

/// Scheduler trait: proactive, optimization-driven scheduling.
///
/// Schedulers differ from Checkers:
/// - Checkers ensure correctness (always active, per-BG patrol)
/// - Schedulers optimize distribution (pluggable, pausable, adaptive intervals)
pub trait Scheduler: Send + Sync {
    /// Unique scheduler name.
    fn name(&self) -> &str;

    /// Type name for registry and creation.
    fn scheduler_type(&self) -> &str;

    /// Attempt to produce operators. Empty vec means no work found.
    fn schedule(&self, ctx: &SchedulerContext<'_>) -> Vec<BGOperator>;

    /// Whether scheduling is currently allowed (check rate limits, etc).
    fn is_schedule_allowed(&self, ctx: &SchedulerContext<'_>) -> bool;

    /// Minimum scheduling interval.
    fn min_interval(&self) -> Duration;

    /// Compute next interval based on current (for adaptive backoff).
    fn next_interval(&self, current: Duration) -> Duration;

    /// Encode current config for API display.
    fn encode_config(&self) -> FsResult<serde_json::Value>;

    /// Handle node event (optional, default no-op).
    fn on_event(&self, _event: &NodeEvent) {}
}

/// Context passed to schedulers (read-only refs to managers + operator controller).
pub struct SchedulerContext<'a> {
    pub pool_manager: &'a PoolManager,
    pub bg_manager: &'a BGManager,
    pub node_manager: &'a NodeManager,
    pub config_manager: &'a ConfigManager,
    pub operator_controller: &'a OperatorController,
}

/// Default adaptive interval logic.
pub struct BaseScheduler;

impl BaseScheduler {
    pub const MIN_INTERVAL: Duration = Duration::from_millis(100);
    pub const MAX_INTERVAL: Duration = Duration::from_secs(5);
    const BACKOFF_FACTOR: f64 = 1.3;

    /// Exponential backoff: current * 1.3, capped at MAX_INTERVAL.
    pub fn default_next_interval(current: Duration) -> Duration {
        let next_ms = (current.as_millis() as f64 * Self::BACKOFF_FACTOR) as u64;
        Duration::from_millis(next_ms).min(Self::MAX_INTERVAL)
    }
}

// ==================== Worker Score (migrated from balance/worker_score.rs) ====================

/// Per-worker score for a specific pool.
#[derive(Debug, Clone)]
pub struct WorkerScore {
    pub worker_id: u32,
    pub bg_count: u32,
    pub leader_count: u32,
    pub weight: f64,
    pub bg_score: f64,
    pub leader_score: f64,
}

/// Compute per-worker scores for the given live workers in a pool.
pub fn compute_pool_scores(
    live_workers: &[u32],
    pool: &PoolInfo,
    bg_manager: &BGManager,
    operator_controller: Option<&OperatorController>,
) -> Vec<WorkerScore> {
    let mut scores: Vec<WorkerScore> = live_workers
        .iter()
        .map(|&worker_id| WorkerScore {
            worker_id,
            bg_count: 0,
            leader_count: 0,
            weight: 1.0,
            bg_score: 0.0,
            leader_score: 0.0,
        })
        .collect();

    // Count BGs and leaders per worker
    for table in bg_manager.list_tables() {
        if table.pool_id() != pool.pool_id {
            continue;
        }
        for bg in bg_manager.list_bgs() {
            if bg.table_id != table.table_id {
                continue;
            }
            for score in &mut scores {
                if bg.replica_set.contains(&score.worker_id) {
                    score.bg_count += 1;
                }
                if bg.lease_owner.as_ref().map(|l| l.node_id) == Some(score.worker_id) {
                    score.leader_count += 1;
                }
            }
        }
    }

    // Compute scores, adjusting for in-flight operator influence
    for score in &mut scores {
        let mut adjusted_bg = score.bg_count as f64;
        let mut adjusted_leader = score.leader_count as f64;
        if let Some(oc) = operator_controller {
            adjusted_bg += oc.get_bg_influence(score.worker_id) as f64;
            adjusted_leader += oc.get_leader_influence(score.worker_id) as f64;
        }
        score.bg_score = adjusted_bg.max(0.0) / score.weight;
        score.leader_score = adjusted_leader.max(0.0) / score.weight;
    }

    scores
}

/// Check whether a balance operation should be performed (with hysteresis).
/// tolerant_ratio_bps is in basis points (e.g. 500 = 5%).
pub fn should_balance(
    source_score: f64,
    target_score: f64,
    mean_score: f64,
    tolerant_ratio_bps: u32,
) -> bool {
    let ratio = tolerant_ratio_bps as f64 / 10_000.0;
    let tolerant = (mean_score * ratio).max(1.0);
    source_score - target_score > tolerant
}

/// Build the default set of schedulers.
pub fn default_schedulers(
    ctx: std::sync::Arc<super::CoordinatorContext>,
    operator_controller: std::sync::Arc<OperatorController>,
) -> Vec<Box<dyn Scheduler>> {
    vec![
        Box::new(bg_balance::BGBalanceScheduler::new(ctx.clone())),
        Box::new(lease_balance::LeaseBalanceScheduler::new(ctx.clone())),
        Box::new(bg_table::BGTableScheduler::new(ctx.clone())),
        Box::new(stats::StatsScheduler::new(ctx, operator_controller)),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_balance_above_threshold() {
        assert!(should_balance(6.0, 4.0, 10.0, 500)); // diff=2 > 1.0
    }

    #[test]
    fn should_balance_within_threshold() {
        assert!(!should_balance(5.5, 5.0, 10.0, 500));
    }

    #[test]
    fn should_balance_single_worker_edge_case() {
        assert!(!should_balance(10.0, 10.0, 10.0, 500));
    }

    #[test]
    fn should_balance_large_cluster() {
        assert!(should_balance(60.0, 40.0, 100.0, 500)); // diff=20 > 5
        assert!(!should_balance(52.0, 48.0, 100.0, 500)); // diff=4 < 5
    }

    #[test]
    fn base_scheduler_interval_backoff() {
        let interval = Duration::from_millis(100);
        let next = BaseScheduler::default_next_interval(interval);
        assert_eq!(next, Duration::from_millis(130));

        // Should cap at MAX_INTERVAL
        let large = Duration::from_secs(10);
        let capped = BaseScheduler::default_next_interval(large);
        assert_eq!(capped, BaseScheduler::MAX_INTERVAL);
    }

    #[test]
    fn base_scheduler_min_interval() {
        assert_eq!(BaseScheduler::MIN_INTERVAL, Duration::from_millis(100));
    }
}
