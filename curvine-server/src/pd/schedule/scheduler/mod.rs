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
pub mod decommission;
pub mod lease_balance;
pub mod stats;

use crate::pd::bg::placement::context::PendingInfluence;
use crate::pd::schedule::{BGOperator, ManagerContext, OperatorController};
use curvine_common::state::PoolType;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum ScheduleEvent {
    WorkerJoinedPools {
        worker_id: u32,
        node_epoch: u64,
        pool_types: Vec<PoolType>,
        event_time_ms: u64,
    },
    WorkerLost {
        worker_id: u32,
        node_epoch: u64,
        event_time_ms: u64,
    },
    WorkerOffline {
        worker_id: u32,
        node_epoch: u64,
        event_time_ms: u64,
    },
    WorkerDecommissionStarted {
        worker_id: u32,
        node_epoch: u64,
        event_time_ms: u64,
    },
    WorkerDecommissionFinished {
        worker_id: u32,
        node_epoch: u64,
        event_time_ms: u64,
    },
    WorkerHeartbeatResumed {
        worker_id: u32,
        node_epoch: u64,
        event_time_ms: u64,
    },
}

/// Scheduler trait: proactive, optimization-driven scheduling.
///
/// Schedulers differ from Checkers:
/// - Checkers ensure correctness (always active)
/// - Schedulers optimize distribution (pluggable, pausable, adaptive intervals)
pub trait Scheduler: Send + Sync {
    fn name(&self) -> &str;

    fn schedule(&self, ctx: &ManagerContext) -> Vec<BGOperator>;

    fn is_schedule_allowed(&self, ctx: &ManagerContext) -> bool;

    fn min_interval(&self) -> Duration;

    fn next_interval(&self, current: Duration) -> Duration;

    fn on_event(&self, _event: &ScheduleEvent) {}

    fn on_leader_start(&self) {}
}

/// Default adaptive interval logic.
pub struct BaseScheduler;

impl BaseScheduler {
    pub const MIN_INTERVAL: Duration = Duration::from_millis(100);
    pub const MAX_INTERVAL: Duration = Duration::from_secs(5);
    const BACKOFF_FACTOR: f64 = 1.3;

    pub fn default_next_interval(current: Duration) -> Duration {
        let next_ms = (current.as_millis() as f64 * Self::BACKOFF_FACTOR) as u64;
        Duration::from_millis(next_ms).min(Self::MAX_INTERVAL)
    }
}

/// Extract pending influence from OperatorController into a pure data struct.
pub fn build_pending_influence(oc: &OperatorController) -> PendingInfluence {
    let (bg_delta, lease_delta) = oc.get_all_pending_deltas();
    PendingInfluence {
        bg_delta,
        lease_delta,
    }
}

/// Build the default set of schedulers.
pub fn default_schedulers(ctx: std::sync::Arc<ManagerContext>) -> Vec<Box<dyn Scheduler>> {
    vec![
        Box::new(bg_balance::BGBalanceScheduler::default()),
        Box::new(lease_balance::LeaseBalanceScheduler::default()),
        Box::new(bg_table::BGTableScheduler::new(ctx.clone())),
        Box::new(decommission::DecommissionScheduler::new()),
        Box::new(stats::StatsScheduler::new()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_scheduler_interval_backoff() {
        let interval = Duration::from_millis(100);
        let next = BaseScheduler::default_next_interval(interval);
        assert_eq!(next, Duration::from_millis(130));

        let large = Duration::from_secs(10);
        let capped = BaseScheduler::default_next_interval(large);
        assert_eq!(capped, BaseScheduler::MAX_INTERVAL);
    }

    #[test]
    fn base_scheduler_min_interval() {
        assert_eq!(BaseScheduler::MIN_INTERVAL, Duration::from_millis(100));
    }
}
