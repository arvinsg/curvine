use super::operator::BGOperator;
use super::operator_controller::OperatorController;
use super::scheduler::{Scheduler, SchedulerContext, default_schedulers};
use super::CoordinatorContext;
use crate::pd::node::NodeEvent;
use curvine_common::FsResult;
use std::sync::Arc;
use std::time::Duration;

/// Wraps a scheduler with runtime state.
struct SchedulerWrapper {
    scheduler: Box<dyn Scheduler>,
    current_interval: Duration,
    last_run_ms: u64,
    paused_until_ms: u64,
}

/// Status of a scheduler for API display.
#[derive(Debug, Clone)]
pub struct SchedulerStatus {
    pub name: String,
    pub scheduler_type: String,
    pub paused: bool,
    pub current_interval_ms: u64,
}

/// Manages and runs all schedulers with adaptive intervals and pause/resume support.
pub struct SchedulerController {
    schedulers: std::sync::RwLock<Vec<SchedulerWrapper>>,
    ctx: Arc<CoordinatorContext>,
    operator_controller: Arc<OperatorController>,
}

impl SchedulerController {
    pub fn new(ctx: Arc<CoordinatorContext>, operator_controller: Arc<OperatorController>) -> Self {
        let defaults = default_schedulers(ctx.clone(), operator_controller.clone());
        let wrappers = defaults
            .into_iter()
            .map(|s| {
                let interval = s.min_interval();
                SchedulerWrapper {
                    scheduler: s,
                    current_interval: interval,
                    last_run_ms: 0,
                    paused_until_ms: 0,
                }
            })
            .collect();
        Self {
            schedulers: std::sync::RwLock::new(wrappers),
            ctx,
            operator_controller,
        }
    }

    /// Run one scheduling tick: iterate all schedulers, execute ready ones.
    /// Returns operators produced in this tick.
    pub fn schedule_tick(&self) -> Vec<BGOperator> {
        let now = orpc::common::LocalTime::mills();
        let mut all_ops = Vec::new();

        let sctx = SchedulerContext {
            pool_manager: self.ctx.pool_manager.as_ref(),
            bg_manager: self.ctx.bg_manager.as_ref(),
            node_manager: self.ctx.node_manager.as_ref(),
            config_manager: self.ctx.config_manager.as_ref(),
            operator_controller: self.operator_controller.as_ref(),
        };

        let mut schedulers = self.schedulers.write().unwrap();
        for wrapper in schedulers.iter_mut() {
            if wrapper.paused_until_ms > now {
                continue;
            }

            let elapsed = now.saturating_sub(wrapper.last_run_ms);
            if elapsed < wrapper.current_interval.as_millis() as u64 {
                continue;
            }

            if !wrapper.scheduler.is_schedule_allowed(&sctx) {
                wrapper.current_interval = wrapper.scheduler.next_interval(wrapper.current_interval);
                wrapper.last_run_ms = now;
                continue;
            }

            let ops = wrapper.scheduler.schedule(&sctx);
            if ops.is_empty() {
                // No work: backoff
                wrapper.current_interval = wrapper.scheduler.next_interval(wrapper.current_interval);
            } else {
                // Work found: reset to min interval
                wrapper.current_interval = wrapper.scheduler.min_interval();
                all_ops.extend(ops);
            }
            wrapper.last_run_ms = now;
        }

        all_ops
    }

    /// Pause a scheduler for the given duration.
    pub fn pause_scheduler(&self, name: &str, duration: Duration) -> FsResult<()> {
        let now = orpc::common::LocalTime::mills();
        let mut schedulers = self.schedulers.write().unwrap();
        for wrapper in schedulers.iter_mut() {
            if wrapper.scheduler.name() == name {
                wrapper.paused_until_ms = now + duration.as_millis() as u64;
                return Ok(());
            }
        }
        Err(curvine_common::FsError::common(format!(
            "scheduler '{}' not found",
            name
        )))
    }

    /// Resume a paused scheduler immediately.
    pub fn resume_scheduler(&self, name: &str) -> FsResult<()> {
        let mut schedulers = self.schedulers.write().unwrap();
        for wrapper in schedulers.iter_mut() {
            if wrapper.scheduler.name() == name {
                wrapper.paused_until_ms = 0;
                return Ok(());
            }
        }
        Err(curvine_common::FsError::common(format!(
            "scheduler '{}' not found",
            name
        )))
    }

    /// List all schedulers with their current status.
    pub fn list_schedulers(&self) -> Vec<SchedulerStatus> {
        let now = orpc::common::LocalTime::mills();
        let schedulers = self.schedulers.read().unwrap();
        schedulers
            .iter()
            .map(|w| SchedulerStatus {
                name: w.scheduler.name().to_string(),
                scheduler_type: w.scheduler.scheduler_type().to_string(),
                paused: w.paused_until_ms > now,
                current_interval_ms: w.current_interval.as_millis() as u64,
            })
            .collect()
    }

    /// Forward a node event to all schedulers that handle events.
    pub fn on_event(&self, event: &NodeEvent) {
        let schedulers = self.schedulers.read().unwrap();
        for wrapper in schedulers.iter() {
            wrapper.scheduler.on_event(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_status_fields() {
        let status = SchedulerStatus {
            name: "test".into(),
            scheduler_type: "test-type".into(),
            paused: false,
            current_interval_ms: 100,
        };
        assert_eq!(status.name, "test");
        assert!(!status.paused);
    }
}
