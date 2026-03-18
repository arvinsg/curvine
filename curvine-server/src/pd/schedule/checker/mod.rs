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

use self::bg_assignment::BGAssignmentChecker;
use self::node_health::NodeHealthChecker;
use self::replica::ReplicaChecker;
use crate::pd::node::NodeEvent;
use crate::pd::schedule::operator::BGOperator;
use crate::pd::schedule::CoordinatorContext;
use curvine_common::state::BlockGroupInfo;
use std::sync::Arc;

pub mod bg_assignment;
pub mod node_health;
pub mod replica;

/// Context passed to checkers (read-only refs to managers)
pub struct CheckerContext<'a> {
    pub pool_manager: &'a crate::pd::pool::PoolManager,
    pub bg_manager: &'a crate::pd::bg::BGManager,
    pub node_manager: &'a crate::pd::node::NodeManager,
    pub config_manager: &'a crate::pd::config::ConfigManager,
}

/// Direct BG push command for a worker (via heartbeat response or RPC)
#[derive(Debug, Clone)]
pub struct BGPushCommand {
    pub worker_id: u32,
    pub add_bgs: Vec<BlockGroupInfo>,
    pub remove_bgs: Vec<u32>,
}

/// Result of a checker patrol
#[derive(Debug, Default)]
pub struct CheckResult {
    pub bg_operators: Vec<BGOperator>,
    pub bg_push_commands: Vec<BGPushCommand>,
}

/// Checker trait: periodic patrol that produces operators and push commands
pub trait Checker: Send + Sync {
    fn name(&self) -> &str;
    fn check(&self, ctx: &CheckerContext<'_>) -> CheckResult;
    fn interval_ms(&self) -> u64;
}

/// Scheduler trait: proactive, event-driven + periodic tasks
pub trait Scheduler: Send + Sync {
    fn name(&self) -> &str;
    fn on_event(&self, event: &NodeEvent);
    fn tick(&self);
}

/// Build default set of checkers
pub fn default_checkers(ctx: Arc<CoordinatorContext>) -> Vec<Box<dyn Checker>> {
    vec![
        Box::new(NodeHealthChecker::new(ctx.clone())),
        Box::new(ReplicaChecker::new(ctx.clone())),
        Box::new(BGAssignmentChecker::new(ctx)),
    ]
}
