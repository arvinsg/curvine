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

use crate::pd::bg::placement::{isolation_score, worst_replica};
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::{BlockGroupInfo, ReplicaState};

pub struct PlacementRuleChecker;

impl super::Checker for PlacementRuleChecker {
    fn name(&self) -> &str {
        "placement-rule-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        if !ctx.config_manager.get_bool(
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED,
            crate::pd::config::keys::PD_SCHEDULE_PLACEMENT_CHECK_ENABLED_DEFAULT,
        ) {
            return None;
        }

        let rule = ctx.bg_manager.placement_rule();
        if rule.is_empty() {
            return None;
        }

        let resident = ctx.bg_manager.get_resident_replicas(bg.bg_id);
        let worker_labels = ctx.pool_manager.get_workers_labels(&resident);

        let worst_worker = worst_replica(&resident, &rule, &worker_labels)?;
        let current_score = isolation_score(&resident, &rule.location_labels, &worker_labels);

        let pool_id = (bg.table_id >> 16) as u16;
        let all_worker_ids: Vec<u32> = ctx.pool_manager.get_live_workers(pool_id);
        let all_worker_labels = ctx.pool_manager.get_workers_labels(&all_worker_ids);

        let mut best_replacement = None;
        let mut best_score = current_score;

        for &candidate in &all_worker_ids {
            if resident.contains(&candidate) {
                continue;
            }
            let mut new_set = resident.clone();
            if let Some(pos) = new_set.iter().position(|&w| w == worst_worker) {
                new_set[pos] = candidate;
            }

            let mut combined_labels = worker_labels.clone();
            if let Some(lbl) = all_worker_labels.get(&candidate) {
                combined_labels.insert(candidate, lbl.clone());
            }

            let passes = combined_labels
                .get(&candidate)
                .map(|l| rule.label_constraints.iter().all(|lc| lc.matches(l)))
                .unwrap_or(rule.label_constraints.is_empty());
            if !passes {
                continue;
            }

            let new_score = isolation_score(&new_set, &rule.location_labels, &combined_labels);
            if new_score > best_score {
                best_replacement = Some(candidate);
                best_score = new_score;
            }
        }

        let new_worker = best_replacement?;

        let mut builder = OperatorBuilder::new(
            OperatorKind::Balance,
            bg.bg_id,
            format!(
                "Placement fix: replace {} with {} (score {:.0} -> {:.0})",
                worst_worker, new_worker, current_score, best_score,
            ),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::PLACEMENT_FIX)
        .add_replica(new_worker)
        .wait_replica_ready(new_worker, ReplicaState::Active);

        if bg
            .lease_owner
            .as_ref()
            .map(|l| l.node_id == worst_worker)
            .unwrap_or(false)
        {
            let to_worker = bg
                .replica_set
                .iter()
                .filter(|&&w| w != worst_worker)
                .find(|&&w| ctx.pool_manager.is_worker_available(w))
                .copied()
                .unwrap_or(new_worker);
            builder = builder.transfer_lease(worst_worker, to_worker);
        }

        builder = builder.remove_replica(worst_worker);
        Some(builder.build())
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::PLACEMENT_RULE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::Checker;
    use curvine_common::state::{BGLease, BGOpState, BGState, BlockGroupInfo};

    fn test_ctx() -> std::sync::Arc<ManagerContext> {
        crate::pd::schedule::checker::tests_common::test_context(
            std::collections::HashMap::new(),
        )
    }

    fn make_bg(bg_id: u32, table_id: u32, replica_set: Vec<u32>) -> BlockGroupInfo {
        let leader = replica_set.first().copied().unwrap_or(0);
        BlockGroupInfo {
            bg_id,
            table_id,
            bg_epoch: 1,
            replica_set,
            state: BGState::Assigned,
            op_state: BGOpState::Idle,
            lease_owner: Some(BGLease {
                node_id: leader,
                epoch: 1,
                grant_time_ms: 0,
            }),
            stats: Default::default(),
        }
    }

    #[test]
    fn no_ops_when_no_rules() {
        let ctx = test_ctx();
        let checker = PlacementRuleChecker;
        let bg = make_bg(1, 0x0001_0001, vec![100, 101, 102]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg.clone(),
            })
            .unwrap();
        assert!(checker.check_bg(&bg, &ctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let checker = PlacementRuleChecker;
        assert_eq!(checker.name(), "placement-rule-checker");
        assert_eq!(checker.priority(), 30);
    }
}
