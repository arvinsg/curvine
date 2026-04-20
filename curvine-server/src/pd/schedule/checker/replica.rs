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

use crate::pd::bg::placement::isolation_score;
use crate::pd::schedule::operator::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use crate::pd::schedule::ManagerContext;
use curvine_common::state::BlockGroupInfo;

pub struct ReplicaChecker;

impl ReplicaChecker {
    fn build_under_replicated_repair(
        bg: &BlockGroupInfo,
        resident_replicas: &[u32],
        ctx: &ManagerContext,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.replica_count() as usize;
        if resident_replicas.len() >= desired {
            return None;
        }
        let needed = desired - resident_replicas.len();
        let new_workers = ctx
            .bg_manager
            .select_replacement_workers(bg, needed as u16)
            .ok()?;
        if new_workers.is_empty() {
            log::error!(
                "BG {} under-replicated ({}/{}), no replacement workers available",
                bg.bg_id,
                resident_replicas.len(),
                desired
            );
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!(
                "Add {} replicas ({}/{})",
                new_workers.len(),
                resident_replicas.len(),
                desired
            ),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::UNDER_REPLICA_REPAIR);

        for &w in &new_workers {
            builder = builder.add_replica(w);
        }

        Some(builder.build())
    }

    fn build_over_replicated_repair(
        bg: &BlockGroupInfo,
        resident_replicas: &[u32],
        ctx: &ManagerContext,
    ) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.replica_count() as usize;
        if resident_replicas.len() <= desired {
            return None;
        }
        let excess = resident_replicas.len() - desired;

        let to_remove = Self::select_replicas_to_remove(bg, resident_replicas, excess, ctx);
        if to_remove.is_empty() {
            return None;
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Remove {} excess replicas", to_remove.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::OVER_REPLICA_REPAIR);

        for &w in &to_remove {
            builder = builder.remove_replica(w);
        }

        Some(builder.build())
    }

    fn select_replicas_to_remove(
        bg: &BlockGroupInfo,
        resident_replicas: &[u32],
        count: usize,
        ctx: &ManagerContext,
    ) -> Vec<u32> {
        let lease_owner_id = bg.lease_owner.as_ref().map(|l| l.node_id).unwrap_or(0);
        let rule = ctx.bg_manager.placement_rule();

        if !rule.is_empty() && !rule.location_labels.is_empty() {
            let worker_labels = ctx.pool_manager.get_workers_labels(resident_replicas);
            let mut candidates: Vec<(u32, f64)> = resident_replicas
                .iter()
                .filter(|&&w| w != lease_owner_id)
                .map(|&w| {
                    // Score without this replica — higher means this replica is less valuable
                    let without: Vec<u32> = resident_replicas
                        .iter()
                        .filter(|&&r| r != w)
                        .copied()
                        .collect();
                    let score = isolation_score(&without, &rule.location_labels, &worker_labels);
                    (w, score)
                })
                .collect();
            candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            candidates.iter().map(|(w, _)| *w).take(count).collect()
        } else {
            // Fallback: prefer non-lease-owner
            let mut to_remove: Vec<u32> = resident_replicas
                .iter()
                .filter(|&&w| w != lease_owner_id)
                .copied()
                .take(count)
                .collect();
            if to_remove.len() < count {
                to_remove.extend(
                    resident_replicas
                        .iter()
                        .filter(|&&w| w == lease_owner_id)
                        .copied()
                        .take(count - to_remove.len()),
                );
            }
            to_remove
        }
    }
}

impl super::Checker for ReplicaChecker {
    fn name(&self) -> &str {
        "replica-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        let table = ctx.bg_manager.get_table(bg.table_id)?;
        let desired = table.replica_count() as usize;
        let resident_replicas = ctx.bg_manager.get_resident_replicas(bg.bg_id);

        if resident_replicas.len() < desired {
            return Self::build_under_replicated_repair(bg, &resident_replicas, ctx);
        }

        if resident_replicas.len() > desired {
            return Self::build_over_replicated_repair(bg, &resident_replicas, ctx);
        }

        None
    }

    fn priority(&self) -> u32 {
        super::CheckerPriority::REPLICA
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pd::schedule::checker::Checker;
    use crate::pd::schedule::ManagerContext;
    use curvine_common::state::{BGLease, BGOpState, BGState};

    fn test_ctx() -> std::sync::Arc<ManagerContext> {
        crate::pd::schedule::checker::tests_common::test_context(std::collections::HashMap::new())
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
    fn no_ops_for_healthy_bgs() {
        let ctx = test_ctx();
        let checker = ReplicaChecker;
        let bg = make_bg(1, 0x0001_0003, vec![100, 101, 102]);
        ctx.bg_manager
            .apply_create_bg(&crate::pd::journal::entry::BGEntry {
                op_ms: 0,
                info: bg.clone(),
            })
            .unwrap();
        // No workers registered -> all replicas are Pending (Resident) -> count=3 == desired=3 -> None
        assert!(checker.check_bg(&bg, &ctx).is_none());
    }

    #[test]
    fn skips_bg_without_table() {
        let ctx = test_ctx();
        let checker = ReplicaChecker;
        let bg = make_bg(1, 0x0001_0003, vec![100]);
        assert!(checker.check_bg(&bg, &ctx).is_none());
    }

    #[test]
    fn name_and_priority() {
        let checker = ReplicaChecker;
        assert_eq!(checker.name(), "replica-checker");
        assert_eq!(checker.priority(), super::super::CheckerPriority::REPLICA);
    }
}
