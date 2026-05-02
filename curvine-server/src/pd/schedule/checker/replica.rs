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

use crate::pd::bg::placement::{isolation_score, Labels, PlacementRule};
use crate::pd::schedule::{BGOperator, ManagerContext, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::BlockGroupInfo;
use std::collections::HashMap;

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
        let has_placement = !rule.is_empty() && !rule.location_labels.is_empty();
        let worker_labels = if has_placement {
            ctx.pool_manager.get_workers_labels(resident_replicas)
        } else {
            HashMap::new()
        };

        let mut candidates: Vec<u32> = resident_replicas
            .iter()
            .filter(|&&w| w != lease_owner_id)
            .copied()
            .collect();

        let sort_keys: HashMap<u32, (f64, usize)> = candidates
            .iter()
            .map(|&w| {
                let iso = Self::isolation_without(w, resident_replicas, &rule, &worker_labels);
                let load = ctx.bg_manager.get_bgs_on_worker(w).len();
                (w, (iso, load))
            })
            .collect();

        candidates.sort_by(|a, b| {
            let (iso_a, load_a) = sort_keys[a];
            let (iso_b, load_b) = sort_keys[b];
            iso_b
                .partial_cmp(&iso_a)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| load_b.cmp(&load_a))
        });

        candidates.truncate(count);

        if candidates.len() < count {
            if resident_replicas.contains(&lease_owner_id) {
                candidates.push(lease_owner_id);
            }
        }
        candidates
    }

    fn isolation_without(
        worker: u32,
        replicas: &[u32],
        rule: &PlacementRule,
        labels: &Labels,
    ) -> f64 {
        if rule.location_labels.is_empty() {
            return 0.0;
        }
        let without: Vec<u32> = replicas.iter().filter(|&&r| r != worker).copied().collect();
        isolation_score(&without, &rule.location_labels, labels)
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
    use super::super::CheckerPriority;
    use super::*;
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use crate::pd::schedule::checker::Checker;
    use crate::pd::schedule::OpPriority;
    use curvine_common::state::ReplicaState;

    #[test]
    fn name_and_priority() {
        let checker = ReplicaChecker;
        assert_eq!(checker.name(), "replica-checker");
        assert_eq!(checker.priority(), CheckerPriority::REPLICA);
    }

    #[test]
    fn skips_bg_without_table() {
        let f = Fixture::new();
        let bg = f.insert_bg(1, 0x0001_0003, vec![100], None);
        // No table inserted → check_bg returns None (table lookup fails via `?`).
        assert!(ReplicaChecker.check_bg(&bg, &f.ctx).is_none());
    }

    #[derive(Debug, Clone, Copy)]
    enum Expect {
        None,
        AddReplicas(usize),
        RemoveReplicas(usize),
    }

    /// Mark a subset of replicas as Offline so `get_resident_replicas` drops them.
    fn offline(wids: &[u32]) -> Vec<(u32, ReplicaState)> {
        wids.iter().map(|&w| (w, ReplicaState::Offline)).collect()
    }

    struct Case {
        name: &'static str,
        desired_replicas: u16,
        replica_set: Vec<u32>,
        offline_wids: Vec<u32>,
        /// If Some, expect AddReplicas and check workers are drawn from this pool.
        candidate_workers: Option<Vec<u32>>,
        expect: Expect,
        /// For AddReplicas: expected priority.
        expect_priority: Option<u32>,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "healthy matches desired -> None",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102],
                offline_wids: vec![],
                candidate_workers: Some(vec![100, 101, 102, 103]),
                expect: Expect::None,
                expect_priority: None,
            },
            Case {
                name: "one replica offline -> add 1",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102],
                offline_wids: vec![102],
                candidate_workers: Some(vec![100, 101, 102, 103, 104]),
                expect: Expect::AddReplicas(1),
                expect_priority: Some(OpPriority::UNDER_REPLICA_REPAIR),
            },
            Case {
                name: "two replicas offline -> add 2",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102],
                offline_wids: vec![101, 102],
                candidate_workers: Some(vec![100, 101, 102, 103, 104, 105]),
                expect: Expect::AddReplicas(2),
                expect_priority: Some(OpPriority::UNDER_REPLICA_REPAIR),
            },
            Case {
                name: "under-replicated but no candidates available -> None",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102],
                offline_wids: vec![101, 102],
                // Only the resident worker is live; no replacement candidates.
                candidate_workers: Some(vec![100]),
                expect: Expect::None,
                expect_priority: None,
            },
            Case {
                name: "over-replicated by 1 -> remove 1",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102, 103],
                offline_wids: vec![],
                candidate_workers: Some(vec![100, 101, 102, 103]),
                expect: Expect::RemoveReplicas(1),
                expect_priority: Some(OpPriority::OVER_REPLICA_REPAIR),
            },
            Case {
                name: "over-replicated by 2 -> remove 2",
                desired_replicas: 3,
                replica_set: vec![100, 101, 102, 103, 104],
                offline_wids: vec![],
                candidate_workers: Some(vec![100, 101, 102, 103, 104]),
                expect: Expect::RemoveReplicas(2),
                expect_priority: Some(OpPriority::OVER_REPLICA_REPAIR),
            },
        ]
    }

    #[test]
    fn table_driven_check_bg() {
        for case in cases() {
            let f = Fixture::new();
            if let Some(ref workers) = case.candidate_workers {
                f.add_workers(workers, POOL_ID_SSD);
            }
            let table_id = f.insert_table(POOL_ID_SSD, case.desired_replicas);
            let bg = f.insert_bg(1, table_id, case.replica_set.clone(), None);
            f.set_replica_states(bg.bg_id, &offline(&case.offline_wids));

            let op = ReplicaChecker.check_bg(&bg, &f.ctx);
            match case.expect {
                Expect::None => {
                    assert!(op.is_none(), "{}: expected None, got {:?}", case.name, op);
                }
                Expect::AddReplicas(n) => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected Add op", case.name));
                    let (add, remove, _) = decompose(&op);
                    assert_eq!(add.len(), n, "{}: add count", case.name);
                    assert!(remove.is_empty(), "{}: unexpected Remove steps", case.name);
                    if let Some(expected_prio) = case.expect_priority {
                        assert_eq!(op.priority, expected_prio, "{}: priority", case.name);
                    }
                }
                Expect::RemoveReplicas(n) => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected Remove op", case.name));
                    let (_, removed, _) = decompose(&op);
                    assert_eq!(removed.len(), n, "{}: remove count", case.name);
                    let lease_owner = bg.lease_owner.as_ref().unwrap().node_id;
                    let non_lease = bg.replica_set.iter().filter(|&&w| w != lease_owner).count();
                    if n <= non_lease {
                        assert!(
                            !removed.contains(&lease_owner),
                            "{}: must not remove lease owner when non-lease replicas suffice",
                            case.name,
                        );
                    }
                    if let Some(expected_prio) = case.expect_priority {
                        assert_eq!(op.priority, expected_prio, "{}: priority", case.name);
                    }
                }
            }
        }
    }

    #[test]
    fn over_replicated_removes_lease_owner_only_as_last_resort() {
        // 5 replicas, desired=3 → remove 2. Lease owner = 100. Should remove two non-lease workers.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103, 104], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103, 104], Some(100));

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed.len(), 2);
        assert!(
            !removed.contains(&100),
            "lease owner 100 removed too eagerly"
        );
    }

    #[test]
    fn over_replicated_can_fall_back_to_lease_owner_when_needed() {
        // desired=1, replica_set=[100, 101] with lease_owner=100. Remove 1 non-lease (101).
        let f = Fixture::new();
        f.add_workers(&[100, 101], POOL_ID_SSD);
        let table_id = f.insert_table(POOL_ID_SSD, 1);
        let bg = f.insert_bg(1, table_id, vec![100, 101], Some(100));

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed, vec![101]);
    }
}
