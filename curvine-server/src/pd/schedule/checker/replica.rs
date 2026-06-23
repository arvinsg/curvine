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
use curvine_common::state::{BlockGroupInfo, NodeState, ReplicaState};
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
            builder = builder
                .add_replica(w)
                .wait_replica_ready(w, ReplicaState::Active);
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
        let lease_owner = bg.lease_owner.as_ref().map(|l| l.node_id);
        let mut to_remove =
            Self::select_replicas_to_remove(ctx, resident_replicas, excess, lease_owner);
        if to_remove.is_empty() {
            return None;
        }

        let mut transfer: Option<(u32, u32)> = None;
        if let Some(lease) = &bg.lease_owner {
            if to_remove.contains(&lease.node_id) {
                let survivor = bg
                    .replica_set
                    .iter()
                    .copied()
                    .find(|w| !to_remove.contains(w));
                match survivor {
                    Some(default_target) => {
                        let new_owner =
                            ctx.pick_lease_fallback_excluding(bg, &to_remove, default_target);
                        transfer = Some((lease.node_id, new_owner));
                    }
                    None => {
                        log::warn!(
                            "BG {} over-replicated: keeping lease owner {} — no survivor for lease transfer",
                            bg.bg_id,
                            lease.node_id,
                        );
                        to_remove.retain(|&w| w != lease.node_id);
                        if to_remove.is_empty() {
                            return None;
                        }
                    }
                }
            }
        }

        let mut builder = OperatorBuilder::new(
            OperatorKind::Repair,
            bg.bg_id,
            format!("Remove {} excess replicas", to_remove.len()),
        )
        .bg_epoch(bg.bg_epoch)
        .priority(OpPriority::OVER_REPLICA_REPAIR);

        if let Some((from, to)) = transfer {
            builder = builder.transfer_lease(from, to);
        }

        for &w in &to_remove {
            builder = builder.remove_replica(w);
        }

        Some(builder.build())
    }

    /// Choose which replicas to remove when a BG is over-replicated.
    ///
    /// Cascade:
    ///   1. replicas on Decommission nodes (prefer first)
    ///   2. replicas on Offline nodes
    ///   3. healthy replicas, ranked by topology+load policy
    fn select_replicas_to_remove(
        ctx: &ManagerContext,
        resident_replicas: &[u32],
        count: usize,
        lease_owner: Option<u32>,
    ) -> Vec<u32> {
        let candidates: Vec<u32> = resident_replicas.iter().copied().collect();
        let on_decommission = filter_by_node_state(&candidates, NodeState::Decommission, ctx);
        let on_offline = filter_by_node_state(&candidates, NodeState::Offline, ctx);
        let healthy: Vec<u32> = candidates
            .iter()
            .copied()
            .filter(|w| !on_decommission.contains(w) && !on_offline.contains(w))
            .collect();
        let healthy_ranked =
            Self::rank_healthy_by_policy(ctx, resident_replicas, &healthy, lease_owner);

        let mut chosen: Vec<u32> = Vec::with_capacity(count);
        for w in on_decommission
            .into_iter()
            .chain(on_offline)
            .chain(healthy_ranked)
        {
            if chosen.len() == count {
                break;
            }
            if !chosen.contains(&w) {
                chosen.push(w);
            }
        }

        chosen
    }

    /// Rank healthy candidates: better isolation (after removal) and higher
    /// load come first.
    fn rank_healthy_by_policy(
        ctx: &ManagerContext,
        resident_replicas: &[u32],
        healthy: &[u32],
        lease_owner: Option<u32>,
    ) -> Vec<u32> {
        let rule = ctx.bg_manager.placement_rule();
        let has_placement = !rule.is_empty() && !rule.location_labels.is_empty();
        let worker_labels = if has_placement {
            ctx.pool_manager.get_workers_labels(resident_replicas)
        } else {
            HashMap::new()
        };

        let mut sorted: Vec<u32> = healthy.to_vec();
        let sort_keys: HashMap<u32, (bool, f64, usize)> = sorted
            .iter()
            .map(|&w| {
                let is_owner = Some(w) == lease_owner;
                let iso = Self::isolation_without(w, resident_replicas, &rule, &worker_labels);
                let load = ctx.bg_manager.get_bgs_on_worker(w).len();
                (w, (is_owner, iso, load))
            })
            .collect();
        sorted.sort_by(|a, b| {
            let (owner_a, iso_a, load_a) = sort_keys[a];
            let (owner_b, iso_b, load_b) = sort_keys[b];
            // Non-owners first (false < true), then higher isolation,
            // then higher load.
            owner_a
                .cmp(&owner_b)
                .then_with(|| {
                    iso_b
                        .partial_cmp(&iso_a)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| load_b.cmp(&load_a))
        });
        sorted
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

/// Workers in `candidates` whose node currently sits in `state`.
fn filter_by_node_state(candidates: &[u32], state: NodeState, ctx: &ManagerContext) -> Vec<u32> {
    candidates
        .iter()
        .copied()
        .filter(|&w| ctx.node_manager.get_node(w).map(|n| n.state) == Some(state))
        .collect()
}

impl super::Checker for ReplicaChecker {
    fn name(&self) -> &str {
        "replica-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        if super::bg_in_leaving_grace(bg, ctx) {
            return None;
        }

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
    use crate::pd::config::keys;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use crate::pd::schedule::checker::Checker;
    use crate::pd::schedule::OpPriority;
    use curvine_common::state::PoolType;
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
                f.add_workers(workers, PoolType::Ssd);
            }
            let table_id = f.insert_table(PoolType::Ssd, case.desired_replicas);
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
                    let wait_active = op
                        .steps
                        .iter()
                        .filter(|step| {
                            matches!(
                                step,
                                crate::pd::schedule::OpStep::WaitReplicaReady {
                                    expected_state: ReplicaState::Active,
                                    ..
                                }
                            )
                        })
                        .count();
                    assert_eq!(wait_active, n, "{}: wait active count", case.name);
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
        f.add_workers(&[100, 101, 102, 103, 104], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
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
        f.add_workers(&[100, 101], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 1);
        let bg = f.insert_bg(1, table_id, vec![100, 101], Some(100));

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed, vec![101]);
    }

    #[test]
    fn over_replica_lost_replicas_treated_as_healthy() {
        // 4 replicas, desired=3 → remove 1. Replica 102 is Lost. Lost nodes /
        // replicas are NOT in the leaving cascade — they may recover. The
        // checker falls through to topology+load policy without preferring 102.
        let f = Fixture::new();
        f.add_workers(&[100, 101, 102, 103], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103], Some(100));
        f.set_replica_states(1, &[(102, ReplicaState::Lost)]);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed.len(), 1);
        // Whichever non-lease replica policy picks is fine; lease owner 100 must
        // not be removed.
        assert!(removed[0] != 100);
    }

    #[test]
    fn skips_bg_with_replica_on_leaving_node() {
        // Within the grace window (default 15min), checker defers to scheduler
        // on Decommission/Offline replicas.
        for leaving_state in [
            curvine_common::state::NodeState::Offline,
            curvine_common::state::NodeState::Decommission,
        ] {
            let f = Fixture::new();
            f.add_workers(&[100, 101, 102, 103], PoolType::Ssd);
            let table_id = f.insert_table(PoolType::Ssd, 3);
            let bg = f.insert_bg(1, table_id, vec![100, 101, 102], Some(100));
            f.set_worker_state(102, leaving_state);

            assert!(
                ReplicaChecker.check_bg(&bg, &f.ctx).is_none(),
                "BG with replica on {:?} node must be skipped within grace",
                leaving_state
            );
        }
    }

    #[test]
    fn takes_over_after_grace_window_expires() {
        // After grace_ms = 0 (i.e. instantly past grace), checker falls back
        // to its repair logic. Use over-replication so the checker has clear
        // work to do regardless of replica states.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101, 102, 103], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103], Some(100));
        f.set_worker_state(102, curvine_common::state::NodeState::Decommission);

        // Without grace, checker takes over: resident=4, desired=3 → remove 1.
        // Cascade picks 102 (decommission node) first.
        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed, vec![102]);
    }

    #[test]
    fn over_replica_prefers_decommission_over_healthy() {
        // 5 replicas, desired=3 → remove 2. Worker 102 on Decommission node.
        // Cascade: pick 102 first (decommission), then one healthy by policy.
        // grace=0 so the leaving check doesn't skip the BG.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101, 102, 103, 104], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103, 104], Some(100));
        f.set_worker_state(102, curvine_common::state::NodeState::Decommission);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&102), "decommission replica must be first");
    }

    #[test]
    fn over_replica_prefers_offline_node_in_cascade() {
        // 5 replicas, desired=3 → remove 2. Worker 102 on Offline node, 103 on
        // Decommission node. Cascade: 103 (decommission) first, then 102 (offline).
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101, 102, 103, 104], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103, 104], Some(100));
        f.set_worker_state(102, curvine_common::state::NodeState::Offline);
        f.set_worker_state(103, curvine_common::state::NodeState::Decommission);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, _) = decompose(&op);
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&102), "offline-node replica removed");
        assert!(removed.contains(&103), "decommission-node replica removed");
    }

    #[test]
    fn over_replica_transfers_lease_when_owner_is_removed() {
        // 5 replicas, desired=3 → remove 2. Lease owner = 102 (which is on a
        // Decommission node, so cascade picks it for removal). Operator must
        // include a TransferLease step to a survivor before the removes.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101, 102, 103, 104], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        // lease_owner = 102 (will be removed)
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103, 104], Some(102));
        f.set_worker_state(102, curvine_common::state::NodeState::Decommission);
        f.set_worker_state(103, curvine_common::state::NodeState::Offline);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, transfers) = decompose(&op);
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&102) && removed.contains(&103));

        assert_eq!(
            transfers.len(),
            1,
            "lease owner removed → transfer expected"
        );
        let (from, to) = transfers[0];
        assert_eq!(
            from, 102,
            "transfer must originate from removed lease owner"
        );
        assert!(!removed.contains(&to), "transfer target must be a survivor");
    }

    #[test]
    fn over_replica_no_transfer_when_owner_survives() {
        // Sanity: lease owner stays in the surviving set → no TransferLease.
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101, 102, 103, 104], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 3);
        // lease_owner = 100 (survives)
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102, 103, 104], Some(100));
        f.set_worker_state(103, curvine_common::state::NodeState::Decommission);
        f.set_worker_state(104, curvine_common::state::NodeState::Offline);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, transfers) = decompose(&op);
        assert_eq!(removed.len(), 2);
        assert!(!removed.contains(&100));
        assert!(transfers.is_empty(), "no transfer when owner survives");
    }

    #[test]
    fn over_replica_keeps_lease_owner_when_no_survivor_available() {
        let mut overrides = std::collections::HashMap::new();
        overrides.insert(
            keys::PD_CHECKER_LEAVING_GRACE_MS.to_string(),
            "0".to_string(),
        );
        let f = Fixture::with_overrides(overrides);
        f.add_workers(&[100, 101], PoolType::Ssd);
        let table_id = f.insert_table(PoolType::Ssd, 0);
        let bg = f.insert_bg(1, table_id, vec![100, 101], Some(100));
        f.set_worker_state(100, curvine_common::state::NodeState::Decommission);
        f.set_worker_state(101, curvine_common::state::NodeState::Decommission);

        let op = ReplicaChecker.check_bg(&bg, &f.ctx).expect("op");
        let (_, removed, transfers) = decompose(&op);
        assert!(
            !removed.contains(&100),
            "lease owner must be kept when no survivor exists; got removed={:?}",
            removed,
        );
        assert!(
            transfers.is_empty(),
            "no transfer can be emitted when survivor is absent; got {:?}",
            transfers,
        );
    }
}
