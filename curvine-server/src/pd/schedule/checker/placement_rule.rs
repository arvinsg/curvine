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

use crate::pd::bg::placement::{
    check_isolation_violation, isolation_score, worker_passes_constraints, worst_replica,
};
use crate::pd::schedule::ManagerContext;
use crate::pd::schedule::{BGOperator, OpPriority, OperatorBuilder, OperatorKind};
use curvine_common::state::{BlockGroupInfo, ReplicaState};

pub struct PlacementRuleChecker;

impl super::Checker for PlacementRuleChecker {
    fn name(&self) -> &str {
        "placement-rule-checker"
    }

    fn check_bg(&self, bg: &BlockGroupInfo, ctx: &ManagerContext) -> Option<BGOperator> {
        if super::bg_in_leaving_grace(bg, ctx) {
            return None;
        }

        let rule = ctx.bg_manager.placement_rule();
        let min_level = rule.min_isolation_level.as_ref()?;

        let resident = ctx.bg_manager.get_resident_replicas(bg.bg_id);
        if resident.len() < 2 {
            return None;
        }

        let worker_labels = ctx.pool_manager.get_workers_labels(&resident);
        if !check_isolation_violation(&resident, min_level, &rule.location_labels, &worker_labels) {
            return None;
        }

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

            let passes =
                worker_passes_constraints(candidate, &combined_labels, &rule.label_constraints);
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
            let to_worker = ctx.pick_lease_fallback(bg, worst_worker, new_worker);
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
    use crate::pd::pool::POOL_ID_SSD;
    use crate::pd::schedule::checker::tests_common::{decompose, Fixture};
    use crate::pd::schedule::checker::Checker;

    #[test]
    fn name_and_priority() {
        let checker = PlacementRuleChecker;
        assert_eq!(checker.name(), "placement-rule-checker");
        assert_eq!(checker.priority(), 30);
    }

    #[test]
    fn no_op_when_no_min_isolation_level() {
        // topology_aware policy but min_isolation_level=None → no op.
        let f = Fixture::with_topology(vec!["az"], None);
        f.add_worker(100, POOL_ID_SSD, &[("az", "a")]);
        f.add_worker(101, POOL_ID_SSD, &[("az", "a")]);
        f.add_worker(102, POOL_ID_SSD, &[("az", "b")]);
        let table_id = f.insert_table(POOL_ID_SSD, 3);
        let bg = f.insert_bg(1, table_id, vec![100, 101, 102], None);
        assert!(PlacementRuleChecker.check_bg(&bg, &f.ctx).is_none());
    }

    #[derive(Debug)]
    enum Expect {
        None,
        Fix {
            // replicas whose worker has the violating label → one of these should be removed.
            worst_candidates: Vec<u32>,
            // candidates that could legally be used as the replacement (passing label constraints).
            new_candidates: Vec<u32>,
            expect_lease_transfer: bool,
        },
    }

    struct Case {
        name: &'static str,
        replica_set: Vec<u32>,
        lease_owner: u32,
        /// (worker_id, pool, labels) — all workers registered as Live.
        workers: Vec<(u32, &'static [(&'static str, &'static str)])>,
        expect: Expect,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "no violation — all in distinct AZs",
                replica_set: vec![100, 101, 102],
                lease_owner: 100,
                workers: vec![
                    (100, &[("az", "a")]),
                    (101, &[("az", "b")]),
                    (102, &[("az", "c")]),
                ],
                expect: Expect::None,
            },
            Case {
                name: "only one resident replica — skipped",
                replica_set: vec![100],
                lease_owner: 100,
                workers: vec![(100, &[("az", "a")])],
                expect: Expect::None,
            },
            Case {
                name: "two replicas share AZ, no alternative candidate",
                replica_set: vec![100, 101],
                lease_owner: 100,
                workers: vec![(100, &[("az", "a")]), (101, &[("az", "a")])],
                expect: Expect::None,
            },
            Case {
                name: "two share AZ, a third worker in another AZ → swap the shared one",
                replica_set: vec![100, 101, 200],
                lease_owner: 100,
                workers: vec![
                    (100, &[("az", "a")]),
                    (101, &[("az", "a")]),
                    (200, &[("az", "b")]),
                    // Free candidate in a fresh AZ.
                    (300, &[("az", "c")]),
                ],
                expect: Expect::Fix {
                    worst_candidates: vec![100, 101],
                    new_candidates: vec![300],
                    expect_lease_transfer: true, // lease owner 100 is one of the worst candidates
                },
            },
            Case {
                name: "shared AZ but worst is not lease owner → no TransferLease",
                replica_set: vec![100, 101, 200],
                lease_owner: 200, // different AZ than 100/101
                workers: vec![
                    (100, &[("az", "a")]),
                    (101, &[("az", "a")]),
                    (200, &[("az", "b")]),
                    (300, &[("az", "c")]),
                ],
                expect: Expect::Fix {
                    worst_candidates: vec![100, 101],
                    new_candidates: vec![300],
                    expect_lease_transfer: false,
                },
            },
        ]
    }

    #[test]
    fn table_driven_check_bg() {
        for case in cases() {
            let f = Fixture::with_topology(vec!["az"], Some("az"));
            for (wid, labels) in &case.workers {
                f.add_worker(*wid, POOL_ID_SSD, labels);
            }
            let table_id = f.insert_table(POOL_ID_SSD, 3);
            let bg = f.insert_bg(
                1,
                table_id,
                case.replica_set.clone(),
                Some(case.lease_owner),
            );

            let op = PlacementRuleChecker.check_bg(&bg, &f.ctx);
            match case.expect {
                Expect::None => {
                    assert!(op.is_none(), "{}: expected None, got {:?}", case.name, op);
                }
                Expect::Fix {
                    worst_candidates,
                    new_candidates,
                    expect_lease_transfer,
                } => {
                    let op = op.unwrap_or_else(|| panic!("{}: expected Fix op", case.name));
                    assert_eq!(
                        op.priority,
                        OpPriority::PLACEMENT_FIX,
                        "{}: priority",
                        case.name
                    );
                    let (add, remove, transfer) = decompose(&op);

                    assert_eq!(add.len(), 1, "{}: one Add step", case.name);
                    assert_eq!(remove.len(), 1, "{}: one Remove step", case.name);
                    assert!(
                        worst_candidates.contains(&remove[0]),
                        "{}: removed worker {} not in worst set {:?}",
                        case.name,
                        remove[0],
                        worst_candidates
                    );
                    assert!(
                        new_candidates.contains(&add[0]),
                        "{}: added worker {} not in new set {:?}",
                        case.name,
                        add[0],
                        new_candidates
                    );

                    if expect_lease_transfer {
                        assert_eq!(transfer.len(), 1, "{}: expected lease transfer", case.name);
                        assert_eq!(
                            transfer[0].0, remove[0],
                            "{}: transfer from worst replica",
                            case.name
                        );
                    } else {
                        assert!(transfer.is_empty(), "{}: no lease transfer", case.name);
                    }
                }
            }
        }
    }
}
