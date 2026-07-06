use super::HashBGTableControl;
use crate::pd::bgtable::placement::{
    build_worker_snapshots, create_hash_policy, BuildHashTableResult, HashPlacementContext,
    HashPlacementPolicy, HashPlanner, HashTableSpec, LabelConstraint, LabelOp, PendingInfluence,
    PlacementContext, PlacementRule, RebuildOptions, WorkerLoadSnapshot,
};
use crate::pd::bgtable::{BGTable, BGTableControl};
use crate::pd::config::keys;
use crate::pd::journal::entry::{BGBatchUpdateEntry, BGUpdateEntry};
use curvine_common::state::{
    BgId, BlockGroupInfo, CacheReplicaPolicy, LabelMatch, NamespaceId, StorageType, TableId,
};
use curvine_common::{FsError, FsResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

struct PlacementInputs {
    workers: HashMap<u32, WorkerLoadSnapshot>,
    rule: PlacementRule,
    policy: Box<dyn HashPlacementPolicy>,
}

struct RebuildPlan {
    changes: Vec<(BlockGroupInfo, BlockGroupInfo)>,
}

/// Stateless placement/rebuild algorithms for Hash BGTables.
///
/// Borrows its owning `HashBGTableControl` for table reads/writes and the
/// hash-domain dependencies (pool/bg/config managers). Keeping these algorithms
/// off the controller keeps the controller's job to table storage; the service
/// is the "decision" layer that operates on that state.
pub struct HashPlacement<'a> {
    controller: &'a HashBGTableControl,
}

impl<'a> HashPlacement<'a> {
    pub(super) fn new(controller: &'a HashBGTableControl) -> Self {
        Self { controller }
    }

    fn balance_policy_strategy(&self) -> String {
        self.controller
            .config_manager
            .get_string(keys::PD_BG_BALANCE_POLICY)
    }

    fn rebuild_tolerant_ratio(&self) -> f64 {
        self.controller
            .config_manager
            .get_u32(keys::PD_BG_REBUILD_TOLERANT_RATIO_BPS) as f64
            / 10_000.0
    }

    fn build_context<'c>(
        &self,
        inputs: &'c PlacementInputs,
        table_id: TableId,
        bucket_count: u32,
        replica_count: u16,
    ) -> HashPlacementContext<'c> {
        let tolerant = self.rebuild_tolerant_ratio();
        HashPlacementContext {
            common: PlacementContext {
                workers: &inputs.workers,
                tolerant_ratio: tolerant,
                primary_tolerant_ratio: tolerant,
            },
            table_id,
            bucket_count,
            replica_count,
        }
    }

    /// Build per-table WorkerLoadSnapshot map for the given table via the shared
    /// snapshot builder. On `init` (new table) there are no existing BGs, so all
    /// counts are zero. The control path carries no operator influence, so
    /// pending deltas are default (zero) — only the scheduler passes real
    /// `PendingInfluence`.
    fn build_hash_worker_snapshots(
        &self,
        table: &BGTable,
        media: StorageType,
        init: bool,
    ) -> HashMap<u32, WorkerLoadSnapshot> {
        let live_workers = self
            .controller
            .pool_manager
            .get_live_workers(table.storage_type());
        let table_bgs = if init {
            Vec::new()
        } else {
            self.table_bgs(table)
        };
        build_worker_snapshots(
            &live_workers,
            &table_bgs,
            &self.controller.pool_manager,
            &PendingInfluence::default(),
            media,
        )
    }

    /// Existing BGs of a Hash table, resolved from the bucket list.
    fn table_bgs(&self, table: &BGTable) -> Vec<Arc<BlockGroupInfo>> {
        let bgs = self.controller.bg_manager.snapshot_all_bgs();
        table
            .hash_table()
            .expect("hash table")
            .buckets()
            .iter()
            .filter_map(|&id| bgs.get(&id).cloned())
            .collect()
    }

    pub fn build_hash_table_plan(
        &self,
        table_id: TableId,
        namespace_id: NamespaceId,
        pool_type: StorageType,
        bucket_count: u32,
        replica_count: u16,
        next_bg_id: BgId,
        worker_labels: Vec<LabelMatch>,
        cache_replica_policy: CacheReplicaPolicy,
    ) -> FsResult<BuildHashTableResult> {
        let table_for_snapshot = BGTable::new_hash_table_with_config(
            table_id,
            namespace_id,
            pool_type,
            replica_count,
            Vec::new(),
            worker_labels.clone(),
            cache_replica_policy.clone(),
        );
        let inputs = self.prepare_placement_inputs(pool_type, &table_for_snapshot, true)?;
        let ctx = self.build_context(&inputs, table_id, bucket_count, replica_count);
        let mut state = inputs.policy.prepare_hash(&ctx)?;
        let spec = HashTableSpec {
            namespace_id,
            pool_type,
            next_bg_id,
            worker_labels,
            cache_replica_policy,
        };
        HashPlanner::new(&ctx, &inputs.rule, inputs.policy.as_ref()).build_table(&spec, &mut state)
    }

    /// Select replacement workers for an existing BG.
    /// Builds a per-table snapshot and runs the full Rule → Policy pipeline.
    /// Existing replicas are excluded. Returns up to `count` distinct workers.
    pub fn select_replacement_workers(
        &self,
        bg: &BlockGroupInfo,
        count: u16,
    ) -> FsResult<Vec<u32>> {
        let table = self
            .controller
            .get_table(bg.table_id)
            .ok_or_else(|| FsError::common(format!("table {} not found", bg.table_id)))?;
        let inputs = self.prepare_placement_inputs(table.storage_type(), &table, false)?;

        let ht = table.hash_table().expect("hash table");
        let ctx = self.build_context(
            &inputs,
            table.table_id(),
            ht.bucket_count(),
            table.replica_count(),
        );
        let mut state = inputs.policy.prepare_hash(&ctx)?;
        let planner = HashPlanner::new(&ctx, &inputs.rule, inputs.policy.as_ref());

        let mut selected: Vec<u32> = Vec::with_capacity(count as usize);
        let mut exclude: HashSet<u32> = bg.replica_set.iter().copied().collect();

        for _ in 0..count {
            let current: Vec<u32> = bg
                .replica_set
                .iter()
                .copied()
                .chain(selected.iter().copied())
                .collect();

            let Some(picked) = planner.select_with_fallback(&current, &exclude, &mut state) else {
                break;
            };
            selected.push(picked);
            exclude.insert(picked);
            state.record_bg_change(None, picked);
        }

        if selected.len() < count as usize {
            return Err(FsError::common(format!(
                "selector returned {} workers for BG {}, need {}",
                selected.len(),
                bg.bg_id,
                count
            )));
        }
        Ok(selected)
    }

    /// Used by the scheduler to inspect what a rebuild would change.
    pub fn compute_rebuild_diff(
        &self,
        table_id: TableId,
    ) -> FsResult<Vec<(BlockGroupInfo, Vec<u32>)>> {
        Ok(self
            .plan_rebuild(table_id)?
            .map(|p| {
                p.changes
                    .into_iter()
                    .map(|(new_bg, old_bg)| (new_bg, old_bg.replica_set))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Plan and persist: propose a Raft batch entry that applies the rebuild.
    pub fn rebuild_hash_table(&self, table_id: TableId) -> FsResult<()> {
        let Some(plan) = self.plan_rebuild(table_id)? else {
            return Ok(());
        };
        if plan.changes.is_empty() {
            return Ok(());
        }
        let entry = self.build_rebuild_batch_entry(&plan);
        self.controller
            .bg_manager
            .propose_batch_update_bg(entry)?
            .into_propose_result("rebuild_hash_table")
    }

    /// Plan-only side of rebuild: read table + existing BGs,
    /// run the placement algorithm, return the diff.
    /// Returns `None` if the table has no existing BGs to rebuild.
    fn plan_rebuild(&self, table_id: TableId) -> FsResult<Option<RebuildPlan>> {
        let table_arc = self
            .controller
            .get_table(table_id)
            .ok_or_else(|| FsError::common(format!("table {} not found", table_id)))?;
        let table: BGTable = (*table_arc).clone();
        let ht = table.hash_table().expect("hash table");

        let existing_bgs: Vec<BlockGroupInfo> = {
            let bgs = self.controller.bg_manager.snapshot_all_bgs();
            ht.buckets()
                .iter()
                .filter(|&&id| id != 0)
                .filter_map(|id| bgs.get(id).map(|arc| (**arc).clone()))
                .collect()
        };
        if existing_bgs.is_empty() {
            return Ok(None);
        }

        let bucket_count = ht.bucket_count();
        let inputs = self.prepare_placement_inputs(table.storage_type(), &table, false)?;
        let ctx = self.build_context(
            &inputs,
            table.table_id(),
            bucket_count,
            table.replica_count(),
        );
        let mut state = inputs.policy.prepare_hash(&ctx)?;

        let result = HashPlanner::new(&ctx, &inputs.rule, inputs.policy.as_ref()).rebuild_table(
            &existing_bgs,
            &RebuildOptions::default(),
            &mut state,
        )?;

        let old_by_id: HashMap<BgId, BlockGroupInfo> = existing_bgs
            .iter()
            .map(|bg| (bg.bg_id, bg.clone()))
            .collect();
        let changes = result
            .updated_bgs
            .into_iter()
            .map(|bg| {
                let old = old_by_id
                    .get(&bg.bg_id)
                    .cloned()
                    .expect("placement result must come from existing BG");
                (bg, old)
            })
            .collect();
        Ok(Some(RebuildPlan { changes }))
    }

    /// Serialize a planned rebuild into a single batched journal entry,
    /// bumping the table's epoch.
    fn build_rebuild_batch_entry(&self, plan: &RebuildPlan) -> BGBatchUpdateEntry {
        let now = orpc::common::LocalTime::mills();
        let updates = plan
            .changes
            .iter()
            .map(|(new_bg, old_bg)| {
                let primary_changed = new_bg.primary != old_bg.primary;
                let isr_changed = new_bg.isr != old_bg.isr;
                BGUpdateEntry {
                    op_ms: now,
                    kind: new_bg.kind,
                    bg_id: new_bg.bg_id,
                    state: None,
                    replica_set: Some(new_bg.replica_set.clone()),
                    isr: Some(new_bg.isr.clone()),
                    primary: primary_changed.then(|| new_bg.primary.clone()),
                    expected_bg_epoch: old_bg.bg_epoch,
                    // Hash: a serving-set change (isr) or a primary change is
                    // client-visible and bumps the table epoch; a pure
                    // replica_set move does not.
                    bump_table_epoch: isr_changed || primary_changed,
                }
            })
            .collect();
        BGBatchUpdateEntry {
            op_ms: now,
            updates,
        }
    }

    /// Rebuild every Hash table in a pool via `rebuild_hash_table`; a per-table
    /// failure is logged and skipped so one bad table never aborts the sweep.
    pub fn rebuild_tables_for_pool(&self, pool_type: StorageType) -> FsResult<()> {
        let table_ids: Vec<TableId> = self
            .controller
            .list_tables()
            .into_iter()
            .filter(|t| t.storage_type() == pool_type)
            .map(|t| t.table_id())
            .collect();
        for table_id in table_ids {
            if let Err(e) = self.rebuild_hash_table(table_id) {
                log::error!("Failed to rebuild table {}: {}", table_id, e);
            }
        }
        Ok(())
    }

    /// Build placement rule from global config + static location labels.
    pub fn placement_rule(&self) -> PlacementRule {
        let policy_name = self
            .controller
            .config_manager
            .get_string(keys::PD_BG_PLACEMENT_POLICY);
        let min_iso = self
            .controller
            .config_manager
            .get_string(keys::PD_BG_MIN_ISOLATION_LEVEL);
        let min_isolation_level = if min_iso.is_empty() {
            None
        } else {
            Some(min_iso)
        };

        match policy_name.as_str() {
            keys::PD_BG_PLACEMENT_POLICY_TOPOLOGY_AWARE => PlacementRule {
                id: keys::PD_BG_PLACEMENT_POLICY_TOPOLOGY_AWARE.into(),
                label_constraints: vec![],
                location_labels: self.controller.location_labels.clone(),
                min_isolation_level,
            },
            _ => PlacementRule {
                min_isolation_level,
                ..PlacementRule::default_rule()
            },
        }
    }

    /// Build placement rule for a specific table, including namespace/table
    /// worker label constraints.
    pub fn placement_rule_for_table(&self, table: &BGTable) -> PlacementRule {
        let mut rule = self.placement_rule();
        for label in table.worker_labels() {
            rule.label_constraints.push(LabelConstraint {
                key: label.key.clone(),
                op: LabelOp::In,
                values: vec![label.value.clone()],
            });
        }
        rule
    }

    /// Assemble the worker snapshot, placement rule, and balance policy for `pool_type`.
    fn prepare_placement_inputs(
        &self,
        pool_type: StorageType,
        table_for_snapshot: &BGTable,
        for_create: bool,
    ) -> FsResult<PlacementInputs> {
        let pool = self.controller.pool_manager.get_pool(pool_type)?;
        let workers = self.build_hash_worker_snapshots(table_for_snapshot, pool.media, for_create);
        let rule = self.placement_rule_for_table(table_for_snapshot);
        let policy = create_hash_policy(&self.balance_policy_strategy());
        Ok(PlacementInputs {
            workers,
            rule,
            policy,
        })
    }
}
