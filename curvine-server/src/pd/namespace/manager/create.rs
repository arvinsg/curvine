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

use super::*;

impl NamespaceManager {
    pub fn create_namespace(&self, request: CreateNamespaceRequest) -> FsResult<()> {
        self.validate_create_request(&request)?;
        if self.get_namespace_by_name(&request.name).is_some() {
            return Err(FsError::already_exists(format!(
                "namespace {} already exists",
                request.name
            )));
        }

        let namespace_id = self.store.get_next_namespace_id()?;
        let bg_count = hash_tier_bg_count(&request)?;
        let first_bg_id = self.bgtable_manager.alloc_bg_ids(bg_count)?;
        let entry = self.build_create_entry(request, namespace_id, first_bg_id)?;
        let outcome = self
            .journal_client
            .propose(PdEntry::CreateNamespace(entry))?;
        apply_outcome_to_result(outcome, namespace_id)
    }

    fn build_create_entry(
        &self,
        request: CreateNamespaceRequest,
        namespace_id: NamespaceId,
        first_bg_id: BgId,
    ) -> FsResult<NamespaceCreateEntry> {
        if namespace_id > MAX_NAMESPACE_ID {
            return Err(FsError::common("namespace id exhausted"));
        }
        let next_namespace_id = namespace_id + 1;

        let now = LocalTime::mills();
        let (cache_tier_tables, tables, bgs) =
            self.build_hash_tables(&request, namespace_id, first_bg_id)?;
        let namespace = assemble_namespace(request, namespace_id, cache_tier_tables, now);

        Ok(NamespaceCreateEntry {
            op_ms: now,
            namespace,
            tables,
            bgs,
            expected_next_namespace_id: namespace_id,
            next_namespace_id,
        })
    }

    fn build_hash_tables(
        &self,
        request: &CreateNamespaceRequest,
        namespace_id: NamespaceId,
        first_bg_id: BgId,
    ) -> FsResult<(Vec<TableId>, Vec<BGTable>, Vec<BlockGroupInfo>)> {
        let tier_count = request.cache_tier_config.pools.len();
        let mut cache_tier_tables = Vec::with_capacity(tier_count);
        let mut tables = Vec::with_capacity(tier_count);
        let mut bgs = Vec::new();
        let mut next_bg_id = first_bg_id;

        for (table_index, pool_type) in request.cache_tier_config.pools.iter().copied().enumerate()
        {
            let table_id = make_table_id(namespace_id, table_index as u8)?;
            let plan = self.bgtable_manager.build_hash_table_plan(
                table_id,
                namespace_id,
                pool_type,
                request.cache_tier_config.bucket_count,
                request.cache_tier_config.replica_count,
                next_bg_id,
                request.cache_tier_config.worker_labels.clone(),
                request.cache_replica_policy.clone(),
            )?;
            next_bg_id += plan.bgs.len() as u64;
            cache_tier_tables.push(table_id);
            tables.push(plan.table);
            bgs.extend(plan.bgs);
        }
        Ok((cache_tier_tables, tables, bgs))
    }

    pub fn apply_create_namespace(
        &self,
        entry: &NamespaceCreateEntry,
        _is_leader: bool,
    ) -> FsResult<ApplyOutcome> {
        if let Err(e) = self.validate_create_entry(entry) {
            log::warn!(
                "apply_create_namespace: rejecting malformed committed entry id={}, name={}: {}",
                entry.namespace.id,
                entry.namespace.name,
                e
            );
            return Ok(ApplyOutcome::stale(format!(
                "malformed create entry: {}",
                e
            )));
        }

        let mut index = self.index.write().unwrap();
        if let Some(outcome) = self.check_existing_namespace(&index, entry)? {
            return Ok(outcome);
        }
        if let Some(outcome) = self.check_namespace_id_cas(entry)? {
            return Ok(outcome);
        }

        let namespace_ops = self.namespace_kv_writes(entry)?;
        let outcome = self.bgtable_manager.apply_namespace_bg_create(
            &entry.tables,
            &entry.bgs,
            namespace_ops,
        )?;
        if !matches!(outcome, ApplyOutcome::Applied | ApplyOutcome::SkippedNoop) {
            return Ok(outcome);
        }

        index.insert(entry.namespace.clone());
        Ok(outcome)
    }

    /// KvWrites for the namespace's own metadata (next-id counter + info),
    /// committed together with the BGTable/BG batch for atomicity.
    fn namespace_kv_writes(&self, entry: &NamespaceCreateEntry) -> FsResult<Vec<KvWrite>> {
        Ok(vec![
            self.store.next_namespace_id_op(entry.next_namespace_id)?,
            self.store.namespace_put_op(&entry.namespace)?,
        ])
    }

    /// Whether a committed namespace exactly matches this entry (idempotent
    /// replay): same NamespaceInfo bytes and the same BGTable/BG metadata.
    fn namespace_matches_entry(
        &self,
        existing: &NamespaceInfo,
        entry: &NamespaceCreateEntry,
    ) -> FsResult<bool> {
        Ok(Serde::serialize(existing)? == Serde::serialize(&entry.namespace)?)
    }

    /// Decide the apply outcome when the namespace id and/or name already exist
    /// in the index. Returns `None` only when both are free (a fresh create).
    fn check_existing_namespace(
        &self,
        index: &NamespaceIndex,
        entry: &NamespaceCreateEntry,
    ) -> FsResult<Option<ApplyOutcome>> {
        let by_id = index.get_by_id(entry.namespace.id);
        let by_name = index.get_by_name(&entry.namespace.name);

        // Fresh id and name.
        if by_id.is_none() && by_name.is_none() {
            return Ok(None);
        }

        // The id and name both resolve to one and the same record: this is
        // either an idempotent replay or a metadata conflict on that record.
        if let (Some(existing), Some(named)) = (&by_id, &by_name) {
            if existing.id == named.id {
                let outcome = if self.namespace_matches_entry(existing, entry)? {
                    ApplyOutcome::SkippedNoop
                } else {
                    ApplyOutcome::stale(format!(
                        "namespace already exists with different metadata: id={}, name={}",
                        entry.namespace.id, entry.namespace.name
                    ))
                };
                return Ok(Some(outcome));
            }
        }

        // Any remaining combination is a cross conflict: the id or the name is
        // already owned, and not by a record matching both keys of this entry.
        Ok(Some(ApplyOutcome::stale(format!(
            "namespace id/name conflict: id {} and name {} do not map to a single existing namespace",
            entry.namespace.id, entry.namespace.name
        ))))
    }

    fn check_namespace_id_cas(
        &self,
        entry: &NamespaceCreateEntry,
    ) -> FsResult<Option<ApplyOutcome>> {
        let current_next_ns = self.store.get_next_namespace_id()?;
        if current_next_ns != entry.expected_next_namespace_id {
            return Ok(Some(ApplyOutcome::stale(format!(
                "next_namespace_id mismatch: current={}, expected={}",
                current_next_ns, entry.expected_next_namespace_id
            ))));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub fn test_build_create_entry(
        &self,
        request: CreateNamespaceRequest,
        namespace_id: NamespaceId,
        first_bg_id: BgId,
    ) -> FsResult<NamespaceCreateEntry> {
        self.validate_create_request(&request)?;
        self.build_create_entry(request, namespace_id, first_bg_id)
    }
}

fn assemble_namespace(
    request: CreateNamespaceRequest,
    namespace_id: NamespaceId,
    cache_tier_tables: Vec<TableId>,
    now: u64,
) -> NamespaceInfo {
    NamespaceInfo {
        id: namespace_id,
        name: request.name,
        block_size: request.block_size,
        cache_tier_tables,
        write_buffer_table: None,
        cache_tier_config: request.cache_tier_config,
        write_buffer_config: None,
        cache_replica_policy: request.cache_replica_policy,
        default_ttl_ms: request.default_ttl_ms,
        ttl_action: request.ttl_action,
        version: 1,
        create_time_ms: now,
        update_time_ms: now,
        properties: request.properties,
    }
}

fn hash_tier_bg_count(request: &CreateNamespaceRequest) -> FsResult<u64> {
    let tier_count = request.cache_tier_config.pools.len() as u64;
    let bucket_count = request.cache_tier_config.bucket_count as u64;
    tier_count
        .checked_mul(bucket_count)
        .ok_or_else(|| FsError::common("namespace BG count overflow"))
}

fn apply_outcome_to_result(outcome: ApplyOutcome, namespace_id: NamespaceId) -> FsResult<()> {
    match outcome {
        ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
        ApplyOutcome::SkippedStale { reason } => Err(FsError::stale_entry(
            "create_namespace",
            namespace_id,
            reason,
        )),
        ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
    }
}
