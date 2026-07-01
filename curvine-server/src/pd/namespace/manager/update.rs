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

use super::create::apply_outcome_to_result;
use super::*;
use curvine_common::state::UpdateNamespaceRequest;

impl NamespaceManager {
    pub fn update_namespace(&self, patch: UpdateNamespaceRequest) -> FsResult<()> {
        let current = self
            .get_namespace(patch.id)
            .ok_or_else(|| FsError::not_found(format!("namespace {} not found", patch.id)))?;

        let namespace_id = current.id;
        let now = LocalTime::mills();
        self.apply_patch(&current, &patch, now)?;

        let entry = NamespaceUpdateEntry {
            op_ms: now,
            expected_version: current.version,
            patch,
        };
        let outcome = self
            .journal_client
            .propose(PdEntry::UpdateNamespace(entry))?;
        apply_outcome_to_result(outcome, namespace_id)
    }

    fn apply_patch(
        &self,
        current: &NamespaceInfo,
        patch: &UpdateNamespaceRequest,
        op_ms: u64,
    ) -> FsResult<NamespaceInfo> {
        let mut updated = current.clone();
        if let Some(block_size) = patch.block_size {
            updated.block_size = block_size;
        }
        if let Some(default_ttl_ms) = patch.default_ttl_ms {
            updated.default_ttl_ms = default_ttl_ms;
        }
        if let Some(ttl_action) = patch.ttl_action {
            updated.ttl_action = ttl_action;
        }
        if let Some(cache_replica_policy) = patch.cache_replica_policy.clone() {
            updated.cache_replica_policy = cache_replica_policy;
        }
        if let Some(properties) = patch.properties.clone() {
            updated.properties = properties;
        }
        updated.version = current.version + 1;
        updated.update_time_ms = op_ms;

        self.validate_update_result(current, &updated)?;
        Ok(updated)
    }

    pub fn apply_update_namespace(&self, entry: &NamespaceUpdateEntry) -> FsResult<ApplyOutcome> {
        let mut index = self.index.write().unwrap();
        let Some(current) = index.get_by_id(entry.patch.id) else {
            return Ok(ApplyOutcome::not_found(format!(
                "namespace {} not found",
                entry.patch.id
            )));
        };

        if current.version != entry.expected_version {
            return Ok(ApplyOutcome::stale(format!(
                "namespace {} version mismatch: current={}, expected={}",
                entry.patch.id, current.version, entry.expected_version
            )));
        }

        let updated = match self.apply_patch(&current, &entry.patch, entry.op_ms) {
            Ok(updated) => updated,
            Err(e) => {
                log::warn!(
                    "apply_update_namespace: rejecting malformed committed entry id={}: {}",
                    entry.patch.id,
                    e
                );
                return Ok(ApplyOutcome::stale(format!(
                    "malformed update entry: {}",
                    e
                )));
            }
        };

        let plan = self
            .bgtable_manager
            .plan_namespace_policy_update(updated.id, &updated.cache_replica_policy)?;

        let mut ops = vec![self.store.namespace_put_op(&updated)?];
        ops.extend(plan.ops.iter().cloned());
        self.store.commit_batch(ops)?;

        self.bgtable_manager.commit_namespace_policy_update(plan);
        index.insert(updated);
        Ok(ApplyOutcome::Applied)
    }

    #[cfg(test)]
    pub fn test_build_update_entry(
        &self,
        patch: UpdateNamespaceRequest,
    ) -> FsResult<NamespaceUpdateEntry> {
        let current = self
            .get_namespace(patch.id)
            .ok_or_else(|| FsError::not_found(format!("namespace {} not found", patch.id)))?;
        let now = LocalTime::mills();
        self.apply_patch(&current, &patch, now)?;
        Ok(NamespaceUpdateEntry {
            op_ms: now,
            expected_version: current.version,
            patch,
        })
    }
}
