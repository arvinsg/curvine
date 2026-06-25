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

use curvine_common::utils::SerdeUtils;
use curvine_common::{FsError, FsResult};
use serde::{Deserialize, Serialize};

/// Structured result of a single `apply_*` invocation in PD modules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplyOutcome {
    /// Apply succeeded.
    Applied,

    /// Apply was a no-op because the target state already matches the entry.
    /// Caller should treat this as success (idempotent).
    SkippedNoop,

    /// Apply was rejected by an explicit CAS guard (epoch / version / state mismatch).
    SkippedStale { reason: String },

    /// Target object does not exist.
    NotFound { reason: String },
}

impl ApplyOutcome {
    pub fn stale(reason: impl Into<String>) -> Self {
        Self::SkippedStale {
            reason: reason.into(),
        }
    }

    pub fn not_found(reason: impl Into<String>) -> Self {
        Self::NotFound {
            reason: reason.into(),
        }
    }

    /// Returns true if the apply mutated state (or was a no-op idempotent success).
    pub fn is_success(&self) -> bool {
        matches!(self, ApplyOutcome::Applied | ApplyOutcome::SkippedNoop)
    }

    /// Serialize to bytes for Raft.
    pub fn encode(&self) -> FsResult<Vec<u8>> {
        SerdeUtils::serialize(self).map_err(Into::into)
    }

    /// Decode from `ProposeResponse.apply_result` bytes.
    pub fn decode(bytes: &[u8]) -> FsResult<Self> {
        if bytes.is_empty() {
            return Ok(ApplyOutcome::Applied);
        }
        SerdeUtils::deserialize(bytes).map_err(Into::into)
    }

    /// Convert this outcome into a propose-path `Result`. Successful outcomes
    /// (Applied / SkippedNoop) become `Ok(())`.
    pub fn into_propose_result(self, kind: &str) -> FsResult<()> {
        match self {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => Ok(()),
            ApplyOutcome::SkippedStale { reason } => {
                Err(FsError::stale_entry(kind, "matching guard", reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applied_round_trips() {
        let bytes = ApplyOutcome::Applied.encode().unwrap();
        assert_eq!(ApplyOutcome::decode(&bytes).unwrap(), ApplyOutcome::Applied);
    }

    #[test]
    fn skipped_stale_carries_reason() {
        let original = ApplyOutcome::stale("epoch mismatch: expected=5, actual=7");
        let bytes = original.encode().unwrap();
        let decoded = ApplyOutcome::decode(&bytes).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn not_found_carries_reason() {
        let original = ApplyOutcome::not_found("pool 4 absent");
        let bytes = original.encode().unwrap();
        let decoded = ApplyOutcome::decode(&bytes).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn empty_bytes_decode_as_applied() {
        // Forward-compat: un-upgraded leaders return empty apply_result.
        assert_eq!(ApplyOutcome::decode(&[]).unwrap(), ApplyOutcome::Applied);
    }

    #[test]
    fn is_success_distinguishes_outcomes() {
        assert!(ApplyOutcome::Applied.is_success());
        assert!(ApplyOutcome::SkippedNoop.is_success());
        assert!(!ApplyOutcome::stale("x").is_success());
        assert!(!ApplyOutcome::not_found("x").is_success());
    }

    #[test]
    fn into_propose_result_maps_outcomes() {
        assert!(ApplyOutcome::Applied.into_propose_result("test").is_ok());
        assert!(ApplyOutcome::SkippedNoop
            .into_propose_result("test")
            .is_ok());

        let stale_err = ApplyOutcome::stale("epoch=5 vs 7")
            .into_propose_result("save_pool")
            .unwrap_err();
        assert!(matches!(stale_err, FsError::StaleEntry(_)));

        let nf_err = ApplyOutcome::not_found("pool 4")
            .into_propose_result("save_pool")
            .unwrap_err();
        assert!(matches!(nf_err, FsError::NotFound(_)));
    }
}
