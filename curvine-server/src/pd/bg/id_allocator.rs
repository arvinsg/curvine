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

use super::BGStore;
use crate::pd::journal::entry::BGIdAllocatorEntry;
use crate::pd::journal::{self, ApplyOutcome, PdEntry};
use curvine_common::state::BgId;
use curvine_common::{FsError, FsResult};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, Default)]
struct IdRange {
    next: BgId,
    end: BgId,
}

impl IdRange {
    fn has_capacity(&self, count: u64) -> bool {
        self.next
            .checked_add(count)
            .map(|new_next| new_next <= self.end)
            .unwrap_or(false)
    }

    fn alloc(&mut self, count: u64) -> FsResult<BgId> {
        let base = self.next;
        self.next = self.next.checked_add(count).ok_or_else(|| {
            FsError::common(format!(
                "BG id allocation overflow: base={base}, count={count}"
            ))
        })?;
        Ok(base)
    }
}

/// Leader-local BG id range allocator.
///
/// The allocator reserves monotonically increasing id ranges through Raft and
/// then serves ids from memory. Unused ids in a reserved range may be skipped
/// after leader changes; ids are never reused.
pub struct BgIdAllocator {
    store: Arc<BGStore>,
    journal_client: Arc<journal::Client>,
    range: Mutex<IdRange>,
    step: u64,
}

impl BgIdAllocator {
    const DEFAULT_STEP: u64 = 4096;

    pub fn new(store: Arc<BGStore>, journal_client: Arc<journal::Client>) -> Self {
        Self::with_step(store, journal_client, Self::DEFAULT_STEP)
    }

    pub fn with_step(store: Arc<BGStore>, journal_client: Arc<journal::Client>, step: u64) -> Self {
        Self {
            store,
            journal_client,
            range: Mutex::new(IdRange::default()),
            step: step.max(1),
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let next_id = self.store.get_next_bg_id()?;
        *self.range.lock().unwrap() = IdRange {
            next: next_id,
            end: next_id,
        };
        Ok(())
    }

    pub fn ensure_next_id_at_least(&self, floor: BgId) -> FsResult<Option<(BgId, BgId)>> {
        let current = self.store.get_next_bg_id()?;
        if current >= floor {
            return Ok(None);
        }

        self.store.set_next_bg_id(floor)?;
        *self.range.lock().unwrap() = IdRange {
            next: floor,
            end: floor,
        };
        Ok(Some((current, floor)))
    }

    pub fn alloc(&self, count: u64) -> FsResult<BgId> {
        if count == 0 {
            return Err(FsError::common("BG id allocation count must be positive"));
        }

        let mut range = self.range.lock().unwrap();
        if !range.has_capacity(count) {
            self.reserve_locked(&mut range, count)?;
        }
        range.alloc(count)
    }

    fn reserve_locked(&self, range: &mut IdRange, min_count: u64) -> FsResult<()> {
        if range.has_capacity(min_count) {
            return Ok(());
        }

        let base = self.store.get_next_bg_id()?;
        let reserve_count = self.step.max(min_count);
        let end = base.checked_add(reserve_count).ok_or_else(|| {
            FsError::common(format!(
                "BG id range overflow: base={base}, reserve_count={reserve_count}"
            ))
        })?;

        let entry = BGIdAllocatorEntry {
            op_ms: orpc::common::LocalTime::mills(),
            expected_next_bg_id: base,
            next_bg_id: end,
        };
        match self.journal_client.propose(PdEntry::AllocateBGId(entry))? {
            ApplyOutcome::Applied | ApplyOutcome::SkippedNoop => {
                *range = IdRange { next: base, end };
                Ok(())
            }
            ApplyOutcome::SkippedStale { reason } => {
                Err(FsError::stale_entry("reserve_bg_id_range", base, reason))
            }
            ApplyOutcome::NotFound { reason } => Err(FsError::not_found(reason)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_capacity_cases() {
        let cases = vec![
            (IdRange { next: 1, end: 5 }, 4, true),
            (IdRange { next: 1, end: 5 }, 5, false),
            (
                IdRange {
                    next: u64::MAX,
                    end: u64::MAX,
                },
                1,
                false,
            ),
        ];
        for (range, count, expected) in cases {
            assert_eq!(range.has_capacity(count), expected);
        }
    }

    #[test]
    fn range_alloc_advances_next() {
        let mut range = IdRange { next: 10, end: 20 };
        assert_eq!(range.alloc(3).unwrap(), 10);
        assert_eq!(range.next, 13);
    }
}
