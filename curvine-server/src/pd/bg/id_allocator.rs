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
use crate::pd::journal::{self, entry::BatchBGEntry, PdEntry};
use curvine_common::FsResult;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn pack(next: u32, end: u32) -> u64 {
    ((next as u64) << 32) | (end as u64)
}

fn unpack(v: u64) -> (u32, u32) {
    ((v >> 32) as u32, v as u32)
}

/// Pre-allocates BG ID ranges to reduce Raft proposal frequency.
///
/// `range` packs `(next, end)` into a single AtomicU64 so that alloc reads
/// both values atomically.
pub struct IdAllocator {
    store: Arc<BGStore>,
    journal_client: Arc<journal::Client>,
    range: AtomicU64,
    step: u32,
    alloc_lock: Mutex<()>,
}

impl IdAllocator {
    const DEFAULT_STEP: u32 = 4096;

    pub fn new(store: Arc<BGStore>, journal_client: Arc<journal::Client>) -> Self {
        Self {
            store,
            journal_client,
            range: AtomicU64::new(pack(0, 0)),
            step: Self::DEFAULT_STEP,
            alloc_lock: Mutex::new(()),
        }
    }

    pub fn restore(&self) -> FsResult<()> {
        let base = self.store.get_next_bg_id()?;
        self.range.store(pack(base, base), Ordering::SeqCst);
        Ok(())
    }

    pub fn alloc(&self, count: u32) -> FsResult<u32> {
        loop {
            let r = self.range.load(Ordering::SeqCst);
            let (next, end) = unpack(r);
            if next + count <= end {
                let new_r = pack(next + count, end);
                if self
                    .range
                    .compare_exchange(r, new_r, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    return Ok(next);
                }
                continue;
            }
            self.realloc(count)?;
        }
    }

    fn realloc(&self, min_count: u32) -> FsResult<()> {
        let _lock = self.alloc_lock.lock().unwrap();
        let (next, end) = unpack(self.range.load(Ordering::SeqCst));
        if next + min_count <= end {
            return Ok(());
        }
        let alloc_size = self.step.max(min_count);
        let base = self.store.get_next_bg_id()?;
        let new_end = base + alloc_size;

        let entry = BatchBGEntry {
            op_ms: orpc::common::LocalTime::mills(),
            table: None,
            creates: vec![],
            updates: vec![],
            next_bg_id: Some(new_end),
            new_table_epoch: None,
        };
        self.journal_client.propose(PdEntry::BatchBG(entry))?;

        self.range.store(pack(base, new_end), Ordering::SeqCst);
        Ok(())
    }
}
