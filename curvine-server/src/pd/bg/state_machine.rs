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

use curvine_common::state::{BGKind, BGState};
use curvine_common::{FsError, FsResult};

/// Validate persisted BG state transitions.
///
/// Hash BG uses Active/Degraded to expose health and never enters Sealed.
/// Capacity BG uses Sealed as the fail-fast write-buffer boundary and must never reopen.
pub fn validate_transition(kind: BGKind, current: BGState, target: BGState) -> FsResult<()> {
    if current == target {
        return Ok(());
    }
    let valid = match kind {
        BGKind::Hash => valid_hash_transition(current, target),
        BGKind::Capacity => valid_capacity_transition(current, target),
    };
    if valid {
        Ok(())
    } else {
        Err(FsError::common(format!(
            "invalid {:?} BG state transition: {:?} -> {:?}",
            kind, current, target
        )))
    }
}

fn valid_hash_transition(current: BGState, target: BGState) -> bool {
    matches!(
        (current, target),
        (BGState::Init, BGState::Active)
            | (BGState::Init, BGState::Degraded)
            | (BGState::Active, BGState::Degraded)
            | (BGState::Degraded, BGState::Active)
    )
}

fn valid_capacity_transition(current: BGState, target: BGState) -> bool {
    matches!(
        (current, target),
        (BGState::Init, BGState::Active) | (BGState::Active, BGState::Sealed)
    )
}

fn next_states(kind: BGKind, state: BGState) -> &'static [BGState] {
    match kind {
        BGKind::Hash => match state {
            BGState::Init => &[BGState::Active, BGState::Degraded],
            BGState::Active => &[BGState::Degraded],
            BGState::Degraded => &[BGState::Active],
            BGState::Sealed => &[],
        },
        BGKind::Capacity => match state {
            BGState::Init => &[BGState::Active],
            BGState::Active => &[BGState::Sealed],
            BGState::Degraded | BGState::Sealed => &[],
        },
    }
}

pub fn is_reachable(kind: BGKind, current: BGState, target: BGState) -> bool {
    if current == target {
        return true;
    }
    let mut visited: Vec<BGState> = Vec::new();
    let mut stack = vec![current];
    while let Some(s) = stack.pop() {
        if visited.contains(&s) {
            continue;
        }
        visited.push(s);
        for next in next_states(kind, s) {
            if *next == target {
                return true;
            }
            stack.push(*next);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_transitions_allow_degraded_but_do_not_allow_sealed() {
        assert!(validate_transition(BGKind::Hash, BGState::Init, BGState::Active).is_ok());
        assert!(validate_transition(BGKind::Hash, BGState::Active, BGState::Degraded).is_ok());
        assert!(validate_transition(BGKind::Hash, BGState::Degraded, BGState::Active).is_ok());
        assert!(validate_transition(BGKind::Hash, BGState::Active, BGState::Sealed).is_err());
        assert!(!is_reachable(
            BGKind::Hash,
            BGState::Active,
            BGState::Sealed
        ));
    }

    #[test]
    fn capacity_transitions_allow_seal_but_not_reopen() {
        assert!(validate_transition(BGKind::Capacity, BGState::Init, BGState::Active).is_ok());
        assert!(validate_transition(BGKind::Capacity, BGState::Active, BGState::Sealed).is_ok());
        assert!(validate_transition(BGKind::Capacity, BGState::Active, BGState::Degraded).is_err());
        assert!(validate_transition(BGKind::Capacity, BGState::Sealed, BGState::Active).is_err());
        assert!(!is_reachable(
            BGKind::Capacity,
            BGState::Sealed,
            BGState::Active
        ));
    }
}
