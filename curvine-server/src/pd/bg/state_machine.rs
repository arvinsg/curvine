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

use curvine_common::state::BGState;
use curvine_common::{FsError, FsResult};

/// Validate and execute BG state transitions:
///
/// ```text
/// Init ──assign──> Assigned/Active
///                        │
///                        ├──worker lost──> Degraded ──recover──> Recovering ──done──> Active
///                        │
///                        ├──table balance──> Rebalancing ──done──> Active
///                        │
///                        └──delete cmd──> Deleting
/// ```
pub fn validate_transition(current: BGState, target: BGState) -> FsResult<()> {
    let valid = match (current, target) {
        (BGState::Init, BGState::Assigned) => true,
        (BGState::Assigned, BGState::Active) => true,
        (BGState::Assigned, BGState::Degraded) => true,
        (BGState::Active, BGState::Degraded) => true,
        (BGState::Active, BGState::Rebalancing) => true,
        (BGState::Degraded, BGState::Recovering) => true,
        (BGState::Recovering, BGState::Active) => true,
        (BGState::Rebalancing, BGState::Active) => true,
        (_, BGState::Deleting) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(FsError::common(format!(
            "invalid BG state transition: {:?} -> {:?}",
            current, target
        )))
    }
}

fn next_states(state: BGState) -> Vec<BGState> {
    match state {
        BGState::Init => vec![BGState::Assigned, BGState::Deleting],
        BGState::Assigned => vec![BGState::Active, BGState::Degraded, BGState::Deleting],
        BGState::Active => vec![BGState::Degraded, BGState::Rebalancing, BGState::Deleting],
        BGState::Degraded => vec![BGState::Recovering, BGState::Deleting],
        BGState::Recovering => vec![BGState::Active, BGState::Deleting],
        BGState::Rebalancing => vec![BGState::Active, BGState::Deleting],
        BGState::Deleting => vec![],
    }
}

pub fn is_reachable(current: BGState, target: BGState) -> bool {
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
        for next in next_states(s) {
            if next == target {
                return true;
            }
            stack.push(next);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_transitions() {
        assert!(validate_transition(BGState::Init, BGState::Assigned).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Active).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Degraded).is_ok());
        assert!(validate_transition(BGState::Active, BGState::Degraded).is_ok());
        assert!(validate_transition(BGState::Active, BGState::Rebalancing).is_ok());
        assert!(validate_transition(BGState::Degraded, BGState::Recovering).is_ok());
        assert!(validate_transition(BGState::Recovering, BGState::Active).is_ok());
        assert!(validate_transition(BGState::Rebalancing, BGState::Active).is_ok());
    }

    #[test]
    fn any_to_deleting() {
        assert!(validate_transition(BGState::Init, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Active, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Degraded, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Recovering, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Rebalancing, BGState::Deleting).is_ok());
    }

    #[test]
    fn invalid_transitions() {
        assert!(validate_transition(BGState::Init, BGState::Degraded).is_err());
        assert!(validate_transition(BGState::Init, BGState::Rebalancing).is_err());
        assert!(validate_transition(BGState::Assigned, BGState::Init).is_err());
        assert!(validate_transition(BGState::Degraded, BGState::Assigned).is_err());
        assert!(validate_transition(BGState::Rebalancing, BGState::Degraded).is_err());
    }

    #[test]
    fn reachability() {
        assert!(is_reachable(BGState::Init, BGState::Assigned));
        assert!(is_reachable(BGState::Assigned, BGState::Recovering));
        assert!(is_reachable(BGState::Init, BGState::Deleting));
        assert!(!is_reachable(BGState::Deleting, BGState::Init));
        // Degraded can loop (Degraded <-> Recovering <-> Active) but never reaches Assigned.
        assert!(!is_reachable(BGState::Degraded, BGState::Assigned));
    }
}
