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
/// Init ──assign──> Assigned
///                      │
///                      ├──worker lost──> Degraded ──recover──> Recovering ──done──> Assigned
///                      │
///                      ├──data migration──> Moving ──done──> Assigned
///                      │
///                      └──delete cmd──> Deleting
/// ```
pub fn validate_transition(current: BGState, target: BGState) -> FsResult<()> {
    let valid = match (current, target) {
        (BGState::Init, BGState::Assigned) => true,
        (BGState::Assigned, BGState::Degraded) => true,
        (BGState::Assigned, BGState::Moving) => true,
        (BGState::Degraded, BGState::Recovering) => true,
        (BGState::Recovering, BGState::Assigned) => true,
        (BGState::Moving, BGState::Assigned) => true,
        // Any state can transition to Deleting
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

/// Check whether the target state is reachable from the current state (transitive).
pub fn is_reachable(current: BGState, target: BGState) -> bool {
    if current == target {
        return true;
    }
    for next in next_states(current) {
        if next == target || is_reachable(next, target) {
            return true;
        }
    }
    false
}

fn next_states(state: BGState) -> Vec<BGState> {
    match state {
        BGState::Init => vec![BGState::Assigned, BGState::Deleting],
        BGState::Assigned => vec![BGState::Degraded, BGState::Moving, BGState::Deleting],
        BGState::Degraded => vec![BGState::Recovering, BGState::Deleting],
        BGState::Recovering => vec![BGState::Assigned, BGState::Deleting],
        BGState::Moving => vec![BGState::Assigned, BGState::Deleting],
        BGState::Deleting => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_transitions() {
        assert!(validate_transition(BGState::Init, BGState::Assigned).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Degraded).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Moving).is_ok());
        assert!(validate_transition(BGState::Degraded, BGState::Recovering).is_ok());
        assert!(validate_transition(BGState::Recovering, BGState::Assigned).is_ok());
        assert!(validate_transition(BGState::Moving, BGState::Assigned).is_ok());
    }

    #[test]
    fn any_to_deleting() {
        assert!(validate_transition(BGState::Init, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Assigned, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Degraded, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Recovering, BGState::Deleting).is_ok());
        assert!(validate_transition(BGState::Moving, BGState::Deleting).is_ok());
    }

    #[test]
    fn invalid_transitions() {
        assert!(validate_transition(BGState::Init, BGState::Degraded).is_err());
        assert!(validate_transition(BGState::Init, BGState::Moving).is_err());
        assert!(validate_transition(BGState::Assigned, BGState::Init).is_err());
        assert!(validate_transition(BGState::Degraded, BGState::Assigned).is_err());
        assert!(validate_transition(BGState::Moving, BGState::Degraded).is_err());
    }

    #[test]
    fn reachability() {
        assert!(is_reachable(BGState::Init, BGState::Assigned));
        assert!(is_reachable(BGState::Assigned, BGState::Recovering));
        assert!(is_reachable(BGState::Init, BGState::Deleting));
        assert!(!is_reachable(BGState::Deleting, BGState::Init));
        assert!(is_reachable(BGState::Degraded, BGState::Assigned));
    }
}
