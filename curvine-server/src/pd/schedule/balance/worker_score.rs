use crate::pd::bg::BGManager;
use curvine_common::state::PoolInfo;

/// Per-worker score for a specific pool.
#[derive(Debug, Clone)]
pub struct WorkerScore {
    pub worker_id: u32,
    pub bg_count: u32,
    pub leader_count: u32,
    pub weight: f64,
    pub bg_score: f64,
    pub leader_score: f64,
}

/// Compute per-worker scores for all allocatable workers in a pool.
pub fn compute_pool_scores(pool: &PoolInfo, bg_manager: &BGManager) -> Vec<WorkerScore> {
    let mut scores: Vec<WorkerScore> = pool
        .allocatable_workers
        .iter()
        .map(|&worker_id| WorkerScore {
            worker_id,
            bg_count: 0,
            leader_count: 0,
            weight: 1.0,
            bg_score: 0.0,
            leader_score: 0.0,
        })
        .collect();

    // Count BGs and leaders per worker
    for table in bg_manager.list_tables() {
        if table.pool_id() != pool.pool_id {
            continue;
        }
        for bg in bg_manager.list_bgs() {
            if bg.table_id != table.table_id {
                continue;
            }
            for score in &mut scores {
                if bg.replica_set.contains(&score.worker_id) {
                    score.bg_count += 1;
                }
                if bg.lease_owner.as_ref().map(|l| l.node_id) == Some(score.worker_id) {
                    score.leader_count += 1;
                }
            }
        }
    }

    // Compute scores
    for score in &mut scores {
        score.bg_score = score.bg_count as f64 / score.weight;
        score.leader_score = score.leader_count as f64 / score.weight;
    }

    scores
}

/// Check whether a balance operation should be performed (with hysteresis).
/// tolerant_ratio_bps is in basis points (e.g. 500 = 5%).
pub fn should_balance(source_score: f64, target_score: f64, mean_score: f64, tolerant_ratio_bps: u32) -> bool {
    let ratio = tolerant_ratio_bps as f64 / 10_000.0;
    let tolerant = (mean_score * ratio).max(1.0);
    source_score - target_score > tolerant
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_balance_above_threshold() {
        // mean=10, ratio=5%, tolerant=max(0.5, 1.0)=1.0
        assert!(should_balance(6.0, 4.0, 10.0, 500)); // diff=2 > 1.0
    }

    #[test]
    fn should_balance_within_threshold() {
        // mean=10, tolerant=1.0, diff=0.5
        assert!(!should_balance(5.5, 5.0, 10.0, 500));
    }

    #[test]
    fn should_balance_single_worker_edge_case() {
        // Only one worker: source == target, diff = 0
        assert!(!should_balance(10.0, 10.0, 10.0, 500));
    }

    #[test]
    fn should_balance_large_cluster() {
        // mean=100, ratio=5%, tolerant=5.0
        assert!(should_balance(60.0, 40.0, 100.0, 500)); // diff=20 > 5
        assert!(!should_balance(52.0, 48.0, 100.0, 500)); // diff=4 < 5
    }
}
