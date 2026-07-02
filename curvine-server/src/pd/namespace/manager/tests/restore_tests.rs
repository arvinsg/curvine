use super::fixtures::{request, test_managers};
use curvine_common::state::make_table_id;

/// After an atomic namespace create, a restore reloads the namespace, its
/// BGTable and BGs from the KV store and heals next_bg_id to at least
/// max(bg_id)+1. There are no orphans to prune because create commits
/// everything in one batch.
#[test]
fn restore_reloads_committed_namespace_state() {
    let (ns_manager, bg_manager, bgtable_manager) = test_managers();
    let entry = ns_manager
        .test_build_create_entry(request("ns1"), 1, 1)
        .unwrap();
    ns_manager.apply_create_namespace(&entry, true).unwrap();

    let table_id = make_table_id(1, 0).unwrap();
    assert!(bgtable_manager.get_table(table_id).is_some());
    assert_eq!(bg_manager.list_all_bgs().len(), 4);
    let max_bg_id = bg_manager
        .list_all_bgs()
        .iter()
        .map(|bg| bg.bg_id)
        .max()
        .unwrap();

    // Restore reloads from the KV store: committed state is unchanged and the
    // BG id floor is healed to max(bg_id)+1.
    bgtable_manager.restore().unwrap();
    ns_manager.restore().unwrap();

    assert!(ns_manager.get_namespace_by_name("ns1").is_some());
    assert!(bgtable_manager.get_table(table_id).is_some());
    assert_eq!(bg_manager.list_all_bgs().len(), 4);
    assert!(bg_manager.get_next_bg_id().unwrap() >= max_bg_id + 1);
}
