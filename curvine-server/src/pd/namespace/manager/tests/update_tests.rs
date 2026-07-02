use super::super::*;
use super::fixtures::{request, test_managers};
use curvine_common::state::{
    make_table_id, CacheAckPolicy, CacheReplicaPolicy, TtlAction, UpdateNamespaceRequest,
};

/// Create ns "ns1" (id 1) and return the managers plus its initial version.
fn seed_ns1() -> (
    Arc<NamespaceManager>,
    Arc<crate::pd::bg::BGManager>,
    Arc<crate::pd::bgtable::BGTableManager>,
) {
    let (ns_manager, bg_manager, bgtable_manager) = test_managers();
    let entry = ns_manager
        .test_build_create_entry(request("ns1"), 1, 1)
        .unwrap();
    ns_manager.apply_create_namespace(&entry, true).unwrap();
    (ns_manager, bg_manager, bgtable_manager)
}

fn patch(id: NamespaceId) -> UpdateNamespaceRequest {
    UpdateNamespaceRequest {
        id,
        ..Default::default()
    }
}

#[test]
fn update_patches_specified_fields() {
    let (ns_manager, _bg, _bgt) = seed_ns1();
    let before = ns_manager.get_namespace(1).unwrap();

    let req = UpdateNamespaceRequest {
        default_ttl_ms: Some(Some(60_000)),
        ttl_action: Some(TtlAction::Delete),
        properties: Some(std::collections::HashMap::from([(
            "team".to_string(),
            "storage".to_string(),
        )])),
        ..patch(1)
    };
    let entry = ns_manager.test_build_update_entry(req).unwrap();
    let outcome = ns_manager.apply_update_namespace(&entry).unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied);

    let after = ns_manager.get_namespace(1).unwrap();
    assert_eq!(after.default_ttl_ms, Some(60_000));
    assert_eq!(after.ttl_action, TtlAction::Delete);
    assert_eq!(after.properties.get("team").map(String::as_str), Some("storage"));
    assert_eq!(after.version, before.version + 1);
    // Untouched fields are preserved.
    assert_eq!(after.name, before.name);
    assert_eq!(after.block_size, before.block_size);
}

#[test]
fn update_clears_ttl() {
    let (ns_manager, _bg, _bgt) = seed_ns1();
    // First set a TTL, then clear it.
    let set = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            default_ttl_ms: Some(Some(1_000)),
            ..patch(1)
        })
        .unwrap();
    ns_manager.apply_update_namespace(&set).unwrap();
    assert_eq!(ns_manager.get_namespace(1).unwrap().default_ttl_ms, Some(1_000));

    let clear = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            default_ttl_ms: Some(None),
            ..patch(1)
        })
        .unwrap();
    ns_manager.apply_update_namespace(&clear).unwrap();
    assert_eq!(ns_manager.get_namespace(1).unwrap().default_ttl_ms, None);
}

#[test]
fn update_policy_propagates_to_tables() {
    let (ns_manager, _bg, bgtable_manager) = seed_ns1();
    let table_id = make_table_id(1, 0).unwrap();
    let epoch_before = bgtable_manager.get_table(table_id).unwrap().epoch();

    let new_policy = CacheReplicaPolicy {
        ack_policy: CacheAckPolicy::AtLeast(2),
        min_isr: 2,
        ..Default::default()
    };
    let entry = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            cache_replica_policy: Some(new_policy.clone()),
            ..patch(1)
        })
        .unwrap();
    let outcome = ns_manager.apply_update_namespace(&entry).unwrap();
    assert_eq!(outcome, ApplyOutcome::Applied);

    let table = bgtable_manager.get_table(table_id).unwrap();
    let hash = table.hash_table().unwrap();
    assert_eq!(hash.cache_replica_policy(), &new_policy);
    assert_eq!(table.epoch(), epoch_before + 1);
    // Namespace copy is consistent with the table copy.
    assert_eq!(ns_manager.get_namespace(1).unwrap().cache_replica_policy, new_policy);
}

#[test]
fn update_rejects_stale_version() {
    let (ns_manager, _bg, _bgt) = seed_ns1();
    // Build an entry, then advance the record so its expected_version goes stale.
    let stale = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            block_size: Some(64 << 20),
            ..patch(1)
        })
        .unwrap();
    let advance = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            block_size: Some(32 << 20),
            ..patch(1)
        })
        .unwrap();
    ns_manager.apply_update_namespace(&advance).unwrap();

    let outcome = ns_manager.apply_update_namespace(&stale).unwrap();
    assert!(matches!(outcome, ApplyOutcome::SkippedStale { .. }));
}

#[test]
fn update_reapply_is_stale() {
    let (ns_manager, _bg, _bgt) = seed_ns1();
    let entry = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            block_size: Some(64 << 20),
            ..patch(1)
        })
        .unwrap();
    // First apply advances the version; re-applying the same entry now fails
    // the version CAS (expected_version is stale) — Raft applies each committed
    // entry once, so this path only occurs on a buggy double-apply.
    assert_eq!(
        ns_manager.apply_update_namespace(&entry).unwrap(),
        ApplyOutcome::Applied
    );
    assert!(matches!(
        ns_manager.apply_update_namespace(&entry).unwrap(),
        ApplyOutcome::SkippedStale { .. }
    ));
}

#[test]
fn update_missing_is_not_found() {
    let (ns_manager, _bg, _bgt) = test_managers();
    let err = ns_manager
        .test_build_update_entry(patch(999))
        .expect_err("missing namespace should fail");
    assert!(matches!(err, FsError::NotFound(_)));
}

#[test]
fn update_rejects_invalid_patch() {
    let (ns_manager, _bg, _bgt) = seed_ns1();
    // block_size = 0 is invalid.
    let err = ns_manager
        .test_build_update_entry(UpdateNamespaceRequest {
            block_size: Some(0),
            ..patch(1)
        })
        .expect_err("zero block_size should fail");
    assert!(matches!(err, FsError::InvalidArgument(_)));
}
