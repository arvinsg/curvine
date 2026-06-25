use super::keys::{PD_BG_MIN_ISOLATION_LEVEL, PD_NODE_HEARTBEAT_TIMEOUT_MS};
use super::manager::ConfigManager;
use super::store::ConfigStore;
use crate::pd::journal::entry::ConfigEntry;
use crate::pd::journal::{self, ApplyOutcome};
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::proto::{GetConfigRequest, ListConfigRequest, SetConfigRequest};
use curvine_common::raft::RaftClient;
use curvine_common::state::ConfigInfo;
use orpc::common::LocalTime;
use std::collections::HashMap;
use std::sync::Arc;

fn make_journal_client() -> Arc<journal::Client> {
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    Arc::new(journal::Client::new(raft))
}

fn make_store() -> Arc<dyn KvStore> {
    Arc::new(MemoryKvEngine::new())
}

fn test_manager(dynamic: HashMap<String, String>) -> ConfigManager {
    test_manager_with_store(make_store(), dynamic)
}

fn test_manager_with_store(
    store: Arc<dyn KvStore>,
    dynamic: HashMap<String, String>,
) -> ConfigManager {
    ConfigManager::new(store, make_journal_client(), dynamic)
}

fn dynamic(entries: &[(&str, &str)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn config_info(key: &str, value: &[u8], version: u64) -> ConfigInfo {
    ConfigInfo {
        key: key.to_string(),
        value: value.to_vec(),
        version,
        mtime: LocalTime::mills(),
    }
}

fn config_entry(key: &str, value: &[u8], expected_version: u64) -> ConfigEntry {
    ConfigEntry {
        op_ms: LocalTime::mills(),
        expected_version,
        info: config_info(key, value, expected_version + 1),
    }
}

#[derive(Debug, Clone, Copy)]
enum ExpectedOutcome {
    Applied,
    Noop,
    Stale,
    NotFound,
}

fn assert_outcome(actual: ApplyOutcome, expected: ExpectedOutcome) {
    match (actual, expected) {
        (ApplyOutcome::Applied, ExpectedOutcome::Applied) => {}
        (ApplyOutcome::SkippedNoop, ExpectedOutcome::Noop) => {}
        (ApplyOutcome::SkippedStale { .. }, ExpectedOutcome::Stale) => {}
        (ApplyOutcome::NotFound { .. }, ExpectedOutcome::NotFound) => {}
        (actual, expected) => {
            panic!("unexpected outcome: actual={actual:?}, expected={expected:?}")
        }
    }
}

#[test]
fn get_config_cases() {
    struct Case {
        name: &'static str,
        dynamic: HashMap<String, String>,
        seed: Vec<ConfigEntry>,
        key: &'static str,
        expected_value: Option<&'static [u8]>,
        expected_version: Option<u64>,
    }

    let cases = vec![
        Case {
            name: "registered key returns override default",
            dynamic: dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "10")]),
            seed: vec![],
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS,
            expected_value: Some(b"10"),
            expected_version: Some(0),
        },
        Case {
            name: "unknown key returns none",
            dynamic: HashMap::new(),
            seed: vec![],
            key: "unknown.key",
            expected_value: None,
            expected_version: None,
        },
        Case {
            name: "persisted value overrides default",
            dynamic: dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "10")]),
            seed: vec![config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"42", 0)],
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS,
            expected_value: Some(b"42"),
            expected_version: Some(1),
        },
    ];

    for case in cases {
        let mgr = test_manager(case.dynamic);
        for entry in &case.seed {
            assert_eq!(mgr.apply_set_config(entry).unwrap(), ApplyOutcome::Applied);
        }

        let resp = mgr
            .get_config(GetConfigRequest {
                key: case.key.to_string(),
            })
            .unwrap();
        match (resp.item, case.expected_value, case.expected_version) {
            (Some(item), Some(value), Some(version)) => {
                assert_eq!(item.value, value, "{}", case.name);
                assert_eq!(item.version, version, "{}", case.name);
            }
            (None, None, None) => {}
            (actual, expected_value, expected_version) => panic!(
                "unexpected get result for {}: actual={actual:?}, expected_value={expected_value:?}, expected_version={expected_version:?}",
                case.name
            ),
        }
    }
}

#[test]
fn apply_set_config_cases() {
    struct Case {
        name: &'static str,
        seed: Vec<ConfigEntry>,
        entry: ConfigEntry,
        expected_outcome: ExpectedOutcome,
        expected_value: &'static [u8],
        expected_version: u64,
    }

    let cases = vec![
        Case {
            name: "apply first update",
            seed: vec![],
            entry: config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"100", 0),
            expected_outcome: ExpectedOutcome::Applied,
            expected_value: b"100",
            expected_version: 1,
        },
        Case {
            name: "apply next update with fresh expected version",
            seed: vec![config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"100", 0)],
            entry: config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"200", 1),
            expected_outcome: ExpectedOutcome::Applied,
            expected_value: b"200",
            expected_version: 2,
        },
        Case {
            name: "reject stale update",
            seed: vec![config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"100", 0)],
            entry: config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"stale", 0),
            expected_outcome: ExpectedOutcome::Stale,
            expected_value: b"100",
            expected_version: 1,
        },
        Case {
            name: "duplicate apply is noop",
            seed: vec![config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"100", 0)],
            entry: config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"100", 0),
            expected_outcome: ExpectedOutcome::Noop,
            expected_value: b"100",
            expected_version: 1,
        },
    ];

    for case in cases {
        let mgr = test_manager(HashMap::new());
        for entry in &case.seed {
            assert_eq!(mgr.apply_set_config(entry).unwrap(), ApplyOutcome::Applied);
        }

        assert_outcome(
            mgr.apply_set_config(&case.entry).unwrap(),
            case.expected_outcome,
        );
        let item = mgr
            .get_config(GetConfigRequest {
                key: PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
            })
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item.value, case.expected_value, "{}", case.name);
        assert_eq!(item.version, case.expected_version, "{}", case.name);
    }
}

#[test]
fn same_expected_version_allows_only_one_apply() {
    let mgr = test_manager(HashMap::new());
    let first = config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"first", 0);
    let second = config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"second", 0);

    assert_eq!(mgr.apply_set_config(&first).unwrap(), ApplyOutcome::Applied);
    assert_outcome(
        mgr.apply_set_config(&second).unwrap(),
        ExpectedOutcome::Stale,
    );

    let item = mgr
        .get_config(GetConfigRequest {
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
        })
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item.value, b"first");
    assert_eq!(item.version, 1);
}

#[test]
fn apply_rejects_invalid_entries() {
    struct Case {
        name: &'static str,
        entry: ConfigEntry,
        expected: ExpectedOutcome,
    }

    let cases = vec![
        Case {
            name: "unknown key",
            entry: config_entry("not.registered", b"v", 0),
            expected: ExpectedOutcome::NotFound,
        },
        Case {
            name: "entry version mismatch",
            entry: ConfigEntry {
                op_ms: LocalTime::mills(),
                expected_version: 0,
                info: config_info(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"bad", 3),
            },
            expected: ExpectedOutcome::Stale,
        },
    ];

    for case in cases {
        let mgr = test_manager(HashMap::new());
        assert_outcome(mgr.apply_set_config(&case.entry).unwrap(), case.expected);
        let item = mgr
            .get_config(GetConfigRequest {
                key: PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
            })
            .unwrap()
            .item
            .unwrap();
        assert_eq!(item.version, 0, "{}", case.name);
    }
}

#[test]
fn set_config_rejects_unknown_key_before_propose() {
    let mgr = test_manager(HashMap::new());

    let result = mgr.set_config(SetConfigRequest {
        key: "not.registered".into(),
        value: b"val".to_vec(),
    });

    assert!(result.is_err());
}

#[test]
fn list_config_cases() {
    let mgr = test_manager(dynamic(&[
        (PD_NODE_HEARTBEAT_TIMEOUT_MS, "123"),
        (PD_BG_MIN_ISOLATION_LEVEL, "rack"),
    ]));
    assert_eq!(
        mgr.apply_set_config(&config_entry(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"persisted", 0))
            .unwrap(),
        ApplyOutcome::Applied
    );

    struct Case {
        name: &'static str,
        prefix: &'static str,
        limit: Option<u32>,
        expected_non_empty: bool,
        expected_prefix: &'static str,
        expected_max_len: usize,
    }

    for case in [
        Case {
            name: "node prefix",
            prefix: "pd.node.",
            limit: None,
            expected_non_empty: true,
            expected_prefix: "pd.node.",
            expected_max_len: 10000,
        },
        Case {
            name: "bg prefix",
            prefix: "pd.bg.",
            limit: None,
            expected_non_empty: true,
            expected_prefix: "pd.bg.",
            expected_max_len: 10000,
        },
        Case {
            name: "limit one",
            prefix: "pd.",
            limit: Some(1),
            expected_non_empty: true,
            expected_prefix: "pd.",
            expected_max_len: 1,
        },
    ] {
        let resp = mgr
            .list_config(ListConfigRequest {
                prefix: case.prefix.into(),
                limit: case.limit,
            })
            .unwrap();
        assert_eq!(
            resp.items.is_empty(),
            !case.expected_non_empty,
            "{}",
            case.name
        );
        assert!(resp.items.len() <= case.expected_max_len, "{}", case.name);
        assert!(
            resp.items
                .iter()
                .all(|i| i.key.starts_with(case.expected_prefix)),
            "{}",
            case.name
        );
        assert!(
            resp.items.windows(2).all(|w| w[0].key <= w[1].key),
            "{} should be sorted by key",
            case.name
        );
    }

    let hb = mgr
        .list_config(ListConfigRequest {
            prefix: "pd.node.".into(),
            limit: None,
        })
        .unwrap()
        .items
        .into_iter()
        .find(|i| i.key == PD_NODE_HEARTBEAT_TIMEOUT_MS)
        .unwrap();
    assert_eq!(hb.value, b"persisted");
    assert_eq!(hb.version, 1);
}

#[test]
fn restore_loads_all_persisted_configs_without_list_limit() {
    let store = make_store();
    let config_store = ConfigStore::new(store.clone());

    for i in 0..1100 {
        config_store
            .set(&config_info(&format!("aa.unknown.{i:04}"), b"ignored", 1))
            .unwrap();
    }
    config_store
        .set(&config_info(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"persisted", 3))
        .unwrap();

    let mgr = test_manager_with_store(store, dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "default")]));
    mgr.restore().unwrap();

    let item = mgr
        .get_config(GetConfigRequest {
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
        })
        .unwrap()
        .item
        .unwrap();
    assert_eq!(item.value, b"persisted");
    assert_eq!(item.version, 3);
}

#[test]
fn restore_rebuilds_cache_from_store_and_filters_unknown_keys() {
    let store = make_store();
    let config_store = ConfigStore::new(store.clone());
    config_store
        .set(&config_info(PD_NODE_HEARTBEAT_TIMEOUT_MS, b"persisted", 7))
        .unwrap();
    config_store
        .set(&config_info("unknown.persisted", b"ignored", 1))
        .unwrap();

    let mgr = test_manager_with_store(
        store,
        dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "override-before-restore")]),
    );
    mgr.restore().unwrap();

    let hb = mgr
        .get_config(GetConfigRequest {
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
        })
        .unwrap()
        .item
        .unwrap();
    assert_eq!(hb.value, b"persisted");
    assert_eq!(hb.version, 7);

    let unknown = mgr
        .get_config(GetConfigRequest {
            key: "unknown.persisted".to_string(),
        })
        .unwrap();
    assert!(unknown.item.is_none());

    let listed = mgr
        .list_config(ListConfigRequest {
            prefix: "unknown".to_string(),
            limit: None,
        })
        .unwrap();
    assert!(listed.items.is_empty());
}
