use super::manager::ConfigManager;
use crate::pd::journal;
use crate::pd::store::memory_kv_engine::MemoryKvEngine;
use crate::pd::store::KvStore;
use curvine_common::conf::JournalConf;
use curvine_common::proto::{GetConfigRequest, ListConfigRequest, SetConfigRequest};
use curvine_common::raft::RaftClient;
use curvine_common::state::ConfigInfo;
use std::collections::HashMap;
use std::sync::Arc;

fn test_manager(dynamic: HashMap<String, String>) -> ConfigManager {
    let engine: Arc<dyn KvStore> = Arc::new(MemoryKvEngine::new());
    let journal_conf = JournalConf::default();
    let rt = journal_conf.create_runtime();
    let raft = RaftClient::from_conf(rt, &journal_conf);
    let jc = Arc::new(journal::Client::new(raft));
    ConfigManager::new(engine, jc, dynamic)
}

fn dynamic(entries: &[(&str, &str)]) -> HashMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn get_returns_default_when_not_persisted() {
    let mgr = test_manager(dynamic(&[("pd.max_moves", "10")]));

    let resp = mgr
        .get_config(GetConfigRequest {
            key: "pd.max_moves".into(),
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item.key, "pd.max_moves");
    assert_eq!(item.value, b"10");
    assert_eq!(item.version, 0);
}

#[test]
fn get_returns_none_for_unknown_key() {
    let mgr = test_manager(dynamic(&[("pd.max_moves", "10")]));

    let resp = mgr
        .get_config(GetConfigRequest {
            key: "unknown.key".into(),
        })
        .unwrap();

    assert!(resp.item.is_none());
}

#[test]
fn get_returns_persisted_over_default() {
    let mgr = test_manager(dynamic(&[("pd.max_moves", "10")]));

    let persisted = ConfigInfo::new("pd.max_moves".to_string(), b"42".to_vec());
    mgr.apply_set_config(&persisted).unwrap();

    let resp = mgr
        .get_config(GetConfigRequest {
            key: "pd.max_moves".into(),
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item.value, b"42");
    assert_eq!(item.version, 1);
}

#[test]
fn set_rejects_unknown_key() {
    let mgr = test_manager(dynamic(&[("pd.max_moves", "10")]));

    let result = mgr.set_config(SetConfigRequest {
        key: "not.registered".into(),
        value: b"val".to_vec(),
    });

    assert!(result.is_err());
}

#[test]
fn list_merges_persisted_and_defaults() {
    let mgr = test_manager(dynamic(&[
        ("pd.a", "default_a"),
        ("pd.b", "default_b"),
        ("other.c", "default_c"),
    ]));

    let persisted = ConfigInfo::new("pd.a".to_string(), b"persisted_a".to_vec());
    mgr.apply_set_config(&persisted).unwrap();

    let resp = mgr
        .list_config(ListConfigRequest {
            prefix: "pd.".into(),
            limit: None,
        })
        .unwrap();

    assert_eq!(resp.items.len(), 2);

    let item_a = resp.items.iter().find(|i| i.key == "pd.a").unwrap();
    assert_eq!(item_a.value, b"persisted_a");
    assert!(item_a.version >= 1);

    let item_b = resp.items.iter().find(|i| i.key == "pd.b").unwrap();
    assert_eq!(item_b.value, b"default_b");
    assert_eq!(item_b.version, 0);
}

#[test]
fn list_respects_prefix_filter() {
    let mgr = test_manager(dynamic(&[("pd.a", "1"), ("pd.b", "2"), ("other.c", "3")]));

    let resp = mgr
        .list_config(ListConfigRequest {
            prefix: "other.".into(),
            limit: None,
        })
        .unwrap();

    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].key, "other.c");
}
