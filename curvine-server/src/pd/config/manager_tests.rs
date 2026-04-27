use super::manager::ConfigManager;
use crate::pd::config::keys::{PD_BG_MIN_ISOLATION_LEVEL, PD_NODE_HEARTBEAT_TIMEOUT_MS};
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
    let mgr = test_manager(dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "10")]));

    let resp = mgr
        .get_config(GetConfigRequest {
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS.into(),
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item.key, PD_NODE_HEARTBEAT_TIMEOUT_MS);
    assert_eq!(item.value, b"10");
    assert_eq!(item.version, 0);
}

#[test]
fn get_returns_none_for_unknown_key() {
    let mgr = test_manager(HashMap::new());

    let resp = mgr
        .get_config(GetConfigRequest {
            key: "unknown.key".into(),
        })
        .unwrap();

    assert!(resp.item.is_none());
}

#[test]
fn get_returns_persisted_over_default() {
    let mgr = test_manager(dynamic(&[(PD_NODE_HEARTBEAT_TIMEOUT_MS, "10")]));

    let persisted = ConfigInfo::new(PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(), b"42".to_vec());
    mgr.apply_set_config(&persisted).unwrap();

    let resp = mgr
        .get_config(GetConfigRequest {
            key: PD_NODE_HEARTBEAT_TIMEOUT_MS.into(),
        })
        .unwrap();

    let item = resp.item.unwrap();
    assert_eq!(item.value, b"42");
    assert_eq!(item.version, 1);
}

#[test]
fn set_rejects_unknown_key() {
    let mgr = test_manager(HashMap::new());

    let result = mgr.set_config(SetConfigRequest {
        key: "not.registered".into(),
        value: b"val".to_vec(),
    });

    assert!(result.is_err());
}

#[test]
fn list_merges_persisted_and_defaults() {
    let mgr = test_manager(dynamic(&[
        (PD_NODE_HEARTBEAT_TIMEOUT_MS, "123"),
        (PD_BG_MIN_ISOLATION_LEVEL, "rack"),
    ]));

    let persisted = ConfigInfo::new(
        PD_NODE_HEARTBEAT_TIMEOUT_MS.to_string(),
        b"persisted".to_vec(),
    );
    mgr.apply_set_config(&persisted).unwrap();

    let resp = mgr
        .list_config(ListConfigRequest {
            prefix: "pd.node.".into(),
            limit: None,
        })
        .unwrap();

    // pd.node.* includes heartbeat_timeout + lost_recovery_window + persist_interval.
    let hb = resp
        .items
        .iter()
        .find(|i| i.key == PD_NODE_HEARTBEAT_TIMEOUT_MS)
        .unwrap();
    assert_eq!(hb.value, b"persisted");
    assert!(hb.version >= 1);

    // Another pd.node.* key stays at its default (version 0).
    let default_item = resp
        .items
        .iter()
        .find(|i| i.key != PD_NODE_HEARTBEAT_TIMEOUT_MS)
        .unwrap();
    assert_eq!(default_item.version, 0);
}

#[test]
fn list_respects_prefix_filter() {
    let mgr = test_manager(HashMap::new());

    let resp_node = mgr
        .list_config(ListConfigRequest {
            prefix: "pd.node.".into(),
            limit: None,
        })
        .unwrap();
    let resp_bg = mgr
        .list_config(ListConfigRequest {
            prefix: "pd.bg.".into(),
            limit: None,
        })
        .unwrap();

    assert!(!resp_node.items.is_empty());
    assert!(resp_node.items.iter().all(|i| i.key.starts_with("pd.node.")));
    assert!(!resp_bg.items.is_empty());
    assert!(resp_bg.items.iter().all(|i| i.key.starts_with("pd.bg.")));
}
