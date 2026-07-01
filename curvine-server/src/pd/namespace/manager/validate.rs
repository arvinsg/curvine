use super::*;
use crate::pd::bgtable::BGTable;
use curvine_common::state::BGKind;

impl NamespaceManager {
    pub(super) fn validate_create_request(&self, request: &CreateNamespaceRequest) -> FsResult<()> {
        validate_namespace_name(&request.name)?;
        if request.block_size == 0 {
            return Err(FsError::common("namespace block_size must be > 0"));
        }
        if request.write_buffer_config.is_some() {
            return Err(FsError::common(
                "write_buffer_config / Capacity BGTable is not supported in phase 1",
            ));
        }
        validate_cache_tier_config(&request.cache_tier_config)?;
        validate_cache_replica_policy(
            &request.cache_replica_policy,
            request.cache_tier_config.replica_count,
        )?;
        Ok(())
    }

    pub(super) fn validate_create_entry(&self, entry: &NamespaceCreateEntry) -> FsResult<()> {
        validate_namespace_info(&entry.namespace)?;
        self.validate_namespace_id_cas(entry)?;
        self.validate_namespace_tables(entry)?;
        Ok(())
    }

    fn validate_namespace_id_cas(&self, entry: &NamespaceCreateEntry) -> FsResult<()> {
        if entry.expected_next_namespace_id != entry.namespace.id {
            return Err(FsError::common(format!(
                "expected_next_namespace_id {} does not match namespace id {}",
                entry.expected_next_namespace_id, entry.namespace.id
            )));
        }
        let expected_next = entry.expected_next_namespace_id.saturating_add(1);
        if entry.next_namespace_id != expected_next {
            return Err(FsError::common(format!(
                "next_namespace_id mismatch: expected={}, actual={}",
                expected_next, entry.next_namespace_id
            )));
        }
        Ok(())
    }

    fn validate_namespace_tables(&self, entry: &NamespaceCreateEntry) -> FsResult<()> {
        let ns = &entry.namespace;
        let tables: Vec<&BGTable> = entry.tables.iter().collect();
        if tables.len() != ns.cache_tier_tables.len() {
            return Err(FsError::common(format!(
                "namespace table count mismatch: namespace={}, batch={}",
                ns.cache_tier_tables.len(),
                tables.len()
            )));
        }

        let mut table_ids = HashSet::with_capacity(tables.len());
        for (table_index, &table_id) in ns.cache_tier_tables.iter().enumerate() {
            let expected = make_table_id(ns.id, table_index as u8)?;
            if table_id != expected {
                return Err(FsError::common(format!(
                    "cache tier table at index {} mismatch: expected={}, actual={}",
                    table_index, expected, table_id
                )));
            }
            if !table_ids.insert(table_id) {
                return Err(FsError::common(format!(
                    "duplicate namespace table id {}",
                    table_id
                )));
            }
        }

        for (table_index, &table_id) in ns.cache_tier_tables.iter().enumerate() {
            let table = tables
                .iter()
                .find(|table| table.table_id() == table_id)
                .ok_or_else(|| {
                    FsError::common(format!(
                        "table {} missing in namespace create entry",
                        table_id
                    ))
                })?;
            self.validate_cache_tier_table(ns, table, table_index)?;
            validate_table_buckets_match_creates(table, &entry.bgs)?;
        }
        Ok(())
    }

    fn validate_cache_tier_table(
        &self,
        ns: &NamespaceInfo,
        table: &BGTable,
        table_index: usize,
    ) -> FsResult<()> {
        if table.kind() != BGKind::Hash {
            return Err(FsError::common(format!(
                "cache tier table {} must be Hash, actual={:?}",
                table.table_id(),
                table.kind()
            )));
        }
        if table.namespace_id() != ns.id {
            return Err(FsError::common(format!(
                "table {} namespace mismatch: expected={}, actual={}",
                table.table_id(),
                ns.id,
                table.namespace_id()
            )));
        }
        let expected_pool = ns.cache_tier_config.pools[table_index];
        if table.storage_type() != expected_pool {
            return Err(FsError::common(format!(
                "table {} pool mismatch: expected={:?}, actual={:?}",
                table.table_id(),
                expected_pool,
                table.storage_type()
            )));
        }
        if table.replica_count() != ns.cache_tier_config.replica_count {
            return Err(FsError::common(format!(
                "table {} replica_count mismatch: expected={}, actual={}",
                table.table_id(),
                ns.cache_tier_config.replica_count,
                table.replica_count()
            )));
        }
        let hash_table = table.hash_table().expect("hash table");
        let bucket_count = hash_table.bucket_count();
        if bucket_count != ns.cache_tier_config.bucket_count {
            return Err(FsError::common(format!(
                "table {} bucket_count mismatch: expected={}, actual={}",
                table.table_id(),
                ns.cache_tier_config.bucket_count,
                bucket_count
            )));
        }
        let policy = hash_table.cache_replica_policy();
        if policy != &ns.cache_replica_policy {
            return Err(FsError::common(format!(
                "table {} cache replica policy mismatch",
                table.table_id()
            )));
        }
        Ok(())
    }
}

fn validate_table_buckets_match_creates(
    table: &BGTable,
    creates: &[curvine_common::state::BlockGroupInfo],
) -> FsResult<()> {
    let created_bg_ids: Vec<BgId> = creates
        .iter()
        .filter(|bg| bg.table_id == table.table_id())
        .map(|bg| bg.bg_id)
        .collect();
    if table.hash_table().expect("hash table").buckets() != created_bg_ids.as_slice() {
        return Err(FsError::common(format!(
            "table {} buckets do not match create BG ids",
            table.table_id()
        )));
    }
    Ok(())
}

fn validate_namespace_info(info: &NamespaceInfo) -> FsResult<()> {
    if info.id == INVALID_NAMESPACE_ID || info.id > MAX_NAMESPACE_ID {
        return Err(FsError::common(format!(
            "namespace_id out of range: {}",
            info.id
        )));
    }
    validate_namespace_name(&info.name)?;
    if info.block_size == 0 {
        return Err(FsError::common("namespace block_size must be > 0"));
    }
    if info.write_buffer_config.is_some() || info.write_buffer_table.is_some() {
        return Err(FsError::common(
            "write_buffer_config / Capacity BGTable is not supported in phase 1",
        ));
    }
    validate_cache_tier_config(&info.cache_tier_config)?;
    validate_cache_replica_policy(
        &info.cache_replica_policy,
        info.cache_tier_config.replica_count,
    )?;
    Ok(())
}

fn validate_namespace_name(name: &str) -> FsResult<()> {
    if name.trim().is_empty() {
        return Err(FsError::common("namespace name must not be empty"));
    }
    if name.trim() != name {
        return Err(FsError::common(
            "namespace name must not contain leading or trailing whitespace",
        ));
    }
    Ok(())
}

fn validate_cache_tier_config(config: &CacheTierConfig) -> FsResult<()> {
    if config.pools.is_empty() {
        return Err(FsError::common("cache_tier_config.pools must not be empty"));
    }
    if config.pools.len() > MAX_TABLES_PER_NAMESPACE {
        return Err(FsError::common(format!(
            "too many cache tiers: {}, max={}",
            config.pools.len(),
            MAX_TABLES_PER_NAMESPACE
        )));
    }
    if config.pools.iter().collect::<HashSet<_>>().len() != config.pools.len() {
        return Err(FsError::common(
            "cache_tier_config.pools must not contain duplicate pool types",
        ));
    }
    if config.replica_count == 0 {
        return Err(FsError::common(
            "cache_tier_config.replica_count must be > 0",
        ));
    }
    if config.bucket_count == 0 {
        return Err(FsError::common(
            "cache_tier_config.bucket_count must be > 0",
        ));
    }
    Ok(())
}

fn validate_cache_replica_policy(
    policy: &curvine_common::state::CacheReplicaPolicy,
    replica_count: u16,
) -> FsResult<()> {
    if policy.min_isr == 0 {
        return Err(FsError::common("cache_replica_policy.min_isr must be > 0"));
    }
    if policy.min_isr > replica_count {
        return Err(FsError::common(format!(
            "cache_replica_policy.min_isr {} must be <= replica_count {}",
            policy.min_isr, replica_count
        )));
    }
    if let CacheAckPolicy::AtLeast(n) = policy.ack_policy {
        if n == 0 || n > replica_count {
            return Err(FsError::common(format!(
                "cache_replica_policy.ack_policy AtLeast({}) must be in 1..=replica_count {}",
                n, replica_count
            )));
        }
    }
    Ok(())
}
