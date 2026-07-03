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

use super::BGTable;
use crate::pd::store::{self, KvStore, KvWrite};
use curvine_common::state::TableId;
use curvine_common::utils::SerdeUtils as Serde;
use orpc::CommonResult;
use std::sync::Arc;

const NS: &str = store::CF_DATA;
const BG_TABLE_PREFIX: u8 = store::PREFIX_BG_TABLE;

pub struct BGTableStore {
    store: Arc<dyn KvStore>,
}

impl BGTableStore {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        Self { store }
    }

    fn table_key(&self, table_id: TableId) -> [u8; 3] {
        let mut key = [0u8; 3];
        key[0] = BG_TABLE_PREFIX;
        key[1..3].copy_from_slice(&table_id.to_be_bytes());
        key
    }

    fn put_op(&self, key: Vec<u8>, value: Vec<u8>) -> KvWrite {
        KvWrite::Put {
            ns: NS.to_string(),
            key,
            value,
        }
    }

    pub fn table_put_op(&self, table: &BGTable) -> CommonResult<KvWrite> {
        Ok(self.put_op(
            self.table_key(table.table_id()).to_vec(),
            Serde::serialize(table)?,
        ))
    }

    pub fn table_delete_op(&self, table_id: TableId) -> KvWrite {
        KvWrite::Delete {
            ns: NS.to_string(),
            key: self.table_key(table_id).to_vec(),
        }
    }

    pub fn write_batch(&self, ops: Vec<KvWrite>) -> CommonResult<()> {
        self.store.write_batch(ops)
    }

    pub fn put_table(&self, table: &BGTable) -> CommonResult<()> {
        let key = self.table_key(table.table_id());
        let value = Serde::serialize(table)?;
        self.store.put(NS, &key, &value)?;
        Ok(())
    }

    pub fn get_table(&self, table_id: TableId) -> CommonResult<Option<BGTable>> {
        let key = self.table_key(table_id);
        match self.store.get(NS, &key)? {
            Some(data) => Ok(Some(Serde::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    pub fn list_tables(&self) -> CommonResult<Vec<BGTable>> {
        let pairs = self.store.scan_prefix(NS, &[BG_TABLE_PREFIX])?;
        let mut tables = Vec::with_capacity(pairs.len());
        for (_key, value) in pairs {
            tables.push(Serde::deserialize(&value)?);
        }
        Ok(tables)
    }
}
