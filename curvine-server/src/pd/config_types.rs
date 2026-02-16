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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ConfigItem {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub scope: Option<ConfigScope>,
    pub mtime: u64,
}

impl ConfigItem {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            version: 1,
            scope: None,
            mtime: orpc::common::LocalTime::mills(),
        }
    }

    pub fn with_scope(mut self, scope: ConfigScope) -> Self {
        self.scope = Some(scope);
        self
    }

    pub fn value_as_string(&self) -> Result<String, std::string::FromUtf8Error> {
        String::from_utf8(self.value.clone())
    }

    pub fn update_value(&mut self, value: Vec<u8>) {
        self.value = value;
        self.version += 1;
        self.mtime = orpc::common::LocalTime::mills();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConfigScope {
    Cluster,
    Node(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListConfigRequest {
    pub prefix: String,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ListConfigResponse {
    pub items: Vec<ConfigItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigRequest {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetConfigResponse {
    pub item: Option<ConfigItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetConfigRequest {
    pub key: String,
    pub value: Vec<u8>,
    pub scope: Option<ConfigScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SetConfigResponse {
    pub success: bool,
    pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteConfigRequest {
    pub key: String,
    pub prev_version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeleteConfigResponse {
    pub success: bool,
}
