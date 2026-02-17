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
pub struct ConfigInfo {
    pub key: String,
    pub value: Vec<u8>,
    pub version: u64,
    pub mtime: u64,
}

impl ConfigInfo {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            version: 1,
            mtime: orpc::common::LocalTime::mills(),
        }
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
