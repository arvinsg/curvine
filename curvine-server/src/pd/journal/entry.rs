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

use curvine_common::state::{ConfigInfo, MountInfo};
use serde::{Deserialize, Serialize};

// mount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MountEntry {
    pub(crate) op_ms: u64,
    pub(crate) info: MountInfo,
}

// umount
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UnMountEntry {
    pub(crate) op_ms: u64,
    pub(crate) id: u32,
}

// config
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ConfigEntry {
    pub(crate) op_ms: u64,
    pub(crate) info: ConfigInfo,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PdEntry {
    Noop,
    SetConfig(ConfigEntry),
    DeleteConfig(String),
    Mount(MountEntry),
    Unmount(u32),
}
