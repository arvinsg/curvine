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

// Conversion between protobuf and internal config types (ConfigItem/ConfigScope in store).

use super::store::{ConfigItem, ConfigScope};
use curvine_common::proto::{
    ConfigItemProto, ConfigScopeProto, SetConfigRequest as PbSetConfigRequest,
};

pub fn config_item_to_pb(item: &ConfigItem) -> ConfigItemProto {
    let scope = match &item.scope {
        Some(ConfigScope::Cluster) => Some(ConfigScopeProto::Cluster),
        Some(ConfigScope::Node(_)) => Some(ConfigScopeProto::Node),
        None => None,
    };
    ConfigItemProto {
        key: item.key.clone(),
        value: item.value.clone(),
        version: item.version,
        scope,
        mtime: item.mtime,
    }
}

fn scope_from_pb(scope: Option<i32>) -> Option<ConfigScope> {
    match scope {
        Some(v) if v == ConfigScopeProto::Cluster as i32 => Some(ConfigScope::Cluster),
        Some(v) if v == ConfigScopeProto::Node as i32 => Some(ConfigScope::Node(String::new())),
        _ => None,
    }
}

pub fn set_config_pb_to_config_item(pb: PbSetConfigRequest) -> ConfigItem {
    let scope = scope_from_pb(pb.scope.map(|e| e as i32));
    let mut item = ConfigItem::new(pb.key, pb.value);
    if let Some(s) = scope {
        item = item.with_scope(s);
    }
    item
}

pub fn set_config_request_from_http(
    key: String,
    value: Vec<u8>,
    scope: Option<ConfigScope>,
) -> PbSetConfigRequest {
    let scope = match scope {
        Some(ConfigScope::Cluster) => Some(ConfigScopeProto::Cluster),
        Some(ConfigScope::Node(_)) => Some(ConfigScopeProto::Node),
        None => None,
    };
    PbSetConfigRequest { key, value, scope }
}
