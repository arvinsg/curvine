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

use crate::conf::JournalConf;
use crate::rocksdb::DBConf;
use orpc::io::net::InetAddr;
use orpc::server::ServerConf;
use log::info;
use orpc::{err_box, try_err, CommonResult};
use serde::{Deserialize, Serialize};
use std::fs::read_to_string;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PdConf {
    #[serde(default = "default_cluster_id")]
    pub cluster_id: String,

    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,

    #[serde(default = "default_web_port")]
    pub web_port: u16,

    #[serde(default = "default_data_dir")]
    pub data_dir: String,

    #[serde(default)]
    pub journal: JournalConf,
}

impl Default for PdConf {
    fn default() -> Self {
        Self {
            cluster_id: default_cluster_id(),
            rpc_port: default_rpc_port(),
            web_port: default_web_port(),
            data_dir: default_data_dir(),
            journal: JournalConf::default(),
        }
    }
}

impl PdConf {
    /// Load PD config from a file path (same pattern as `ClusterConf::from`).
    pub fn from<T: AsRef<str>>(path: T) -> CommonResult<Self> {
        let path = path.as_ref();
        let s = try_err!(read_to_string(path));
        let conf = try_err!(toml::from_str::<Self>(&s));
        Ok(conf)
    }

    /// Validate required fields so startup fails fast with a clear error.
    pub fn check(&self) -> CommonResult<()> {
        if self.cluster_id.is_empty() {
            return err_box!("PD config: cluster_id must not be empty");
        }
        if self.data_dir.is_empty() {
            return err_box!("PD config: data_dir must not be empty");
        }
        if self.journal.journal_dir.is_empty() {
            return err_box!("PD config: journal.journal_dir must not be empty");
        }
        if self.journal.journal_addrs.is_empty() {
            return err_box!(
                "PD config: journal.journal_addrs must not be empty (at least one Raft peer)"
            );
        }
        Ok(())
    }

    pub fn pd_server_conf(&self) -> ServerConf {
        let mut conf = ServerConf::default();
        conf.port = self.rpc_port;
        conf
    }

    pub fn pd_rocks_conf(&self) -> DBConf {
        DBConf::new(&self.data_dir)
    }

    pub fn local_addr(&self) -> InetAddr {
        self.journal.local_addr()
    }

    pub fn print(&self) {
        info!("PD Configuration:");
        info!("  cluster_id: {}", self.cluster_id);
        info!("  rpc_port: {}", self.rpc_port);
        info!("  web_port: {}", self.web_port);
        info!("  data_dir: {}", self.data_dir);
    }
}

fn default_cluster_id() -> String {
    "curvine".to_string()
}

fn default_rpc_port() -> u16 {
    2379
}

fn default_web_port() -> u16 {
    2380
}

fn default_data_dir() -> String {
    "/data/pd/data".to_string()
}
