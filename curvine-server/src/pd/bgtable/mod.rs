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

mod capacity;
mod control;
mod hash;
mod manager;
mod mutator;
pub mod placement;
mod store;
mod table;
mod table_registry;

pub use capacity::{CapacityBGTableControl, CapacityPlacement};
pub use control::BGTableControl;
pub use hash::{HashBGTableControl, HashPlacement};
pub use mutator::{BGMutator, PreparedTables, PrepareTablesResult};
pub use manager::BGTableManager;
pub use store::BGTableStore;
pub use table::{BGTable, BGTableBase, BGTableStats, CapacityBGTable, HashBGTable};
