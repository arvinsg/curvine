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

use crate::proto::*;
use crate::state::*;
use crate::{FsError, FsResult};
use orpc::{try_err, CommonResult};
use prost::bytes::BytesMut;
use prost::Message;
use std::fmt::Debug;

pub struct ProtoUtils;

impl ProtoUtils {
    pub fn storage_info_to_pb(item: StorageInfo) -> StorageInfoProto {
        StorageInfoProto {
            dir_id: item.dir_id,
            storage_id: item.storage_id,
            failed: false,
            capacity: item.capacity,
            fs_used: item.fs_used,
            non_fs_used: item.non_fs_used,
            available: item.available,
            reserved_bytes: item.reserved_bytes,
            storage_type: item.storage_type.into(),
            block_num: item.block_num,
            dir_path: item.dir_path,
        }
    }

    pub fn storage_info_from_pb(item: StorageInfoProto) -> StorageInfo {
        StorageInfo {
            dir_id: item.dir_id,
            storage_id: item.storage_id,
            failed: false,
            capacity: item.capacity,
            fs_used: item.fs_used,
            non_fs_used: item.non_fs_used,
            available: item.available,
            reserved_bytes: item.reserved_bytes,
            storage_type: StorageType::from(item.storage_type),
            block_num: item.block_num,
            dir_path: item.dir_path,
        }
    }

    pub fn worker_address_to_pb(addr: &WorkerAddress) -> WorkerAddressProto {
        WorkerAddressProto {
            worker_id: addr.worker_id.to_owned(),
            hostname: addr.hostname.to_owned(),
            ip_addr: addr.ip_addr.to_owned(),
            rpc_port: addr.rpc_port,
            web_port: addr.web_port,
        }
    }

    pub fn worker_address_from_pb(addr: &WorkerAddressProto) -> WorkerAddress {
        WorkerAddress {
            worker_id: addr.worker_id.to_owned(),
            hostname: addr.hostname.to_owned(),
            ip_addr: addr.ip_addr.to_owned(),
            rpc_port: addr.rpc_port,
            web_port: addr.web_port,
        }
    }

    pub fn storage_info_list_from_pb(list: Vec<StorageInfoProto>) -> Vec<StorageInfo> {
        let mut res = Vec::with_capacity(list.len());
        for item in list {
            let info = Self::storage_info_from_pb(item);
            res.push(info)
        }

        res
    }

    pub fn client_address_from_pb(addr: ClientAddressProto) -> ClientAddress {
        ClientAddress {
            client_name: addr.client_name,
            hostname: addr.hostname,
            ip_addr: addr.ip_addr,
            port: addr.port,
        }
    }

    pub fn client_address_to_pb(addr: ClientAddress) -> ClientAddressProto {
        ClientAddressProto {
            client_name: addr.client_name,
            hostname: addr.hostname,
            ip_addr: addr.ip_addr,
            port: addr.port,
        }
    }

    pub fn extend_block_from_pb(block: ExtendedBlockProto) -> ExtendedBlock {
        ExtendedBlock {
            id: block.id,
            len: block.block_size,
            storage_type: StorageType::from(block.storage_type),
            file_type: FileType::from(block.file_type),
            alloc_opts: block.alloc_opts.map(Self::file_alloc_opts_from_pb),
        }
    }

    pub fn extend_block_to_pb(block: ExtendedBlock) -> ExtendedBlockProto {
        ExtendedBlockProto {
            id: block.id,
            block_size: block.len,
            storage_type: block.storage_type.into(),
            file_type: block.file_type.into(),
            alloc_opts: block.alloc_opts.map(Self::file_alloc_opts_to_pb),
        }
    }

    pub fn block_location_from_pb(locations: BlockLocationProto) -> BlockLocation {
        BlockLocation {
            worker_id: locations.worker_id,
            storage_type: StorageType::from(locations.storage_type),
        }
    }

    pub fn block_location_to_pb(locations: BlockLocation) -> BlockLocationProto {
        BlockLocationProto {
            worker_id: locations.worker_id,
            storage_type: locations.storage_type.into(),
        }
    }

    pub fn commit_block_from_pb(block: CommitBlockProto) -> CommitBlock {
        let mut locations = vec![];
        for item in block.locations {
            locations.push(Self::block_location_from_pb(item))
        }

        CommitBlock {
            block_id: block.block_id,
            block_len: block.block_len,
            locations,
        }
    }

    pub fn commit_block_to_pb(block: CommitBlock) -> CommitBlockProto {
        let mut locations = vec![];
        for item in block.locations {
            locations.push(Self::block_location_to_pb(item))
        }

        CommitBlockProto {
            block_id: block.block_id,
            block_len: block.block_len,
            locations,
        }
    }

    pub fn located_block_to_pb(block: LocatedBlock) -> LocatedBlockProto {
        let b = Self::extend_block_to_pb(block.block);

        let locs: Vec<WorkerAddressProto> =
            block.locs.iter().map(Self::worker_address_to_pb).collect();

        LocatedBlockProto {
            block: b,
            offset: 0,
            locs,
        }
    }

    pub fn located_block_from_pb(block: LocatedBlockProto) -> LocatedBlock {
        let locs = block
            .locs
            .iter()
            .map(Self::worker_address_from_pb)
            .collect();

        LocatedBlock {
            block: Self::extend_block_from_pb(block.block),
            locs,
        }
    }

    pub fn encode<T: Message + Debug>(proto: T) -> CommonResult<BytesMut> {
        let mut bytes = BytesMut::with_capacity(proto.encoded_len());
        try_err!(proto.encode(&mut bytes));
        Ok(bytes)
    }

    pub fn storage_policy_to_pb(policy: StoragePolicy) -> StoragePolicyProto {
        StoragePolicyProto {
            storage_type: policy.storage_type.into(),
            ttl_ms: policy.ttl_ms,
            ttl_action: policy.ttl_action.into(),
            ufs_mtime: policy.ufs_mtime,
        }
    }

    pub fn storage_policy_from_pb(policy: StoragePolicyProto) -> StoragePolicy {
        StoragePolicy {
            storage_type: StorageType::from(policy.storage_type),
            ttl_ms: policy.ttl_ms,
            ttl_action: TtlAction::from(policy.ttl_action),
            ufs_mtime: policy.ufs_mtime,
        }
    }

    pub fn file_status_to_pb(status: FileStatus) -> FileStatusProto {
        FileStatusProto {
            id: status.id,
            path: status.path,
            name: status.name,
            is_dir: status.is_dir,
            mtime: status.mtime,
            atime: status.atime,
            children_num: status.children_num,
            is_complete: status.is_complete,
            len: status.len,
            replicas: status.replicas,
            block_size: status.block_size,
            file_type: status.file_type.into(),
            x_attr: status.x_attr,
            storage_policy: Self::storage_policy_to_pb(status.storage_policy),

            owner: status.owner,
            group: status.group,
            mode: status.mode,
            target: status.target,
            nlink: status.nlink,
        }
    }

    pub fn file_status_from_pb(status: FileStatusProto) -> FileStatus {
        FileStatus {
            id: status.id,
            path: status.path,
            name: status.name,
            is_dir: status.is_dir,
            mtime: status.mtime,
            atime: status.atime,
            children_num: status.children_num,
            is_complete: status.is_complete,
            len: status.len,
            replicas: status.replicas,
            block_size: status.block_size,
            file_type: FileType::from(status.file_type),
            x_attr: status.x_attr,
            storage_policy: Self::storage_policy_from_pb(status.storage_policy),
            owner: status.owner,
            group: status.group,
            mode: status.mode,
            nlink: status.nlink,
            target: status.target,
        }
    }

    pub fn file_blocks_to_pb(src: FileBlocks) -> FileBlocksProto {
        let block_locs: Vec<LocatedBlockProto> = src
            .block_locs
            .into_iter()
            .map(Self::located_block_to_pb)
            .collect();

        FileBlocksProto {
            status: Self::file_status_to_pb(src.status),
            block_locs,
        }
    }

    pub fn file_blocks_from_pb(src: FileBlocksProto) -> FileBlocks {
        let block_locs: Vec<LocatedBlock> = src
            .block_locs
            .into_iter()
            .map(Self::located_block_from_pb)
            .collect();

        FileBlocks {
            status: Self::file_status_from_pb(src.status),
            block_locs,
        }
    }

    pub fn master_info_to_pb(src: MasterInfo) -> GetMasterInfoResponse {
        let mut pb = GetMasterInfoResponse {
            active_master: src.active_master,
            journal_nodes: src.journal_nodes,
            inode_dir_num: src.inode_dir_num,
            inode_file_num: src.inode_file_num,
            block_num: src.block_num,
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            reserved_bytes: src.reserved_bytes,
            ..Default::default()
        };

        for item in src.live_workers {
            pb.live_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.blacklist_workers {
            pb.blacklist_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.decommission_workers {
            pb.decommission_workers.push(Self::worker_info_to_pb(item));
        }

        for item in src.lost_workers {
            pb.lost_workers.push(Self::worker_info_to_pb(item));
        }

        pb
    }

    pub fn worker_info_to_pb(src: WorkerInfo) -> WorkerInfoProto {
        let mut pb = WorkerInfoProto {
            address: ProtoUtils::worker_address_to_pb(&src.address),
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            last_update: src.last_update,
            reserved_bytes: src.reserved_bytes,
            storage_map: Default::default(),
        };

        for item in src.storage_map {
            pb.storage_map
                .insert(item.0, Self::storage_info_to_pb(item.1));
        }

        pb
    }

    pub fn master_info_from_pb(src: GetMasterInfoResponse) -> MasterInfo {
        MasterInfo {
            active_master: src.active_master,
            journal_nodes: src.journal_nodes,
            inode_dir_num: src.inode_dir_num,
            inode_file_num: src.inode_file_num,
            block_num: src.block_num,
            capacity: src.capacity,
            available: src.available,
            fs_used: src.fs_used,
            non_fs_used: src.non_fs_used,
            reserved_bytes: src.reserved_bytes,
            live_workers: Self::worker_info_from_pb(src.live_workers),
            blacklist_workers: Self::worker_info_from_pb(src.blacklist_workers),
            decommission_workers: Self::worker_info_from_pb(src.decommission_workers),
            lost_workers: Self::worker_info_from_pb(src.lost_workers),
        }
    }

    pub fn worker_info_from_pb(workers: Vec<WorkerInfoProto>) -> Vec<WorkerInfo> {
        let mut vec = vec![];
        for info in workers {
            let mut worker_info = WorkerInfo {
                address: Self::worker_address_from_pb(&info.address),
                capacity: info.capacity,
                available: info.available,
                fs_used: info.fs_used,
                non_fs_used: info.non_fs_used,
                reserved_bytes: info.reserved_bytes,
                ..Default::default()
            };
            for (k, v) in info.storage_map {
                worker_info
                    .storage_map
                    .insert(k, Self::storage_info_from_pb(v));
            }
            vec.push(worker_info);
        }

        vec
    }

    pub fn worker_cmd_to_pb(cmds: Vec<WorkerCommand>) -> Vec<WorkerCommandProto> {
        let mut vec = vec![];
        for cmd in cmds {
            match cmd {
                WorkerCommand::DeleteBlock(cmd) => {
                    let pb_cmd = WorkerCommandProto {
                        delete_block: Some(DeleteBlockCmdProto { blocks: cmd.blocks }),
                    };
                    vec.push(pb_cmd)
                }
            }
        }

        vec
    }

    pub fn worker_cmd_from_pb(cmds: Vec<WorkerCommandProto>) -> Vec<WorkerCommand> {
        let mut vec = vec![];
        for cmd in cmds {
            if let Some(c) = cmd.delete_block {
                let my_cmd = WorkerCommand::DeleteBlock(DeleteBlockCmd { blocks: c.blocks });
                vec.push(my_cmd);
            }
        }
        vec
    }

    pub fn block_report_list_from_pb(list: BlockReportListRequest) -> BlockReportList {
        let mut dst = BlockReportList {
            cluster_id: list.cluster_id,
            worker_id: list.worker_id,
            full_report: list.full_report,
            total_len: list.total_len,
            blocks: vec![],
        };
        for block in list.blocks {
            let dst_blocks = BlockReportInfo {
                id: block.id,
                status: BlockReportStatus::from(block.status),
                storage_type: StorageType::from(block.storage_type),
                block_size: block.block_size,
            };
            dst.blocks.push(dst_blocks);
        }
        dst
    }

    pub fn set_attr_opts_to_pb(opts: SetAttrOpts) -> SetAttrOptsProto {
        SetAttrOptsProto {
            recursive: opts.recursive,
            replicas: opts.replicas,
            owner: opts.owner,
            group: opts.group,
            mode: opts.mode,
            atime: opts.atime,
            mtime: opts.mtime,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(|v| v as i32),
            add_x_attr: opts.add_x_attr,
            remove_x_attr: opts.remove_x_attr,
            ufs_mtime: opts.ufs_mtime,
        }
    }

    pub fn set_attr_opts_from_pb(opts: SetAttrOptsProto) -> SetAttrOpts {
        SetAttrOpts {
            recursive: opts.recursive,
            replicas: opts.replicas,
            owner: opts.owner,
            group: opts.group,
            mode: opts.mode,
            atime: opts.atime,
            mtime: opts.mtime,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(TtlAction::from),
            add_x_attr: opts.add_x_attr,
            remove_x_attr: opts.remove_x_attr,
            ufs_mtime: opts.ufs_mtime,
        }
    }

    pub fn create_opts_to_pb(
        opts: CreateFileOpts,
        client_name: impl Into<String>,
    ) -> CreateFileOptsProto {
        CreateFileOptsProto {
            create_flag: 0,
            create_parent: opts.create_parent,
            file_type: opts.file_type.into(),
            replicas: opts.replicas as i32,
            block_size: opts.block_size,
            x_attr: opts.x_attr,
            storage_policy: ProtoUtils::storage_policy_to_pb(opts.storage_policy),
            client_name: client_name.into(),
            mode: opts.mode,
            owner: opts.owner,
            group: opts.group,
        }
    }

    pub fn create_opts_from_pb(opts: CreateFileOptsProto) -> CreateFileOpts {
        CreateFileOpts {
            create_parent: opts.create_parent,
            file_type: FileType::from(opts.file_type),
            replicas: opts.replicas as u16,
            block_size: opts.block_size,
            x_attr: opts.x_attr,
            storage_policy: ProtoUtils::storage_policy_from_pb(opts.storage_policy),
            mode: opts.mode,
            client_name: opts.client_name,
            owner: opts.owner,
            group: opts.group,
        }
    }

    pub fn mkdir_opts_to_pb(opts: MkdirOpts) -> MkdirOptsProto {
        MkdirOptsProto {
            create_parent: opts.create_parent,
            x_attr: opts.x_attr,
            storage_policy: ProtoUtils::storage_policy_to_pb(opts.storage_policy),
            mode: opts.mode,
            owner: opts.owner,
            group: opts.group,
        }
    }

    pub fn mkdir_opts_from_pb(opts: MkdirOptsProto) -> MkdirOpts {
        MkdirOpts {
            create_parent: opts.create_parent,
            x_attr: opts.x_attr,
            storage_policy: ProtoUtils::storage_policy_from_pb(opts.storage_policy),
            mode: opts.mode,
            owner: opts.owner,
            group: opts.group,
        }
    }

    pub fn mount_info_to_pb(info: MountInfo) -> MountInfoProto {
        MountInfoProto {
            cv_path: info.cv_path,
            ufs_path: info.ufs_path,
            mount_id: info.mount_id,
            namespace_id: info.namespace_id as u32,
            properties: info.properties,
            mount_type: info.mount_type.into(),
            write_type: info.write_type.into(),
            provider: info.provider.map(|v| v.into()),
            version: Some(info.version),
        }
    }

    pub fn mount_info_from_pb(info: MountInfoProto) -> MountInfo {
        MountInfo {
            cv_path: info.cv_path,
            ufs_path: info.ufs_path,
            mount_id: info.mount_id,
            namespace_id: info.namespace_id as NamespaceId,
            properties: info.properties,
            mount_type: info.mount_type.into(),
            write_type: WriteType::from(info.write_type),
            provider: info.provider.map(|x| x.into()),
            version: info.version.unwrap_or_default(),
        }
    }

    pub fn mount_options_to_pb(opts: MountOptions) -> MountOptionsProto {
        MountOptionsProto {
            update: opts.update,
            add_properties: opts.add_properties,
            mount_type: opts.mount_type.into(),
            remove_properties: opts.remove_properties,
            write_type: opts.write_type.into(),
            provider: opts.provider.map(|v| v.into()),
            namespace_name: opts.namespace_name,
        }
    }

    pub fn mount_options_from_pb(opts: MountOptionsProto) -> MountOptions {
        MountOptions {
            update: opts.update,
            add_properties: opts.add_properties,
            mount_type: MountType::from(opts.mount_type),
            remove_properties: opts.remove_properties,
            write_type: opts.write_type.into(),
            provider: opts.provider.map(Provider::from),
            namespace_name: opts.namespace_name,
        }
    }

    pub fn work_progress_to_pb(report: JobTaskProgress) -> JobTaskProgressProto {
        JobTaskProgressProto {
            loaded_size: report.loaded_size,
            total_size: report.total_size,
            update_time: report.update_time,
            state: report.state as i8 as i32,
            message: report.message,
        }
    }

    pub fn work_progress_from_pb(report: JobTaskProgressProto) -> JobTaskProgress {
        JobTaskProgress {
            loaded_size: report.loaded_size,
            total_size: report.total_size,
            update_time: report.update_time,
            state: JobTaskState::from(report.state as i8),
            message: report.message,
        }
    }

    pub fn metrics_report_from_pb(report: Vec<MetricValueProto>) -> Vec<MetricValue> {
        report
            .into_iter()
            .map(|metric| MetricValue {
                metric_type: metric.metric_type.into(),
                name: metric.name,
                value: metric.value,
                tags: metric.tags,
            })
            .collect()
    }

    pub fn metrics_report_to_pb(report: Vec<MetricValue>) -> Vec<MetricValueProto> {
        report
            .into_iter()
            .map(|metric| MetricValueProto {
                metric_type: metric.metric_type.into(),
                name: metric.name,
                value: metric.value,
                tags: metric.tags,
            })
            .collect()
    }

    pub fn file_alloc_opts_to_pb(opts: FileAllocOpts) -> FileAllocOptsProto {
        FileAllocOptsProto {
            truncate: opts.truncate,
            off: opts.off,
            len: opts.len,
            mode: opts.mode.bits(),
        }
    }

    pub fn file_alloc_opts_from_pb(opts: FileAllocOptsProto) -> FileAllocOpts {
        FileAllocOpts {
            truncate: opts.truncate,
            off: opts.off,
            len: opts.len,
            mode: FileAllocMode::from_bits_truncate(opts.mode),
        }
    }

    pub fn file_lock_to_pb(lock: FileLock) -> FileLockProto {
        FileLockProto {
            client_id: lock.client_id,
            owner_id: lock.owner_id,
            pid: lock.pid,
            acquire_time: lock.acquire_time,
            lock_type: lock.lock_type as i32,
            lock_flags: lock.lock_flags as i32,
            start: lock.start,
            end: lock.end,
        }
    }

    pub fn file_lock_from_pb(lock: FileLockProto) -> FileLock {
        FileLock {
            client_id: lock.client_id,
            owner_id: lock.owner_id,
            pid: lock.pid,
            acquire_time: lock.acquire_time,
            lock_type: LockType::from(lock.lock_type as u8),
            lock_flags: LockFlags::from(lock.lock_flags as u8),
            start: lock.start,
            end: lock.end,
        }
    }

    pub fn config_info_to_pb(item: &ConfigInfo) -> ConfigItemProto {
        ConfigItemProto {
            key: item.key.clone(),
            value: item.value.clone(),
            version: item.version,
            mtime: item.mtime,
        }
    }

    pub fn config_info_from_pb(pb: ConfigItemProto) -> ConfigInfo {
        let mut item = ConfigInfo::new(pb.key, pb.value);
        item.version = pb.version;
        item.mtime = pb.mtime;
        item
    }

    pub fn set_config_request_to_config_info(pb: SetConfigRequest) -> ConfigInfo {
        ConfigInfo::new(pb.key, pb.value)
    }

    pub fn set_config_request_from_http(key: String, value: Vec<u8>) -> SetConfigRequest {
        SetConfigRequest { key, value }
    }

    fn invalid_proto(msg: impl Into<String>) -> FsError {
        FsError::common(format!("invalid proto: {}", msg.into()))
    }

    pub fn node_type_to_pb(t: NodeType) -> i32 {
        match t {
            NodeType::Worker => 0,
            NodeType::Meta => 1,
            NodeType::Task => 2,
        }
    }

    pub fn node_type_from_pb(v: i32) -> FsResult<NodeType> {
        match v {
            0 => Ok(NodeType::Worker),
            1 => Ok(NodeType::Meta),
            2 => Ok(NodeType::Task),
            _ => Err(Self::invalid_proto(format!("unknown node_type={}", v))),
        }
    }

    pub fn node_state_to_pb(s: NodeState) -> i32 {
        match s {
            NodeState::Starting => 0,
            NodeState::Live => 1,
            NodeState::Lost => 2,
            NodeState::Offline => 3,
            NodeState::Decommission => 4,
            NodeState::Blacklist => 5,
        }
    }

    pub fn node_state_from_pb(v: i32) -> FsResult<NodeState> {
        match v {
            0 => Ok(NodeState::Starting),
            1 => Ok(NodeState::Live),
            2 => Ok(NodeState::Lost),
            3 => Ok(NodeState::Offline),
            4 => Ok(NodeState::Decommission),
            5 => Ok(NodeState::Blacklist),
            _ => Err(Self::invalid_proto(format!("unknown node_state={}", v))),
        }
    }

    pub fn replica_state_to_pb(s: ReplicaState) -> i32 {
        match s {
            ReplicaState::Pending => 0,
            ReplicaState::Recovering => 1,
            ReplicaState::Syncing => 2,
            ReplicaState::Active => 3,
            ReplicaState::Sealed => 4,
            ReplicaState::Draining => 5,
        }
    }

    pub fn replica_state_from_pb(v: i32) -> FsResult<ReplicaState> {
        match v {
            0 => Ok(ReplicaState::Pending),
            1 => Ok(ReplicaState::Recovering),
            2 => Ok(ReplicaState::Syncing),
            3 => Ok(ReplicaState::Active),
            4 => Ok(ReplicaState::Sealed),
            5 => Ok(ReplicaState::Draining),
            _ => Err(Self::invalid_proto(format!("unknown replica_state={}", v))),
        }
    }

    pub fn bg_state_to_pb(s: BGState) -> i32 {
        match s {
            BGState::Init => 0,
            BGState::Active => 1,
            BGState::Degraded => 2,
            BGState::Sealed => 3,
        }
    }

    pub fn bg_state_from_pb(v: i32) -> FsResult<BGState> {
        match v {
            0 => Ok(BGState::Init),
            1 => Ok(BGState::Active),
            2 => Ok(BGState::Degraded),
            3 => Ok(BGState::Sealed),
            _ => Err(Self::invalid_proto(format!("unknown bg_state={}", v))),
        }
    }

    pub fn bg_op_state_to_pb(s: BGOpState) -> i32 {
        match s {
            BGOpState::Idle => 0,
            BGOpState::Repairing => 1,
            BGOpState::Rebalancing => 2,
            BGOpState::PrimaryTransfer => 3,
            BGOpState::Sealing => 4,
            BGOpState::Deleting => 5,
        }
    }

    pub fn bg_op_state_from_pb(v: i32) -> FsResult<BGOpState> {
        match v {
            0 => Ok(BGOpState::Idle),
            1 => Ok(BGOpState::Repairing),
            2 => Ok(BGOpState::Rebalancing),
            3 => Ok(BGOpState::PrimaryTransfer),
            4 => Ok(BGOpState::Sealing),
            5 => Ok(BGOpState::Deleting),
            _ => Err(Self::invalid_proto(format!("unknown bg_op_state={}", v))),
        }
    }

    pub fn bg_kind_to_pb(kind: BGKind) -> u32 {
        match kind {
            BGKind::Hash => 0,
            BGKind::Capacity => 1,
        }
    }

    pub fn bg_kind_from_pb(v: u32) -> FsResult<BGKind> {
        match v {
            0 => Ok(BGKind::Hash),
            1 => Ok(BGKind::Capacity),
            _ => Err(Self::invalid_proto(format!("unknown bg_kind={}", v))),
        }
    }

    pub fn rw_policy_to_pb(p: RwPolicy) -> i32 {
        match p {
            RwPolicy::LeaderOnly => 0,
            RwPolicy::LeaderWriteFollowerRead => 1,
        }
    }

    pub fn rw_policy_from_pb(v: i32) -> FsResult<RwPolicy> {
        match v {
            0 => Ok(RwPolicy::LeaderOnly),
            1 => Ok(RwPolicy::LeaderWriteFollowerRead),
            _ => Err(Self::invalid_proto(format!("unknown rw_policy={}", v))),
        }
    }

    pub fn meta_node_mode_to_pb(m: MetaNodeMode) -> i32 {
        match m {
            MetaNodeMode::Proxy => 0,
            MetaNodeMode::Shard => 1,
            MetaNodeMode::Federation => 2,
        }
    }

    pub fn meta_node_mode_from_pb(v: i32) -> FsResult<MetaNodeMode> {
        match v {
            0 => Ok(MetaNodeMode::Proxy),
            1 => Ok(MetaNodeMode::Shard),
            2 => Ok(MetaNodeMode::Federation),
            _ => Err(Self::invalid_proto(format!("unknown meta_node_mode={}", v))),
        }
    }

    pub fn federation_route_config_to_pb(
        config: &FederationRouteConfig,
    ) -> FederationRouteConfigProto {
        FederationRouteConfigProto {
            hash_level: config.hash_level.map(|v| v as u32),
        }
    }

    pub fn federation_route_config_from_pb(
        config: FederationRouteConfigProto,
    ) -> FederationRouteConfig {
        FederationRouteConfig {
            hash_level: config.hash_level.map(|v| v as u8),
        }
    }

    pub fn node_address_to_pb(addr: &NodeAddress) -> NodeAddressProto {
        NodeAddressProto {
            hostname: addr.hostname.clone(),
            ip: addr.ip.clone(),
            rpc_port: addr.rpc_port as u32,
            web_port: addr.web_port as u32,
        }
    }

    pub fn node_address_from_pb(addr: NodeAddressProto) -> FsResult<NodeAddress> {
        if addr.rpc_port > u16::MAX as u32 || addr.web_port > u16::MAX as u32 {
            return Err(Self::invalid_proto(format!(
                "node address port out of range rpc_port={} web_port={}",
                addr.rpc_port, addr.web_port
            )));
        }
        Ok(NodeAddress {
            hostname: addr.hostname,
            ip: addr.ip,
            rpc_port: addr.rpc_port as u16,
            web_port: addr.web_port as u16,
        })
    }

    pub fn system_stats_to_pb(stats: &SystemStats) -> SystemStatsProto {
        SystemStatsProto {
            cpu_usage: stats.cpu_usage,
            memory_usage: stats.memory_usage,
        }
    }

    pub fn system_stats_from_pb(stats: SystemStatsProto) -> SystemStats {
        SystemStats {
            cpu_usage: stats.cpu_usage,
            memory_usage: stats.memory_usage,
        }
    }

    pub fn node_base_to_pb(base: &NodeBase) -> NodeBaseProto {
        NodeBaseProto {
            node_id: base.node_id,
            node_type: Self::node_type_to_pb(base.node_type),
            address: Self::node_address_to_pb(&base.address),
            labels: base.labels.clone(),
            software_version: base.software_version.clone(),
            startup_time_ms: base.startup_time_ms,
        }
    }

    pub fn node_base_from_pb(base: NodeBaseProto) -> FsResult<NodeBase> {
        if base.node_id == 0 {
            return Err(Self::invalid_proto("node_id must not be 0"));
        }
        Ok(NodeBase {
            node_id: base.node_id,
            node_type: Self::node_type_from_pb(base.node_type)?,
            address: Self::node_address_from_pb(base.address)?,
            labels: base.labels,
            software_version: base.software_version,
            startup_time_ms: base.startup_time_ms,
        })
    }

    pub fn storage_spec_to_pb(spec: &StorageSpec) -> StorageSpecProto {
        StorageSpecProto {
            dir_id: spec.dir_id,
            storage_id: spec.storage_id.clone(),
            failed: spec.failed,
            storage_type: spec.storage_type.into(),
            dir_path: spec.dir_path.clone(),
        }
    }

    pub fn storage_spec_from_pb(spec: StorageSpecProto) -> StorageSpec {
        StorageSpec {
            dir_id: spec.dir_id,
            storage_id: spec.storage_id,
            failed: spec.failed,
            storage_type: StorageType::from(spec.storage_type),
            dir_path: spec.dir_path,
        }
    }

    pub fn storage_stats_to_pb(stats: &StorageStats) -> StorageStatsProto {
        StorageStatsProto {
            capacity: stats.capacity,
            fs_used: stats.fs_used,
            non_fs_used: stats.non_fs_used,
            available: stats.available,
            reserved_bytes: stats.reserved_bytes,
            block_num: stats.block_num,
            dir_path: stats.dir_path.clone(),
        }
    }

    pub fn storage_stats_from_pb(stats: StorageStatsProto) -> StorageStats {
        StorageStats {
            capacity: stats.capacity,
            fs_used: stats.fs_used,
            non_fs_used: stats.non_fs_used,
            available: stats.available,
            reserved_bytes: stats.reserved_bytes,
            block_num: stats.block_num,
            dir_path: stats.dir_path,
        }
    }

    pub fn bg_stats_to_pb(stats: &BGStats) -> BgStatsProto {
        BgStatsProto {
            used_bytes: stats.used_bytes,
            free_bytes: stats.free_bytes,
            block_count: stats.block_count,
            last_report_ms: stats.last_report_ms,
        }
    }

    pub fn bg_stats_from_pb(stats: BgStatsProto) -> BGStats {
        BGStats {
            used_bytes: stats.used_bytes,
            free_bytes: stats.free_bytes,
            block_count: stats.block_count,
            last_report_ms: stats.last_report_ms,
        }
    }

    pub fn worker_bg_report_to_pb(report: &WorkerBGReport) -> WorkerBgReportProto {
        WorkerBgReportProto {
            kind: Self::bg_kind_to_pb(report.kind),
            bg_id: report.bg_id,
            bg_epoch: report.bg_epoch,
            state: Self::replica_state_to_pb(report.state),
            stats: Some(Self::bg_stats_to_pb(&report.stats)),
            isr_remove_candidates: report.isr_remove_candidates.clone(),
        }
    }

    pub fn worker_bg_report_from_pb(report: WorkerBgReportProto) -> FsResult<WorkerBGReport> {
        Ok(WorkerBGReport {
            kind: Self::bg_kind_from_pb(report.kind)?,
            bg_id: report.bg_id,
            bg_epoch: report.bg_epoch,
            state: Self::replica_state_from_pb(report.state)?,
            stats: report.stats.map(Self::bg_stats_from_pb).unwrap_or_default(),
            isr_remove_candidates: report.isr_remove_candidates,
        })
    }

    pub fn worker_register_payload_to_pb(
        payload: &WorkerNodePayload,
    ) -> WorkerRegisterPayloadProto {
        WorkerRegisterPayloadProto {
            storage_specs: payload
                .storage_specs
                .iter()
                .map(|(k, v)| (k.clone(), Self::storage_spec_to_pb(v)))
                .collect(),
        }
    }

    pub fn worker_register_payload_from_pb(
        payload: WorkerRegisterPayloadProto,
    ) -> WorkerNodePayload {
        WorkerNodePayload {
            storage_specs: payload
                .storage_specs
                .into_iter()
                .map(|(k, v)| (k, Self::storage_spec_from_pb(v)))
                .collect(),
            storage_stats: Default::default(),
            bg_reports: Default::default(),
        }
    }

    pub fn worker_heartbeat_payload_to_pb(
        payload: &WorkerHeartbeatPayload,
    ) -> WorkerHeartbeatPayloadProto {
        WorkerHeartbeatPayloadProto {
            storage_stats: payload
                .storage_stats
                .iter()
                .map(|(k, v)| (k.clone(), Self::storage_stats_to_pb(v)))
                .collect(),
            sys_stats: Self::system_stats_to_pb(&payload.sys_stats),
            bg_reports: payload
                .bg_reports
                .iter()
                .map(Self::worker_bg_report_to_pb)
                .collect(),
        }
    }

    pub fn worker_heartbeat_payload_from_pb(
        payload: WorkerHeartbeatPayloadProto,
    ) -> FsResult<WorkerHeartbeatPayload> {
        let mut bg_reports = Vec::with_capacity(payload.bg_reports.len());
        for report in payload.bg_reports {
            bg_reports.push(Self::worker_bg_report_from_pb(report)?);
        }
        Ok(WorkerHeartbeatPayload {
            storage_stats: payload
                .storage_stats
                .into_iter()
                .map(|(k, v)| (k, Self::storage_stats_from_pb(v)))
                .collect(),
            sys_stats: Self::system_stats_from_pb(payload.sys_stats),
            bg_reports,
        })
    }

    pub fn peer_info_to_pb(peer: &PeerInfo) -> PeerInfoProto {
        PeerInfoProto {
            node_id: peer.node_id,
            address: Self::node_address_to_pb(&peer.address),
            is_leader: peer.is_leader,
        }
    }

    pub fn peer_info_from_pb(peer: PeerInfoProto) -> FsResult<PeerInfo> {
        Ok(PeerInfo {
            node_id: peer.node_id,
            address: Self::node_address_from_pb(peer.address)?,
            is_leader: peer.is_leader,
        })
    }

    pub fn node_group_info_to_pb(group: &NodeGroupInfo) -> NodeGroupInfoProto {
        NodeGroupInfoProto {
            group_id: group.group_id,
            peers: group.peers.iter().map(Self::peer_info_to_pb).collect(),
        }
    }

    pub fn node_group_info_from_pb(group: NodeGroupInfoProto) -> FsResult<NodeGroupInfo> {
        let mut peers = Vec::with_capacity(group.peers.len());
        for peer in group.peers {
            peers.push(Self::peer_info_from_pb(peer)?);
        }
        Ok(NodeGroupInfo {
            group_id: group.group_id,
            peers,
        })
    }

    pub fn inodes_stats_to_pb(stats: &InodesStats) -> InodesStatsProto {
        InodesStatsProto {
            inode_count: stats.inode_count,
            dir_count: stats.dir_count,
            file_count: stats.file_count,
            total_size: stats.total_size,
        }
    }

    pub fn inodes_stats_from_pb(stats: InodesStatsProto) -> InodesStats {
        InodesStats {
            inode_count: stats.inode_count,
            dir_count: stats.dir_count,
            file_count: stats.file_count,
            total_size: stats.total_size,
        }
    }

    pub fn meta_register_payload_to_pb(payload: &MetaNodePayload) -> MetaRegisterPayloadProto {
        MetaRegisterPayloadProto {
            group_id: payload.group_id,
            peers: payload.peers.iter().map(Self::peer_info_to_pb).collect(),
            rw_policy: Self::rw_policy_to_pb(payload.rw_policy),
            group_epoch: payload.group_epoch,
        }
    }

    pub fn meta_register_payload_from_pb(
        payload: MetaRegisterPayloadProto,
    ) -> FsResult<MetaNodePayload> {
        let mut peers = Vec::with_capacity(payload.peers.len());
        for peer in payload.peers {
            peers.push(Self::peer_info_from_pb(peer)?);
        }
        Ok(MetaNodePayload {
            group_id: payload.group_id,
            peers,
            rw_policy: Self::rw_policy_from_pb(payload.rw_policy)?,
            group_epoch: payload.group_epoch,
            stats: Default::default(),
        })
    }

    pub fn meta_heartbeat_payload_to_pb(
        payload: &MetaHeartbeatPayload,
    ) -> MetaHeartbeatPayloadProto {
        MetaHeartbeatPayloadProto {
            group_id: payload.group_id,
            group_epoch: payload.group_epoch,
            is_leader: payload.is_leader,
            peers: payload.peers.iter().map(Self::peer_info_to_pb).collect(),
            rw_policy: Self::rw_policy_to_pb(payload.rw_policy),
            inodes_stats: Self::inodes_stats_to_pb(&payload.inodes_stats),
            sys_stats: Self::system_stats_to_pb(&payload.sys_stats),
        }
    }

    pub fn meta_heartbeat_payload_from_pb(
        payload: MetaHeartbeatPayloadProto,
    ) -> FsResult<MetaHeartbeatPayload> {
        let mut peers = Vec::with_capacity(payload.peers.len());
        for peer in payload.peers {
            peers.push(Self::peer_info_from_pb(peer)?);
        }
        Ok(MetaHeartbeatPayload {
            group_id: payload.group_id,
            group_epoch: payload.group_epoch,
            is_leader: payload.is_leader,
            peers,
            rw_policy: Self::rw_policy_from_pb(payload.rw_policy)?,
            inodes_stats: Self::inodes_stats_from_pb(payload.inodes_stats),
            sys_stats: Self::system_stats_from_pb(payload.sys_stats),
        })
    }

    pub fn task_node_stats_to_pb(stats: &TaskNodeStats) -> TaskNodeStatsProto {
        TaskNodeStatsProto {
            running_tasks: stats.running_tasks,
            failed_tasks: stats.failed_tasks,
        }
    }

    pub fn task_node_stats_from_pb(stats: TaskNodeStatsProto) -> TaskNodeStats {
        TaskNodeStats {
            running_tasks: stats.running_tasks,
            failed_tasks: stats.failed_tasks,
        }
    }

    pub fn task_register_payload_to_pb(_payload: &TaskNodePayload) -> TaskRegisterPayloadProto {
        TaskRegisterPayloadProto {}
    }

    pub fn task_register_payload_from_pb(_payload: TaskRegisterPayloadProto) -> TaskNodePayload {
        TaskNodePayload::default()
    }

    pub fn task_heartbeat_payload_to_pb(
        payload: &TaskHeartbeatPayload,
    ) -> TaskHeartbeatPayloadProto {
        TaskHeartbeatPayloadProto {
            sys_stats: Self::system_stats_to_pb(&payload.sys_stats),
            stats: Self::task_node_stats_to_pb(&payload.stats),
        }
    }

    pub fn task_heartbeat_payload_from_pb(
        payload: TaskHeartbeatPayloadProto,
    ) -> TaskHeartbeatPayload {
        TaskHeartbeatPayload {
            sys_stats: Self::system_stats_from_pb(payload.sys_stats),
            stats: Self::task_node_stats_from_pb(payload.stats),
        }
    }

    pub fn register_request_to_pb(req: &RegisterRequest) -> NodeRegisterRequest {
        let (worker, meta, task) = match &req.payload {
            NodePayload::Worker(p) => (Some(Self::worker_register_payload_to_pb(p)), None, None),
            NodePayload::Meta(p) => (None, Some(Self::meta_register_payload_to_pb(p)), None),
            NodePayload::Task(p) => (None, None, Some(Self::task_register_payload_to_pb(p))),
        };
        NodeRegisterRequest {
            cluster_id: req.cluster_id.clone(),
            base: Self::node_base_to_pb(&req.base),
            worker,
            meta,
            task,
        }
    }

    pub fn register_request_from_pb(req: NodeRegisterRequest) -> FsResult<RegisterRequest> {
        let base = Self::node_base_from_pb(req.base)?;
        let payload = match (base.node_type, req.worker, req.meta, req.task) {
            (NodeType::Worker, Some(worker), None, None) => {
                NodePayload::Worker(Self::worker_register_payload_from_pb(worker))
            }
            (NodeType::Meta, None, Some(meta), None) => {
                NodePayload::Meta(Self::meta_register_payload_from_pb(meta)?)
            }
            (NodeType::Task, None, None, Some(task)) => {
                NodePayload::Task(Self::task_register_payload_from_pb(task))
            }
            (node_type, worker, meta, task) => {
                return Err(Self::invalid_proto(format!(
                    "register payload mismatch node_type={:?}, worker_set={}, meta_set={}, task_set={}",
                    node_type,
                    worker.is_some(),
                    meta.is_some(),
                    task.is_some()
                )));
            }
        };
        Ok(RegisterRequest {
            cluster_id: req.cluster_id,
            base,
            payload,
        })
    }

    pub fn heartbeat_request_to_pb(req: &HeartbeatRequest) -> NodeHeartbeatRequest {
        let (worker, meta, task) = match &req.payload {
            HeartbeatPayload::Worker(p) => {
                (Some(Self::worker_heartbeat_payload_to_pb(p)), None, None)
            }
            HeartbeatPayload::Meta(p) => (None, Some(Self::meta_heartbeat_payload_to_pb(p)), None),
            HeartbeatPayload::Task(p) => (None, None, Some(Self::task_heartbeat_payload_to_pb(p))),
        };
        NodeHeartbeatRequest {
            cluster_id: req.cluster_id.clone(),
            node_id: req.node_id,
            node_type: Self::node_type_to_pb(req.node_type),
            epoch: req.epoch,
            timestamp_ms: req.timestamp_ms,
            address: Self::node_address_to_pb(&req.address),
            worker,
            meta,
            task,
        }
    }

    pub fn heartbeat_request_from_pb(req: NodeHeartbeatRequest) -> FsResult<HeartbeatRequest> {
        if req.node_id == 0 {
            return Err(Self::invalid_proto("node_id must not be 0"));
        }
        let node_type = Self::node_type_from_pb(req.node_type)?;
        let payload = match (node_type, req.worker, req.meta, req.task) {
            (NodeType::Worker, Some(worker), None, None) => {
                HeartbeatPayload::Worker(Self::worker_heartbeat_payload_from_pb(worker)?)
            }
            (NodeType::Meta, None, Some(meta), None) => {
                HeartbeatPayload::Meta(Self::meta_heartbeat_payload_from_pb(meta)?)
            }
            (NodeType::Task, None, None, Some(task)) => {
                HeartbeatPayload::Task(Self::task_heartbeat_payload_from_pb(task))
            }
            (node_type, worker, meta, task) => {
                return Err(Self::invalid_proto(format!(
                    "heartbeat payload mismatch node_type={:?}, worker_set={}, meta_set={}, task_set={}",
                    node_type,
                    worker.is_some(),
                    meta.is_some(),
                    task.is_some()
                )));
            }
        };
        Ok(HeartbeatRequest {
            cluster_id: req.cluster_id,
            node_id: req.node_id,
            node_type,
            epoch: req.epoch,
            timestamp_ms: req.timestamp_ms,
            address: Self::node_address_from_pb(req.address)?,
            payload,
        })
    }

    pub fn bg_primary_to_pb(primary: &BGPrimary) -> BgPrimaryProto {
        BgPrimaryProto {
            node_id: primary.node_id,
            epoch: primary.epoch,
            grant_time_ms: primary.grant_time_ms,
        }
    }

    pub fn bg_primary_from_pb(primary: BgPrimaryProto) -> BGPrimary {
        BGPrimary {
            node_id: primary.node_id,
            epoch: primary.epoch,
            grant_time_ms: primary.grant_time_ms,
        }
    }

    fn required_bg_primary(primary: Option<BgPrimaryProto>) -> FsResult<BGPrimary> {
        primary
            .map(Self::bg_primary_from_pb)
            .ok_or_else(|| Self::invalid_proto("missing bg primary"))
    }

    pub fn replica_info_to_pb(replica: &ReplicaInfo) -> ReplicaInfoProto {
        ReplicaInfoProto {
            node_id: replica.node_id,
            address: Self::node_address_to_pb(&replica.address),
            state: Self::node_state_to_pb(replica.state),
            labels: replica.labels.clone(),
        }
    }

    pub fn replica_info_from_pb(replica: ReplicaInfoProto) -> FsResult<ReplicaInfo> {
        Ok(ReplicaInfo {
            node_id: replica.node_id,
            address: Self::node_address_from_pb(replica.address)?,
            state: Self::node_state_from_pb(replica.state)?,
            labels: replica.labels,
        })
    }

    pub fn block_group_info_to_pb(bg: &BlockGroupInfo) -> BlockGroupInfoProto {
        BlockGroupInfoProto {
            bg_id: bg.bg_id,
            table_id: bg.table_id as u32,
            bg_epoch: bg.bg_epoch,
            replica_set: bg.replica_set.clone(),
            state: Self::bg_state_to_pb(bg.state),
            op_state: Self::bg_op_state_to_pb(bg.op_state),
            primary: Some(Self::bg_primary_to_pb(&bg.primary)),
            kind: Self::bg_kind_to_pb(bg.kind),
            isr: bg.isr.clone(),
        }
    }

    pub fn block_group_info_from_pb(bg: BlockGroupInfoProto) -> FsResult<BlockGroupInfo> {
        Ok(BlockGroupInfo {
            bg_id: bg.bg_id,
            table_id: u16::try_from(bg.table_id).map_err(|_| {
                Self::invalid_proto(format!("table_id out of range: {}", bg.table_id))
            })?,
            kind: Self::bg_kind_from_pb(bg.kind)?,
            bg_epoch: bg.bg_epoch,
            replica_set: bg.replica_set,
            isr: bg.isr,
            state: Self::bg_state_from_pb(bg.state)?,
            op_state: Self::bg_op_state_from_pb(bg.op_state)?,
            primary: Self::required_bg_primary(bg.primary)?,
            replicas: Default::default(),
            stats: Default::default(),
        })
    }

    pub fn block_group_route_view_to_pb(bg: &BlockGroupRouteView) -> BlockGroupRouteViewProto {
        BlockGroupRouteViewProto {
            bg_id: bg.bg_id,
            table_id: bg.table_id as u32,
            bg_epoch: bg.bg_epoch,
            serving_replicas: bg
                .serving_replicas
                .iter()
                .map(Self::replica_info_to_pb)
                .collect(),
            state: Self::bg_state_to_pb(bg.state),
            primary: Some(Self::bg_primary_to_pb(&bg.primary)),
            kind: Self::bg_kind_to_pb(bg.kind),
        }
    }

    pub fn block_group_route_view_from_pb(
        bg: BlockGroupRouteViewProto,
    ) -> FsResult<BlockGroupRouteView> {
        let mut serving_replicas = Vec::with_capacity(bg.serving_replicas.len());
        for replica in bg.serving_replicas {
            serving_replicas.push(Self::replica_info_from_pb(replica)?);
        }
        Ok(BlockGroupRouteView {
            bg_id: bg.bg_id,
            table_id: u16::try_from(bg.table_id).map_err(|_| {
                Self::invalid_proto(format!("table_id out of range: {}", bg.table_id))
            })?,
            kind: Self::bg_kind_from_pb(bg.kind)?,
            bg_epoch: bg.bg_epoch,
            serving_replicas,
            state: Self::bg_state_from_pb(bg.state)?,
            primary: Self::required_bg_primary(bg.primary)?,
        })
    }

    pub fn bg_table_summary_to_pb(summary: &BGTableSummary) -> BgTableSummaryProto {
        match summary {
            BGTableSummary::Hash(s) => BgTableSummaryProto {
                table_id: s.table_id as u32,
                epoch: s.epoch,
                kind: Self::bg_kind_to_pb(BGKind::Hash),
                buckets: s
                    .buckets
                    .iter()
                    .map(Self::block_group_route_view_to_pb)
                    .collect(),
                active_bgs: vec![],
                cache_replica_policy: Some(Self::cache_replica_policy_to_pb(
                    &s.cache_replica_policy,
                )),
            },
            BGTableSummary::Capacity(s) => BgTableSummaryProto {
                table_id: s.table_id as u32,
                epoch: s.epoch,
                kind: Self::bg_kind_to_pb(BGKind::Capacity),
                buckets: vec![],
                active_bgs: s
                    .active_bgs
                    .iter()
                    .map(Self::block_group_route_view_to_pb)
                    .collect(),
                cache_replica_policy: None,
            },
        }
    }

    pub fn bg_table_summary_from_pb(summary: BgTableSummaryProto) -> FsResult<BGTableSummary> {
        let table_id = u16::try_from(summary.table_id).map_err(|_| {
            Self::invalid_proto(format!("table_id out of range: {}", summary.table_id))
        })?;
        match Self::bg_kind_from_pb(summary.kind)? {
            BGKind::Hash => {
                let mut buckets = Vec::with_capacity(summary.buckets.len());
                for bucket in summary.buckets {
                    buckets.push(Self::block_group_route_view_from_pb(bucket)?);
                }
                Ok(BGTableSummary::Hash(HashBGTableSummary {
                    table_id,
                    epoch: summary.epoch,
                    cache_replica_policy: summary
                        .cache_replica_policy
                        .map(Self::cache_replica_policy_from_pb)
                        .transpose()?
                        .unwrap_or_default(),
                    buckets,
                }))
            }
            BGKind::Capacity => {
                let mut active_bgs = Vec::with_capacity(summary.active_bgs.len());
                for bg in summary.active_bgs {
                    active_bgs.push(Self::block_group_route_view_from_pb(bg)?);
                }
                Ok(BGTableSummary::Capacity(CapacityBGTableSummary {
                    table_id,
                    epoch: summary.epoch,
                    active_bgs,
                }))
            }
        }
    }

    pub fn simple_bg_table_view_to_pb(view: &SimpleBGTableView) -> SimpleBgTableViewProto {
        SimpleBgTableViewProto {
            table_id: view.table_id as u32,
            kind: Self::bg_kind_to_pb(view.kind),
            storage_type: view.storage_type.into(),
            replica_count: view.replica_count as u32,
            route_epoch: view.route_epoch,
        }
    }

    pub fn simple_bg_table_view_from_pb(
        view: SimpleBgTableViewProto,
    ) -> FsResult<SimpleBGTableView> {
        Ok(SimpleBGTableView {
            table_id: u16::try_from(view.table_id).map_err(|_| {
                Self::invalid_proto(format!("table_id out of range: {}", view.table_id))
            })?,
            kind: Self::bg_kind_from_pb(view.kind)?,
            storage_type: StorageType::from(view.storage_type),
            replica_count: u16::try_from(view.replica_count).map_err(|_| {
                Self::invalid_proto(format!(
                    "replica_count out of range: {}",
                    view.replica_count
                ))
            })?,
            route_epoch: view.route_epoch,
        })
    }

    pub fn simple_namespace_view_to_pb(view: &SimpleNamespaceView) -> SimpleNamespaceViewProto {
        SimpleNamespaceViewProto {
            namespace_id: view.namespace_id as u32,
            name: view.name.clone(),
            cache_tier_tables: view
                .cache_tier_tables
                .iter()
                .map(Self::simple_bg_table_view_to_pb)
                .collect(),
            write_buffer_table: view
                .write_buffer_table
                .as_ref()
                .map(Self::simple_bg_table_view_to_pb),
        }
    }

    pub fn simple_namespace_view_from_pb(
        view: SimpleNamespaceViewProto,
    ) -> FsResult<SimpleNamespaceView> {
        let mut cache_tier_tables = Vec::with_capacity(view.cache_tier_tables.len());
        for table in view.cache_tier_tables {
            cache_tier_tables.push(Self::simple_bg_table_view_from_pb(table)?);
        }
        Ok(SimpleNamespaceView {
            namespace_id: u16::try_from(view.namespace_id).map_err(|_| {
                Self::invalid_proto(format!("namespace_id out of range: {}", view.namespace_id))
            })?,
            name: view.name,
            cache_tier_tables,
            write_buffer_table: view
                .write_buffer_table
                .map(Self::simple_bg_table_view_from_pb)
                .transpose()?,
        })
    }

    pub fn simple_mount_brief_to_pb(view: &SimpleMountBrief) -> SimpleMountBriefProto {
        SimpleMountBriefProto {
            mount_id: view.mount_id,
            cv_path: view.cv_path.clone(),
            namespace_id: view.namespace_id as u32,
            version: view.version,
        }
    }

    pub fn simple_mount_brief_from_pb(view: SimpleMountBriefProto) -> FsResult<SimpleMountBrief> {
        Ok(SimpleMountBrief {
            mount_id: view.mount_id,
            cv_path: view.cv_path,
            namespace_id: u16::try_from(view.namespace_id).map_err(|_| {
                Self::invalid_proto(format!("namespace_id out of range: {}", view.namespace_id))
            })?,
            version: view.version,
        })
    }

    pub fn simple_mount_view_to_pb(view: &SimpleMountView) -> SimpleMountViewProto {
        SimpleMountViewProto {
            version: view.version,
            mounts: view
                .mounts
                .iter()
                .map(Self::simple_mount_brief_to_pb)
                .collect(),
        }
    }

    pub fn simple_mount_view_from_pb(view: SimpleMountViewProto) -> FsResult<SimpleMountView> {
        let mut mounts = Vec::with_capacity(view.mounts.len());
        for mount in view.mounts {
            mounts.push(Self::simple_mount_brief_from_pb(mount)?);
        }
        Ok(SimpleMountView {
            version: view.version,
            mounts,
        })
    }

    pub fn simple_meta_route_view_to_pb(view: &SimpleMetaRouteView) -> SimpleMetaRouteViewProto {
        SimpleMetaRouteViewProto {
            mode: Self::meta_node_mode_to_pb(view.mode),
            static_route_version: view.static_route_version,
            group_view_epoch: view.group_view_epoch,
        }
    }

    pub fn simple_meta_route_view_from_pb(
        view: SimpleMetaRouteViewProto,
    ) -> FsResult<SimpleMetaRouteView> {
        Ok(SimpleMetaRouteView {
            mode: Self::meta_node_mode_from_pb(view.mode)?,
            static_route_version: view.static_route_version,
            group_view_epoch: view.group_view_epoch,
        })
    }

    pub fn simple_cluster_view_to_pb(view: &SimpleClusterView) -> SimpleClusterViewProto {
        SimpleClusterViewProto {
            cluster_id: view.cluster_id.clone(),
            namespaces: view
                .namespaces
                .iter()
                .map(Self::simple_namespace_view_to_pb)
                .collect(),
            mount: Self::simple_mount_view_to_pb(&view.mount),
            meta_route: Self::simple_meta_route_view_to_pb(&view.meta_route),
        }
    }

    pub fn simple_cluster_view_from_pb(
        view: SimpleClusterViewProto,
    ) -> FsResult<SimpleClusterView> {
        let mut namespaces = Vec::with_capacity(view.namespaces.len());
        for namespace in view.namespaces {
            namespaces.push(Self::simple_namespace_view_from_pb(namespace)?);
        }
        Ok(SimpleClusterView {
            cluster_id: view.cluster_id,
            namespaces,
            mount: Self::simple_mount_view_from_pb(view.mount)?,
            meta_route: Self::simple_meta_route_view_from_pb(view.meta_route)?,
        })
    }

    pub fn simple_meta_route_hint_to_pb(hint: &SimpleMetaRouteHint) -> SimpleMetaRouteHintProto {
        SimpleMetaRouteHintProto {
            static_route_version: hint.static_route_version,
            group_view_epoch: hint.group_view_epoch,
        }
    }

    pub fn simple_meta_route_hint_from_pb(hint: SimpleMetaRouteHintProto) -> SimpleMetaRouteHint {
        SimpleMetaRouteHint {
            static_route_version: hint.static_route_version,
            group_view_epoch: hint.group_view_epoch,
        }
    }

    pub fn simple_cluster_view_hint_to_pb(
        hint: &SimpleClusterViewHint,
    ) -> SimpleClusterViewHintProto {
        SimpleClusterViewHintProto {
            mount_version: hint.mount_version,
            bg_table_route_epochs: hint
                .bg_table_route_epochs
                .iter()
                .map(|(k, v)| (*k as u32, *v))
                .collect(),
            meta_route: Self::simple_meta_route_hint_to_pb(&hint.meta_route),
        }
    }

    pub fn simple_cluster_view_hint_from_pb(
        hint: SimpleClusterViewHintProto,
    ) -> FsResult<SimpleClusterViewHint> {
        Ok(SimpleClusterViewHint {
            mount_version: hint.mount_version,
            bg_table_route_epochs: hint
                .bg_table_route_epochs
                .into_iter()
                .map(|(k, v)| {
                    let key = u16::try_from(k).map_err(|_| {
                        Self::invalid_proto(format!("table_id out of range: {}", k))
                    })?;
                    Ok((key, v))
                })
                .collect::<FsResult<_>>()?,
            meta_route: Self::simple_meta_route_hint_from_pb(hint.meta_route),
        })
    }

    pub fn worker_heartbeat_response_to_pb(
        resp: &crate::state::WorkerHeartbeatResponse,
    ) -> WorkerHeartbeatResponseProto {
        WorkerHeartbeatResponseProto {
            add_bgs: resp
                .add_bgs
                .iter()
                .map(Self::block_group_info_to_pb)
                .collect(),
            remove_bgs: resp.remove_bgs.clone(),
            update_bgs: resp
                .update_bgs
                .iter()
                .map(Self::block_group_info_to_pb)
                .collect(),
        }
    }

    pub fn worker_heartbeat_response_from_pb(
        resp: WorkerHeartbeatResponseProto,
    ) -> FsResult<crate::state::WorkerHeartbeatResponse> {
        let mut add_bgs = Vec::with_capacity(resp.add_bgs.len());
        for bg in resp.add_bgs {
            add_bgs.push(Self::block_group_info_from_pb(bg)?);
        }
        let mut update_bgs = Vec::with_capacity(resp.update_bgs.len());
        for bg in resp.update_bgs {
            update_bgs.push(Self::block_group_info_from_pb(bg)?);
        }
        Ok(crate::state::WorkerHeartbeatResponse {
            add_bgs,
            remove_bgs: resp.remove_bgs,
            update_bgs,
        })
    }

    pub fn path_route_entry_to_pb(entry: &PathRouteEntry) -> PathRouteEntryProto {
        PathRouteEntryProto {
            path: entry.path.clone(),
            group_id: entry.group_id,
            create_time_ms: entry.create_time_ms,
            update_time_ms: entry.update_time_ms,
        }
    }

    pub fn path_route_entry_from_pb(entry: PathRouteEntryProto) -> PathRouteEntry {
        PathRouteEntry {
            path: entry.path,
            group_id: entry.group_id,
            create_time_ms: entry.create_time_ms,
            update_time_ms: entry.update_time_ms,
        }
    }

    pub fn path_route_table_to_pb(table: &PathRouteTable) -> PathRouteTableProto {
        PathRouteTableProto {
            version: table.version,
            routes: table
                .routes
                .iter()
                .map(Self::path_route_entry_to_pb)
                .collect(),
            last_update_ms: table.last_update_ms,
        }
    }

    pub fn path_route_table_from_pb(table: PathRouteTableProto) -> PathRouteTable {
        let mut out = PathRouteTable::default();
        out.version = table.version;
        out.routes = table
            .routes
            .into_iter()
            .map(Self::path_route_entry_from_pb)
            .collect();
        out.last_update_ms = table.last_update_ms;
        out
    }

    pub fn path_route_update_to_pb(update: &PathRouteUpdate) -> PathRouteUpdateProto {
        PathRouteUpdateProto {
            version: update.version,
            routes: update
                .routes
                .iter()
                .map(Self::path_route_entry_to_pb)
                .collect(),
        }
    }

    pub fn path_route_update_from_pb(update: PathRouteUpdateProto) -> FsResult<PathRouteUpdate> {
        Ok(PathRouteUpdate {
            version: update.version,
            routes: update
                .routes
                .into_iter()
                .map(Self::path_route_entry_from_pb)
                .collect(),
        })
    }

    pub fn node_group_update_to_pb(update: &NodeGroupUpdate) -> NodeGroupUpdateProto {
        match &update.action {
            NodeGroupUpdateAction::AddGroup { groups } => NodeGroupUpdateProto {
                version: update.version,
                action_type: 0,
                groups: groups.iter().map(Self::node_group_info_to_pb).collect(),
                group_ids: vec![],
            },
            NodeGroupUpdateAction::RemoveGroup { group_ids } => NodeGroupUpdateProto {
                version: update.version,
                action_type: 1,
                groups: vec![],
                group_ids: group_ids.clone(),
            },
        }
    }

    pub fn node_group_update_from_pb(update: NodeGroupUpdateProto) -> FsResult<NodeGroupUpdate> {
        let action = match update.action_type {
            0 => {
                let mut groups = Vec::with_capacity(update.groups.len());
                for group in update.groups {
                    groups.push(Self::node_group_info_from_pb(group)?);
                }
                NodeGroupUpdateAction::AddGroup { groups }
            }
            1 => NodeGroupUpdateAction::RemoveGroup {
                group_ids: update.group_ids,
            },
            v => {
                return Err(Self::invalid_proto(format!(
                    "unknown node_group_update_action={}",
                    v
                )))
            }
        };
        Ok(NodeGroupUpdate {
            version: update.version,
            action,
        })
    }

    pub fn meta_heartbeat_response_to_pb(
        resp: &MetaHeartbeatResponse,
    ) -> MetaHeartbeatResponseProto {
        MetaHeartbeatResponseProto {
            path_route_update: resp
                .path_route_update
                .as_ref()
                .map(Self::path_route_update_to_pb),
            node_group_update: resp
                .node_group_update
                .as_ref()
                .map(Self::node_group_update_to_pb),
        }
    }

    pub fn meta_heartbeat_response_from_pb(
        resp: MetaHeartbeatResponseProto,
    ) -> FsResult<MetaHeartbeatResponse> {
        Ok(MetaHeartbeatResponse {
            path_route_update: resp
                .path_route_update
                .map(Self::path_route_update_from_pb)
                .transpose()?,
            node_group_update: resp
                .node_group_update
                .map(Self::node_group_update_from_pb)
                .transpose()?,
        })
    }

    pub fn task_heartbeat_response_to_pb(
        _resp: &TaskHeartbeatResponse,
    ) -> TaskHeartbeatResponseProto {
        TaskHeartbeatResponseProto {}
    }

    pub fn task_heartbeat_response_from_pb(
        _resp: TaskHeartbeatResponseProto,
    ) -> TaskHeartbeatResponse {
        TaskHeartbeatResponse::default()
    }

    pub fn heartbeat_response_to_pb(resp: &HeartbeatResponse) -> NodeHeartbeatResponseProto {
        let (worker, meta, task) = match &resp.payload {
            HeartbeatResponsePayload::Worker(w) => {
                (Some(Self::worker_heartbeat_response_to_pb(w)), None, None)
            }
            HeartbeatResponsePayload::Meta(m) => {
                (None, Some(Self::meta_heartbeat_response_to_pb(m)), None)
            }
            HeartbeatResponsePayload::Task(t) => {
                (None, None, Some(Self::task_heartbeat_response_to_pb(t)))
            }
        };
        NodeHeartbeatResponseProto {
            error: resp.error.clone(),
            epoch: resp.epoch,
            mount_version: resp.mount_version,
            table_epochs: resp
                .table_epochs
                .iter()
                .map(|(k, v)| (*k as u32, *v))
                .collect(),
            worker,
            meta,
            task,
            simple_cluster_view_hint: Some(Self::simple_cluster_view_hint_to_pb(
                &resp.simple_cluster_view_hint,
            )),
        }
    }

    pub fn heartbeat_response_from_pb(
        resp: NodeHeartbeatResponseProto,
    ) -> FsResult<HeartbeatResponse> {
        let payload = match (resp.worker, resp.meta, resp.task) {
            (Some(worker), None, None) => {
                HeartbeatResponsePayload::Worker(Self::worker_heartbeat_response_from_pb(worker)?)
            }
            (None, Some(meta), None) => {
                HeartbeatResponsePayload::Meta(Self::meta_heartbeat_response_from_pb(meta)?)
            }
            (None, None, Some(task)) => {
                HeartbeatResponsePayload::Task(Self::task_heartbeat_response_from_pb(task))
            }
            (worker, meta, task) => {
                return Err(Self::invalid_proto(format!(
                    "heartbeat response payload mismatch worker_set={} meta_set={} task_set={}",
                    worker.is_some(),
                    meta.is_some(),
                    task.is_some()
                )));
            }
        };
        Ok(HeartbeatResponse {
            error: resp.error,
            epoch: resp.epoch,
            mount_version: resp.mount_version,
            table_epochs: resp
                .table_epochs
                .into_iter()
                .map(|(k, v)| {
                    let key = u16::try_from(k).map_err(|_| {
                        Self::invalid_proto(format!("table_id out of range: {}", k))
                    })?;
                    Ok((key, v))
                })
                .collect::<FsResult<_>>()?,
            simple_cluster_view_hint: resp
                .simple_cluster_view_hint
                .map(Self::simple_cluster_view_hint_from_pb)
                .transpose()?
                .unwrap_or_default(),
            payload,
        })
    }

    pub fn register_response_to_pb(resp: &HeartbeatResponse) -> NodeRegisterResponse {
        NodeRegisterResponse {
            response: Self::heartbeat_response_to_pb(resp),
        }
    }

    pub fn register_response_from_pb(resp: NodeRegisterResponse) -> FsResult<HeartbeatResponse> {
        Self::heartbeat_response_from_pb(resp.response)
    }

    pub fn label_match_to_pb(label: &LabelMatch) -> LabelMatchProto {
        LabelMatchProto {
            key: label.key.clone(),
            value: label.value.clone(),
        }
    }

    pub fn label_match_from_pb(label: LabelMatchProto) -> LabelMatch {
        LabelMatch {
            key: label.key,
            value: label.value,
        }
    }

    pub fn pool_type_to_pb(pool: StorageType) -> String {
        pool.as_str_name().to_string()
    }

    pub fn pool_type_from_pb(pool: &str) -> FsResult<StorageType> {
        let media = StorageType::try_from(pool).map_err(|e| FsError::common(e.to_string()))?;
        if !is_pool_storage_type(media) {
            return Err(FsError::common(format!(
                "invalid pool storage type: {}",
                pool
            )));
        }
        Ok(media)
    }

    pub fn cache_ack_policy_to_pb(policy: &CacheAckPolicy) -> (i32, Option<u32>) {
        match policy {
            CacheAckPolicy::One => (CacheAckPolicyProto::One as i32, None),
            CacheAckPolicy::Majority => (CacheAckPolicyProto::Majority as i32, None),
            CacheAckPolicy::AtLeast(n) => (CacheAckPolicyProto::AtLeast as i32, Some(*n as u32)),
        }
    }

    pub fn cache_ack_policy_from_pb(
        policy: i32,
        at_least: Option<u32>,
    ) -> FsResult<CacheAckPolicy> {
        match policy {
            x if x == CacheAckPolicyProto::One as i32 => Ok(CacheAckPolicy::One),
            x if x == CacheAckPolicyProto::Majority as i32 => Ok(CacheAckPolicy::Majority),
            x if x == CacheAckPolicyProto::AtLeast as i32 => {
                let n = at_least.ok_or_else(|| {
                    Self::invalid_proto("cache ack policy AtLeast requires at_least".to_string())
                })?;
                if n == 0 || n > u16::MAX as u32 {
                    return Err(Self::invalid_proto(format!(
                        "cache ack at_least out of range: {}",
                        n
                    )));
                }
                Ok(CacheAckPolicy::AtLeast(n as u16))
            }
            _ => Err(Self::invalid_proto(format!(
                "unknown cache_ack_policy={}",
                policy
            ))),
        }
    }

    pub fn cache_read_policy_to_pb(policy: &CacheReadPolicy) -> i32 {
        match policy {
            CacheReadPolicy::Nearest => CacheReadPolicyProto::Nearest as i32,
            CacheReadPolicy::PrimaryFirst => CacheReadPolicyProto::PrimaryFirst as i32,
            CacheReadPolicy::Random => CacheReadPolicyProto::Random as i32,
        }
    }

    pub fn cache_read_policy_from_pb(policy: i32) -> FsResult<CacheReadPolicy> {
        match policy {
            x if x == CacheReadPolicyProto::Nearest as i32 => Ok(CacheReadPolicy::Nearest),
            x if x == CacheReadPolicyProto::PrimaryFirst as i32 => {
                Ok(CacheReadPolicy::PrimaryFirst)
            }
            x if x == CacheReadPolicyProto::Random as i32 => Ok(CacheReadPolicy::Random),
            _ => Err(Self::invalid_proto(format!(
                "unknown cache_read_policy={}",
                policy
            ))),
        }
    }

    pub fn cache_replica_policy_to_pb(policy: &CacheReplicaPolicy) -> CacheReplicaPolicyProto {
        let (ack_policy, at_least) = Self::cache_ack_policy_to_pb(&policy.ack_policy);
        CacheReplicaPolicyProto {
            ack_policy,
            read_policy: Self::cache_read_policy_to_pb(&policy.read_policy),
            min_isr: policy.min_isr as u32,
            at_least,
        }
    }

    pub fn cache_replica_policy_from_pb(
        policy: CacheReplicaPolicyProto,
    ) -> FsResult<CacheReplicaPolicy> {
        if policy.min_isr == 0 || policy.min_isr > u16::MAX as u32 {
            return Err(Self::invalid_proto(format!(
                "cache min_isr out of range: {}",
                policy.min_isr
            )));
        }
        Ok(CacheReplicaPolicy {
            ack_policy: Self::cache_ack_policy_from_pb(policy.ack_policy, policy.at_least)?,
            read_policy: Self::cache_read_policy_from_pb(policy.read_policy)?,
            min_isr: policy.min_isr as u16,
        })
    }

    pub fn cache_tier_config_to_pb(config: &CacheTierConfig) -> CacheTierConfigProto {
        CacheTierConfigProto {
            pools: config
                .pools
                .iter()
                .copied()
                .map(Self::pool_type_to_pb)
                .collect(),
            replica_count: config.replica_count as u32,
            bucket_count: config.bucket_count,
            worker_labels: config
                .worker_labels
                .iter()
                .map(Self::label_match_to_pb)
                .collect(),
        }
    }

    pub fn cache_tier_config_from_pb(config: CacheTierConfigProto) -> FsResult<CacheTierConfig> {
        let mut pools = Vec::with_capacity(config.pools.len());
        for pool in config.pools {
            pools.push(Self::pool_type_from_pb(&pool)?);
        }
        Ok(CacheTierConfig {
            pools,
            replica_count: config.replica_count as u16,
            bucket_count: config.bucket_count,
            worker_labels: config
                .worker_labels
                .into_iter()
                .map(Self::label_match_from_pb)
                .collect(),
        })
    }

    pub fn write_buffer_config_to_pb(config: &WriteBufferConfig) -> WriteBufferConfigProto {
        WriteBufferConfigProto {
            pool: Self::pool_type_to_pb(config.pool),
            replica_count: config.replica_count as u32,
            capacity_bg_size: config.capacity_bg_size,
            min_active_bgs: config.min_active_bgs,
            worker_labels: config
                .worker_labels
                .iter()
                .map(Self::label_match_to_pb)
                .collect(),
        }
    }

    pub fn write_buffer_config_from_pb(
        config: WriteBufferConfigProto,
    ) -> FsResult<WriteBufferConfig> {
        Ok(WriteBufferConfig {
            pool: Self::pool_type_from_pb(&config.pool)?,
            replica_count: config.replica_count as u16,
            capacity_bg_size: config.capacity_bg_size,
            min_active_bgs: config.min_active_bgs,
            worker_labels: config
                .worker_labels
                .into_iter()
                .map(Self::label_match_from_pb)
                .collect(),
        })
    }

    pub fn namespace_info_to_pb(info: &NamespaceInfo) -> NamespaceInfoProto {
        NamespaceInfoProto {
            id: info.id as u32,
            name: info.name.clone(),
            block_size: info.block_size,
            cache_tier_tables: info.cache_tier_tables.iter().map(|id| *id as u32).collect(),
            write_buffer_table: info.write_buffer_table.map(|id| id as u32),
            cache_tier_config: Self::cache_tier_config_to_pb(&info.cache_tier_config),
            write_buffer_config: info
                .write_buffer_config
                .as_ref()
                .map(Self::write_buffer_config_to_pb),
            cache_replica_policy: Self::cache_replica_policy_to_pb(&info.cache_replica_policy),
            default_ttl_ms: info.default_ttl_ms,
            ttl_action: info.ttl_action.into(),
            version: info.version,
            create_time_ms: info.create_time_ms,
            update_time_ms: info.update_time_ms,
            properties: info.properties.clone(),
        }
    }

    pub fn namespace_info_from_pb(info: NamespaceInfoProto) -> FsResult<NamespaceInfo> {
        Ok(NamespaceInfo {
            id: info.id as NamespaceId,
            name: info.name,
            block_size: info.block_size,
            cache_tier_tables: info
                .cache_tier_tables
                .into_iter()
                .map(|id| id as TableId)
                .collect(),
            write_buffer_table: info.write_buffer_table.map(|id| id as TableId),
            cache_tier_config: Self::cache_tier_config_from_pb(info.cache_tier_config)?,
            write_buffer_config: info
                .write_buffer_config
                .map(Self::write_buffer_config_from_pb)
                .transpose()?,
            cache_replica_policy: Self::cache_replica_policy_from_pb(info.cache_replica_policy)?,
            default_ttl_ms: info.default_ttl_ms,
            ttl_action: info.ttl_action.into(),
            version: info.version,
            create_time_ms: info.create_time_ms,
            update_time_ms: info.update_time_ms,
            properties: info.properties,
        })
    }

    pub fn create_namespace_request_to_pb(
        request: &CreateNamespaceRequest,
    ) -> CreateNamespaceRequestProto {
        CreateNamespaceRequestProto {
            name: request.name.clone(),
            block_size: request.block_size,
            cache_tier_config: Self::cache_tier_config_to_pb(&request.cache_tier_config),
            write_buffer_config: request
                .write_buffer_config
                .as_ref()
                .map(Self::write_buffer_config_to_pb),
            cache_replica_policy: Self::cache_replica_policy_to_pb(&request.cache_replica_policy),
            default_ttl_ms: request.default_ttl_ms,
            ttl_action: request.ttl_action.into(),
            properties: request.properties.clone(),
        }
    }

    pub fn create_namespace_request_from_pb(
        request: CreateNamespaceRequestProto,
    ) -> FsResult<CreateNamespaceRequest> {
        Ok(CreateNamespaceRequest {
            name: request.name,
            block_size: request.block_size,
            cache_tier_config: Self::cache_tier_config_from_pb(request.cache_tier_config)?,
            write_buffer_config: request
                .write_buffer_config
                .map(Self::write_buffer_config_from_pb)
                .transpose()?,
            cache_replica_policy: Self::cache_replica_policy_from_pb(request.cache_replica_policy)?,
            default_ttl_ms: request.default_ttl_ms,
            ttl_action: request.ttl_action.into(),
            properties: request.properties,
        })
    }

    pub fn meta_route_summary_to_pb(summary: &MetaRouteSummary) -> MetaRouteSummaryProto {
        MetaRouteSummaryProto {
            mode: Self::meta_node_mode_to_pb(summary.mode),
            version: summary.version,
            federation_route_config: Some(Self::federation_route_config_to_pb(
                &summary.federation_route_config,
            )),
            path_table: summary
                .path_table
                .as_ref()
                .map(Self::path_route_table_to_pb),
            meta_groups: summary
                .meta_groups
                .iter()
                .map(|(k, v)| (*k, Self::node_group_info_to_pb(v)))
                .collect(),
            group_id_order: summary.group_id_order.clone(),
        }
    }

    pub fn meta_route_summary_from_pb(
        summary: MetaRouteSummaryProto,
    ) -> FsResult<MetaRouteSummary> {
        let mut meta_groups = std::collections::HashMap::new();
        for (k, v) in summary.meta_groups {
            meta_groups.insert(k, Self::node_group_info_from_pb(v)?);
        }
        Ok(MetaRouteSummary {
            mode: Self::meta_node_mode_from_pb(summary.mode)?,
            version: summary.version,
            federation_route_config: summary
                .federation_route_config
                .map(Self::federation_route_config_from_pb)
                .unwrap_or_default(),
            path_table: summary.path_table.map(Self::path_route_table_from_pb),
            meta_groups,
            group_id_order: summary.group_id_order,
        })
    }
}

#[cfg(test)]
mod pd_proto_utils_tests {
    use super::ProtoUtils;
    use crate::proto::{MetaHeartbeatResponseProto, MetaRegisterPayloadProto};
    use crate::state::*;
    use std::collections::HashMap;

    fn node_addr(id: u32) -> NodeAddress {
        NodeAddress {
            hostname: format!("host-{}", id),
            ip: format!("10.0.0.{}", id),
            rpc_port: 8000 + id as u16,
            web_port: 9000 + id as u16,
        }
    }

    fn worker_register_request() -> RegisterRequest {
        let mut specs = HashMap::new();
        specs.insert(
            "s1".to_string(),
            StorageSpec {
                dir_id: 1,
                storage_id: "s1".to_string(),
                failed: false,
                storage_type: StorageType::Disk,
                dir_path: "/data1".to_string(),
            },
        );
        RegisterRequest {
            cluster_id: "curvine".to_string(),
            base: NodeBase {
                node_id: 10,
                node_type: NodeType::Worker,
                address: node_addr(10),
                labels: HashMap::from([("rack".to_string(), "r1".to_string())]),
                software_version: "test".to_string(),
                startup_time_ms: 123,
            },
            payload: NodePayload::Worker(WorkerNodePayload {
                storage_specs: specs,
                ..Default::default()
            }),
        }
    }

    fn worker_heartbeat_request() -> HeartbeatRequest {
        let mut storage_stats = HashMap::new();
        storage_stats.insert(
            "s1".to_string(),
            StorageStats {
                capacity: 100,
                fs_used: 10,
                non_fs_used: 2,
                available: 88,
                reserved_bytes: 1,
                block_num: 3,
                dir_path: "/data1".to_string(),
            },
        );
        HeartbeatRequest {
            cluster_id: "curvine".to_string(),
            node_id: 10,
            node_type: NodeType::Worker,
            epoch: 2,
            timestamp_ms: 456,
            address: node_addr(10),
            payload: HeartbeatPayload::Worker(WorkerHeartbeatPayload {
                storage_stats,
                sys_stats: SystemStats {
                    cpu_usage: 0.5,
                    memory_usage: 0.6,
                },
                bg_reports: vec![WorkerBGReport {
                    kind: BGKind::Hash,
                    bg_id: 7,
                    bg_epoch: 11,
                    state: ReplicaState::Active,
                    stats: BGStats {
                        used_bytes: 1,
                        free_bytes: 2,
                        block_count: 3,
                        last_report_ms: 4,
                    },
                    ..Default::default()
                }],
            }),
        }
    }
    fn task_register_request() -> RegisterRequest {
        RegisterRequest {
            cluster_id: "curvine".to_string(),
            base: NodeBase {
                node_id: 30,
                node_type: NodeType::Task,
                address: node_addr(30),
                labels: HashMap::from([("rack".to_string(), "r2".to_string())]),
                software_version: "test".to_string(),
                startup_time_ms: 789,
            },
            payload: NodePayload::Task(TaskNodePayload::default()),
        }
    }

    fn task_heartbeat_request() -> HeartbeatRequest {
        HeartbeatRequest {
            cluster_id: "curvine".to_string(),
            node_id: 30,
            node_type: NodeType::Task,
            epoch: 4,
            timestamp_ms: 999,
            address: node_addr(30),
            payload: HeartbeatPayload::Task(TaskHeartbeatPayload {
                sys_stats: SystemStats {
                    cpu_usage: 0.7,
                    memory_usage: 0.8,
                },
                stats: TaskNodeStats {
                    running_tasks: 2,
                    failed_tasks: 3,
                },
            }),
        }
    }

    fn meta_register_payload_pb() -> MetaRegisterPayloadProto {
        MetaRegisterPayloadProto {
            group_id: 1,
            peers: vec![],
            rw_policy: ProtoUtils::rw_policy_to_pb(RwPolicy::LeaderOnly),
            group_epoch: 1,
        }
    }

    #[test]
    fn register_request_worker_round_trip() {
        let req = worker_register_request();
        let decoded =
            ProtoUtils::register_request_from_pb(ProtoUtils::register_request_to_pb(&req))
                .expect("round trip register");
        assert_eq!(decoded.cluster_id, req.cluster_id);
        assert_eq!(decoded.base.node_id, req.base.node_id);
        assert_eq!(decoded.base.node_type, NodeType::Worker);
        match decoded.payload {
            NodePayload::Worker(w) => assert!(w.storage_specs.contains_key("s1")),
            _ => panic!("expected worker payload"),
        }
    }

    #[test]
    fn heartbeat_request_worker_round_trip() {
        let req = worker_heartbeat_request();
        let decoded =
            ProtoUtils::heartbeat_request_from_pb(ProtoUtils::heartbeat_request_to_pb(&req))
                .expect("round trip heartbeat");
        assert_eq!(decoded.node_id, req.node_id);
        assert_eq!(decoded.node_type, NodeType::Worker);
        assert_eq!(decoded.epoch, req.epoch);
        match decoded.payload {
            HeartbeatPayload::Worker(w) => {
                assert_eq!(w.bg_reports[0].kind, BGKind::Hash);
                assert_eq!(w.bg_reports[0].bg_epoch, 11);
                assert_eq!(w.bg_reports[0].state, ReplicaState::Active);
            }
            _ => panic!("expected worker heartbeat payload"),
        }
    }

    #[test]
    fn register_request_task_round_trip() {
        let req = task_register_request();
        let decoded =
            ProtoUtils::register_request_from_pb(ProtoUtils::register_request_to_pb(&req))
                .expect("round trip task register");
        assert_eq!(decoded.cluster_id, req.cluster_id);
        assert_eq!(decoded.base.node_id, req.base.node_id);
        assert_eq!(decoded.base.node_type, NodeType::Task);
        assert!(matches!(decoded.payload, NodePayload::Task(_)));
    }

    #[test]
    fn heartbeat_request_task_round_trip() {
        let req = task_heartbeat_request();
        let decoded =
            ProtoUtils::heartbeat_request_from_pb(ProtoUtils::heartbeat_request_to_pb(&req))
                .expect("round trip task heartbeat");
        assert_eq!(decoded.node_id, req.node_id);
        assert_eq!(decoded.node_type, NodeType::Task);
        assert_eq!(decoded.epoch, req.epoch);
        match decoded.payload {
            HeartbeatPayload::Task(t) => {
                assert_eq!(t.stats.running_tasks, 2);
                assert_eq!(t.stats.failed_tasks, 3);
                assert_eq!(t.sys_stats.cpu_usage, 0.7);
            }
            _ => panic!("expected task heartbeat payload"),
        }
    }

    #[test]
    fn simple_cluster_view_round_trip() {
        let view = SimpleClusterView {
            cluster_id: "cluster-a".to_string(),
            namespaces: vec![SimpleNamespaceView {
                namespace_id: 1,
                name: "default".to_string(),
                cache_tier_tables: vec![SimpleBGTableView {
                    table_id: 0x11,
                    kind: BGKind::Hash,
                    storage_type: StorageType::Ssd,
                    replica_count: 3,
                    route_epoch: 7,
                }],
                write_buffer_table: None,
            }],
            mount: SimpleMountView {
                version: 9,
                mounts: vec![SimpleMountBrief {
                    mount_id: 1,
                    cv_path: "/".to_string(),
                    namespace_id: 1,
                    version: 2,
                }],
            },
            meta_route: SimpleMetaRouteView {
                mode: MetaNodeMode::Federation,
                static_route_version: 3,
                group_view_epoch: 4,
            },
        };

        let decoded =
            ProtoUtils::simple_cluster_view_from_pb(ProtoUtils::simple_cluster_view_to_pb(&view))
                .expect("simple cluster view round trip");
        assert_eq!(decoded, view);
    }

    #[test]
    fn heartbeat_response_task_round_trip() {
        let resp = HeartbeatResponse {
            error: None,
            epoch: 5,
            mount_version: 6,
            table_epochs: HashMap::from([(2, 10)]),
            simple_cluster_view_hint: Default::default(),
            payload: HeartbeatResponsePayload::Task(TaskHeartbeatResponse::default()),
        };
        let decoded =
            ProtoUtils::heartbeat_response_from_pb(ProtoUtils::heartbeat_response_to_pb(&resp))
                .expect("round trip task heartbeat response");
        assert_eq!(decoded.epoch, resp.epoch);
        assert_eq!(decoded.mount_version, resp.mount_version);
        assert_eq!(decoded.table_epochs.get(&2), Some(&10));
        assert!(matches!(decoded.payload, HeartbeatResponsePayload::Task(_)));
    }

    #[test]
    fn register_response_round_trip() {
        let resp = HeartbeatResponse {
            error: Some("soft".to_string()),
            epoch: 3,
            mount_version: 4,
            table_epochs: HashMap::from([(1, 9)]),
            simple_cluster_view_hint: Default::default(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: vec![BlockGroupInfo {
                    bg_id: 7,
                    table_id: 1,
                    kind: crate::state::BGKind::Hash,
                    bg_epoch: 9,
                    replica_set: vec![10],
                    isr: vec![10],
                    state: BGState::Active,
                    op_state: BGOpState::Idle,
                    primary: BGPrimary {
                        node_id: 10,
                        epoch: 1,
                        grant_time_ms: 0,
                    },
                    stats: Default::default(),
                    replicas: Default::default(),
                }],
                remove_bgs: vec![8],
                update_bgs: vec![],
            }),
        };
        let decoded =
            ProtoUtils::register_response_from_pb(ProtoUtils::register_response_to_pb(&resp))
                .expect("round trip register response");
        assert_eq!(decoded.error, resp.error);
        assert_eq!(decoded.table_epochs.get(&1), Some(&9));
        match decoded.payload {
            HeartbeatResponsePayload::Worker(w) => {
                assert_eq!(w.add_bgs[0].bg_id, 7);
                assert_eq!(w.remove_bgs, vec![8]);
            }
            _ => panic!("expected worker response"),
        }
    }

    #[test]
    fn namespace_proto_round_trip() {
        let mut properties = HashMap::new();
        properties.insert("owner".to_string(), "test".to_string());
        let req = CreateNamespaceRequest {
            name: "ns1".to_string(),
            block_size: 4 * 1024 * 1024,
            cache_tier_config: CacheTierConfig {
                pools: vec![StorageType::Ssd, StorageType::Hdd],
                replica_count: 2,
                bucket_count: 64,
                worker_labels: vec![LabelMatch {
                    key: "rack".to_string(),
                    value: "r1".to_string(),
                }],
            },
            write_buffer_config: None,
            cache_replica_policy: CacheReplicaPolicy::default(),
            default_ttl_ms: Some(3600),
            ttl_action: TtlAction::Delete,
            properties: properties.clone(),
        };

        let decoded_req = ProtoUtils::create_namespace_request_from_pb(
            ProtoUtils::create_namespace_request_to_pb(&req),
        )
        .expect("round trip create namespace request");
        assert_eq!(decoded_req, req);

        let info = NamespaceInfo {
            id: 1,
            name: req.name.clone(),
            block_size: req.block_size,
            cache_tier_tables: vec![16, 17],
            write_buffer_table: None,
            cache_tier_config: req.cache_tier_config.clone(),
            write_buffer_config: None,
            cache_replica_policy: CacheReplicaPolicy::default(),
            default_ttl_ms: req.default_ttl_ms,
            ttl_action: req.ttl_action,
            version: 1,
            create_time_ms: 100,
            update_time_ms: 200,
            properties,
        };
        let decoded_info =
            ProtoUtils::namespace_info_from_pb(ProtoUtils::namespace_info_to_pb(&info))
                .expect("round trip namespace info");
        assert_eq!(decoded_info, info);
    }

    #[test]
    fn meta_route_summary_round_trip() {
        let summary = MetaRouteSummary {
            mode: MetaNodeMode::Federation,
            version: 10,
            federation_route_config: FederationRouteConfig::new(Some(2)),
            path_table: None,
            meta_groups: HashMap::from([(
                1,
                NodeGroupInfo {
                    group_id: 1,
                    peers: vec![PeerInfo {
                        node_id: 20,
                        address: node_addr(20),
                        is_leader: Some(true),
                    }],
                },
            )]),
            group_id_order: vec![1],
        };
        let decoded =
            ProtoUtils::meta_route_summary_from_pb(ProtoUtils::meta_route_summary_to_pb(&summary))
                .expect("round trip meta route summary");
        assert_eq!(decoded.mode, MetaNodeMode::Federation);
        assert_eq!(decoded.version, 10);
        assert_eq!(decoded.federation_route_config.hash_level, Some(2));
        assert!(decoded.meta_groups.contains_key(&1));
    }

    #[test]
    fn register_request_rejects_both_payloads() {
        let mut pb = ProtoUtils::register_request_to_pb(&worker_register_request());
        pb.meta = Some(meta_register_payload_pb());
        assert!(ProtoUtils::register_request_from_pb(pb).is_err());
    }

    #[test]
    fn register_request_rejects_missing_payload() {
        let mut pb = ProtoUtils::register_request_to_pb(&worker_register_request());
        pb.worker = None;
        assert!(ProtoUtils::register_request_from_pb(pb).is_err());
    }

    #[test]
    fn heartbeat_request_rejects_zero_node_id() {
        let mut pb = ProtoUtils::heartbeat_request_to_pb(&worker_heartbeat_request());
        pb.node_id = 0;
        assert!(ProtoUtils::heartbeat_request_from_pb(pb).is_err());
    }

    #[test]
    fn heartbeat_request_rejects_invalid_node_type() {
        let mut pb = ProtoUtils::heartbeat_request_to_pb(&worker_heartbeat_request());
        pb.node_type = 99;
        assert!(ProtoUtils::heartbeat_request_from_pb(pb).is_err());
    }

    #[test]
    fn heartbeat_response_rejects_both_payloads() {
        let resp = HeartbeatResponse {
            error: None,
            epoch: 1,
            mount_version: 1,
            table_epochs: HashMap::new(),
            simple_cluster_view_hint: Default::default(),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse::default()),
        };
        let mut pb = ProtoUtils::heartbeat_response_to_pb(&resp);
        pb.meta = Some(MetaHeartbeatResponseProto {
            path_route_update: None,
            node_group_update: None,
        });
        assert!(ProtoUtils::heartbeat_response_from_pb(pb).is_err());
    }
}
