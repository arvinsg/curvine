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
            properties: info.properties,
            ttl_ms: info.ttl_ms,
            ttl_action: info.ttl_action.into(),
            consistency_strategy: info.consistency_strategy.into(),
            storage_type: info.storage_type.map(|v| v.into()),
            block_size: info.block_size,
            replicas: info.replicas,
            mount_type: info.mount_type.into(),
            write_type: info.write_type.into(),
            provider: info.provider.map(|v| v.into()),
        }
    }

    pub fn mount_info_from_pb(info: MountInfoProto) -> MountInfo {
        MountInfo {
            cv_path: info.cv_path,
            ufs_path: info.ufs_path,
            mount_id: info.mount_id,
            properties: info.properties,
            ttl_ms: info.ttl_ms,
            ttl_action: info.ttl_action.into(),
            consistency_strategy: info.consistency_strategy.into(),
            storage_type: info.storage_type.map(|x| x.into()),
            block_size: info.block_size,
            replicas: info.replicas,
            mount_type: info.mount_type.into(),
            write_type: WriteType::from(info.write_type),
            provider: info.provider.map(|x| x.into()),
        }
    }

    pub fn mount_options_to_pb(opts: MountOptions) -> MountOptionsProto {
        MountOptionsProto {
            update: opts.update,
            add_properties: opts.add_properties,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(|v| v.into()),
            consistency_strategy: opts.consistency_strategy.map(|v| v.into()),
            storage_type: opts.storage_type.map(|v| v.into()),
            block_size: opts.block_size,
            replicas: opts.replicas,
            mount_type: opts.mount_type.into(),
            remove_properties: opts.remove_properties,
            write_type: opts.write_type.into(),
            provider: opts.provider.map(|v| v.into()),
        }
    }

    pub fn mount_options_from_pb(opts: MountOptionsProto) -> MountOptions {
        MountOptions {
            update: opts.update,
            add_properties: opts.add_properties,
            ttl_ms: opts.ttl_ms,
            ttl_action: opts.ttl_action.map(TtlAction::from),
            consistency_strategy: opts.consistency_strategy.map(ConsistencyStrategy::from),
            storage_type: opts.storage_type.map(StorageType::from),
            block_size: opts.block_size,
            replicas: opts.replicas,
            mount_type: MountType::from(opts.mount_type),
            remove_properties: opts.remove_properties,
            write_type: opts.write_type.into(),
            provider: opts.provider.map(Provider::from),
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
        }
    }

    pub fn node_type_from_pb(v: i32) -> FsResult<NodeType> {
        match v {
            0 => Ok(NodeType::Worker),
            1 => Ok(NodeType::Meta),
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
            ReplicaState::Syncing => 1,
            ReplicaState::Active => 2,
            ReplicaState::Lost => 3,
            ReplicaState::Offline => 4,
        }
    }

    pub fn replica_state_from_pb(v: i32) -> FsResult<ReplicaState> {
        match v {
            0 => Ok(ReplicaState::Pending),
            1 => Ok(ReplicaState::Syncing),
            2 => Ok(ReplicaState::Active),
            3 => Ok(ReplicaState::Lost),
            4 => Ok(ReplicaState::Offline),
            _ => Err(Self::invalid_proto(format!("unknown replica_state={}", v))),
        }
    }

    pub fn bg_state_to_pb(s: BGState) -> i32 {
        match s {
            BGState::Init => 0,
            BGState::Assigned => 1,
            BGState::Active => 2,
            BGState::Degraded => 3,
            BGState::Recovering => 4,
            BGState::Rebalancing => 5,
            BGState::Deleting => 6,
        }
    }

    pub fn bg_state_from_pb(v: i32) -> FsResult<BGState> {
        match v {
            0 => Ok(BGState::Init),
            1 => Ok(BGState::Assigned),
            2 => Ok(BGState::Active),
            3 => Ok(BGState::Degraded),
            4 => Ok(BGState::Recovering),
            5 => Ok(BGState::Rebalancing),
            6 => Ok(BGState::Deleting),
            _ => Err(Self::invalid_proto(format!("unknown bg_state={}", v))),
        }
    }

    pub fn bg_op_state_to_pb(s: BGOpState) -> i32 {
        match s {
            BGOpState::Idle => 0,
            BGOpState::Recovering => 1,
            BGOpState::Rebalancing => 2,
            BGOpState::LeaseBalancing => 3,
            BGOpState::Deleting => 4,
        }
    }

    pub fn bg_op_state_from_pb(v: i32) -> FsResult<BGOpState> {
        match v {
            0 => Ok(BGOpState::Idle),
            1 => Ok(BGOpState::Recovering),
            2 => Ok(BGOpState::Rebalancing),
            3 => Ok(BGOpState::LeaseBalancing),
            4 => Ok(BGOpState::Deleting),
            _ => Err(Self::invalid_proto(format!("unknown bg_op_state={}", v))),
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

    pub fn federation_route_mode_to_pb(m: FederationRouteMode) -> i32 {
        match m {
            FederationRouteMode::Static => 0,
            FederationRouteMode::Hash => 1,
        }
    }

    pub fn federation_route_mode_from_pb(v: i32) -> FsResult<FederationRouteMode> {
        match v {
            0 => Ok(FederationRouteMode::Static),
            1 => Ok(FederationRouteMode::Hash),
            _ => Err(Self::invalid_proto(format!(
                "unknown federation_route_mode={}",
                v
            ))),
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
            bg_id: report.bg_id,
            state: Self::replica_state_to_pb(report.state),
            stats: Some(Self::bg_stats_to_pb(&report.stats)),
        }
    }

    pub fn worker_bg_report_from_pb(report: WorkerBgReportProto) -> FsResult<WorkerBGReport> {
        Ok(WorkerBGReport {
            bg_id: report.bg_id,
            state: Self::replica_state_from_pb(report.state)?,
            stats: report.stats.map(Self::bg_stats_from_pb).unwrap_or_default(),
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
            bg_epochs: Default::default(),
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
            bg_epochs: payload.bg_epochs.clone(),
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
            bg_epochs: payload.bg_epochs,
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

    pub fn register_request_to_pb(req: &RegisterRequest) -> NodeRegisterRequest {
        let (worker, meta) = match &req.payload {
            NodePayload::Worker(p) => (Some(Self::worker_register_payload_to_pb(p)), None),
            NodePayload::Meta(p) => (None, Some(Self::meta_register_payload_to_pb(p))),
        };
        NodeRegisterRequest {
            cluster_id: req.cluster_id.clone(),
            base: Self::node_base_to_pb(&req.base),
            worker,
            meta,
        }
    }

    pub fn register_request_from_pb(req: NodeRegisterRequest) -> FsResult<RegisterRequest> {
        let base = Self::node_base_from_pb(req.base)?;
        let payload = match (base.node_type, req.worker, req.meta) {
            (NodeType::Worker, Some(worker), None) => {
                NodePayload::Worker(Self::worker_register_payload_from_pb(worker))
            }
            (NodeType::Meta, None, Some(meta)) => {
                NodePayload::Meta(Self::meta_register_payload_from_pb(meta)?)
            }
            (node_type, worker, meta) => {
                return Err(Self::invalid_proto(format!(
                    "register payload mismatch node_type={:?}, worker_set={}, meta_set={}",
                    node_type,
                    worker.is_some(),
                    meta.is_some()
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
        let (worker, meta) = match &req.payload {
            HeartbeatPayload::Worker(p) => (Some(Self::worker_heartbeat_payload_to_pb(p)), None),
            HeartbeatPayload::Meta(p) => (None, Some(Self::meta_heartbeat_payload_to_pb(p))),
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
        }
    }

    pub fn heartbeat_request_from_pb(req: NodeHeartbeatRequest) -> FsResult<HeartbeatRequest> {
        if req.node_id == 0 {
            return Err(Self::invalid_proto("node_id must not be 0"));
        }
        let node_type = Self::node_type_from_pb(req.node_type)?;
        let payload = match (node_type, req.worker, req.meta) {
            (NodeType::Worker, Some(worker), None) => {
                HeartbeatPayload::Worker(Self::worker_heartbeat_payload_from_pb(worker)?)
            }
            (NodeType::Meta, None, Some(meta)) => {
                HeartbeatPayload::Meta(Self::meta_heartbeat_payload_from_pb(meta)?)
            }
            (node_type, worker, meta) => {
                return Err(Self::invalid_proto(format!(
                    "heartbeat payload mismatch node_type={:?}, worker_set={}, meta_set={}",
                    node_type,
                    worker.is_some(),
                    meta.is_some()
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

    pub fn bg_lease_to_pb(lease: &BGLease) -> BgLeaseProto {
        BgLeaseProto {
            node_id: lease.node_id,
            epoch: lease.epoch,
            grant_time_ms: lease.grant_time_ms,
        }
    }

    pub fn bg_lease_from_pb(lease: BgLeaseProto) -> BGLease {
        BGLease {
            node_id: lease.node_id,
            epoch: lease.epoch,
            grant_time_ms: lease.grant_time_ms,
        }
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
            table_id: bg.table_id,
            bg_epoch: bg.bg_epoch,
            replica_set: bg.replica_set.clone(),
            state: Self::bg_state_to_pb(bg.state),
            op_state: Self::bg_op_state_to_pb(bg.op_state),
            lease_owner: bg.lease_owner.as_ref().map(Self::bg_lease_to_pb),
        }
    }

    pub fn block_group_info_from_pb(bg: BlockGroupInfoProto) -> FsResult<BlockGroupInfo> {
        Ok(BlockGroupInfo {
            bg_id: bg.bg_id,
            table_id: bg.table_id,
            bg_epoch: bg.bg_epoch,
            replica_set: bg.replica_set,
            state: Self::bg_state_from_pb(bg.state)?,
            op_state: Self::bg_op_state_from_pb(bg.op_state)?,
            lease_owner: bg.lease_owner.map(Self::bg_lease_from_pb),
            stats: Default::default(),
        })
    }

    pub fn block_group_info_view_to_pb(bg: &BlockGroupInfoView) -> BlockGroupInfoViewProto {
        BlockGroupInfoViewProto {
            bg_id: bg.bg_id,
            table_id: bg.table_id,
            bg_epoch: bg.bg_epoch,
            replica_set: bg
                .replica_set
                .iter()
                .map(Self::replica_info_to_pb)
                .collect(),
            state: Self::bg_state_to_pb(bg.state),
            op_state: Self::bg_op_state_to_pb(bg.op_state),
            lease_owner: bg.lease_owner.as_ref().map(Self::bg_lease_to_pb),
        }
    }

    pub fn block_group_info_view_from_pb(
        bg: BlockGroupInfoViewProto,
    ) -> FsResult<BlockGroupInfoView> {
        let mut replicas = Vec::with_capacity(bg.replica_set.len());
        for replica in bg.replica_set {
            replicas.push(Self::replica_info_from_pb(replica)?);
        }
        Ok(BlockGroupInfoView {
            bg_id: bg.bg_id,
            table_id: bg.table_id,
            bg_epoch: bg.bg_epoch,
            replica_set: replicas,
            state: Self::bg_state_from_pb(bg.state)?,
            op_state: Self::bg_op_state_from_pb(bg.op_state)?,
            lease_owner: bg.lease_owner.map(Self::bg_lease_from_pb),
        })
    }

    pub fn bg_table_summary_to_pb(summary: &BGTableSummary) -> BgTableSummaryProto {
        BgTableSummaryProto {
            table_id: summary.table_id,
            bucket_count: summary.bucket_count,
            epoch: summary.epoch,
            last_rebuild_ms: summary.last_rebuild_ms,
            buckets: summary
                .buckets
                .iter()
                .map(Self::block_group_info_view_to_pb)
                .collect(),
        }
    }

    pub fn bg_table_summary_from_pb(summary: BgTableSummaryProto) -> FsResult<BGTableSummary> {
        let mut buckets = Vec::with_capacity(summary.buckets.len());
        for bucket in summary.buckets {
            buckets.push(Self::block_group_info_view_from_pb(bucket)?);
        }
        Ok(BGTableSummary {
            table_id: summary.table_id,
            bucket_count: summary.bucket_count,
            epoch: summary.epoch,
            last_rebuild_ms: summary.last_rebuild_ms,
            buckets,
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
            expected_table_version: 0,
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
        match &update.action {
            RouteUpdateAction::FullSync { routes } => PathRouteUpdateProto {
                version: update.version,
                action_type: 0,
                routes: routes.iter().map(Self::path_route_entry_to_pb).collect(),
                added: vec![],
                removed: vec![],
            },
            RouteUpdateAction::Incremental { added, removed } => PathRouteUpdateProto {
                version: update.version,
                action_type: 1,
                routes: vec![],
                added: added.iter().map(Self::path_route_entry_to_pb).collect(),
                removed: removed.clone(),
            },
        }
    }

    pub fn path_route_update_from_pb(update: PathRouteUpdateProto) -> FsResult<PathRouteUpdate> {
        let action = match update.action_type {
            0 => RouteUpdateAction::FullSync {
                routes: update
                    .routes
                    .into_iter()
                    .map(Self::path_route_entry_from_pb)
                    .collect(),
            },
            1 => RouteUpdateAction::Incremental {
                added: update
                    .added
                    .into_iter()
                    .map(Self::path_route_entry_from_pb)
                    .collect(),
                removed: update.removed,
            },
            v => {
                return Err(Self::invalid_proto(format!(
                    "unknown route_update_action={}",
                    v
                )))
            }
        };
        Ok(PathRouteUpdate {
            version: update.version,
            action,
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

    pub fn heartbeat_response_to_pb(resp: &HeartbeatResponse) -> NodeHeartbeatResponseProto {
        let (worker, meta) = match &resp.payload {
            HeartbeatResponsePayload::Worker(w) => {
                (Some(Self::worker_heartbeat_response_to_pb(w)), None)
            }
            HeartbeatResponsePayload::Meta(m) => {
                (None, Some(Self::meta_heartbeat_response_to_pb(m)))
            }
        };
        NodeHeartbeatResponseProto {
            error: resp.error.clone(),
            epoch: resp.epoch,
            mount_version: resp.mount_version,
            table_epochs: resp.table_epochs.clone(),
            worker,
            meta,
        }
    }

    pub fn heartbeat_response_from_pb(
        resp: NodeHeartbeatResponseProto,
    ) -> FsResult<HeartbeatResponse> {
        let payload = match (resp.worker, resp.meta) {
            (Some(worker), None) => {
                HeartbeatResponsePayload::Worker(Self::worker_heartbeat_response_from_pb(worker)?)
            }
            (None, Some(meta)) => {
                HeartbeatResponsePayload::Meta(Self::meta_heartbeat_response_from_pb(meta)?)
            }
            (worker, meta) => {
                return Err(Self::invalid_proto(format!(
                    "heartbeat response payload mismatch worker_set={} meta_set={}",
                    worker.is_some(),
                    meta.is_some()
                )));
            }
        };
        Ok(HeartbeatResponse {
            error: resp.error,
            epoch: resp.epoch,
            mount_version: resp.mount_version,
            table_epochs: resp.table_epochs,
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

    pub fn meta_route_summary_to_pb(summary: &MetaRouteSummary) -> MetaRouteSummaryProto {
        MetaRouteSummaryProto {
            mode: Self::meta_node_mode_to_pb(summary.mode),
            version: summary.version,
            federation_route_mode: summary
                .federation_route_mode
                .map(Self::federation_route_mode_to_pb),
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
            federation_hash_level: summary.federation_hash_level.map(|v| v as u32),
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
            federation_route_mode: summary
                .federation_route_mode
                .map(Self::federation_route_mode_from_pb)
                .transpose()?,
            path_table: summary.path_table.map(Self::path_route_table_from_pb),
            meta_groups,
            group_id_order: summary.group_id_order,
            federation_hash_level: summary.federation_hash_level.map(|v| v as u8),
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
                bg_epochs: HashMap::from([(7, 11)]),
                bg_reports: vec![WorkerBGReport {
                    bg_id: 7,
                    state: ReplicaState::Active,
                    stats: BGStats {
                        used_bytes: 1,
                        free_bytes: 2,
                        block_count: 3,
                        last_report_ms: 4,
                    },
                }],
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
                assert_eq!(w.bg_epochs.get(&7), Some(&11));
                assert_eq!(w.bg_reports[0].state, ReplicaState::Active);
            }
            _ => panic!("expected worker heartbeat payload"),
        }
    }

    #[test]
    fn register_response_round_trip() {
        let resp = HeartbeatResponse {
            error: Some("soft".to_string()),
            epoch: 3,
            mount_version: 4,
            table_epochs: HashMap::from([(1, 9)]),
            payload: HeartbeatResponsePayload::Worker(WorkerHeartbeatResponse {
                add_bgs: vec![BlockGroupInfo {
                    bg_id: 7,
                    table_id: 1,
                    bg_epoch: 9,
                    replica_set: vec![10],
                    state: BGState::Active,
                    op_state: BGOpState::Idle,
                    lease_owner: None,
                    stats: Default::default(),
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
    fn meta_route_summary_round_trip() {
        let summary = MetaRouteSummary {
            mode: MetaNodeMode::Federation,
            version: 10,
            federation_route_mode: Some(FederationRouteMode::Hash),
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
            federation_hash_level: Some(2),
        };
        let decoded =
            ProtoUtils::meta_route_summary_from_pb(ProtoUtils::meta_route_summary_to_pb(&summary))
                .expect("round trip meta route summary");
        assert_eq!(decoded.mode, MetaNodeMode::Federation);
        assert_eq!(decoded.version, 10);
        assert_eq!(decoded.federation_hash_level, Some(2));
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
