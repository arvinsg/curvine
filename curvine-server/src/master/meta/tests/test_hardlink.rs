#[cfg(test)]
mod tests {
    use crate::master::meta::inode::InodePath;
    use crate::master::meta::FsDir;
    use crate::master::journal::JournalWriter;
    use crate::master::Master;
    use curvine_common::conf::{ClusterConf, JournalConf};
    use curvine_common::state::CreateFileOpts;
    use curvine_common::raft::RaftClient;
    use std::sync::Arc;
    use crate::master::meta::inode::ttl::ttl_bucket::TtlBucketList;
    use orpc::runtime::Runtime;

    fn setup_test_env() -> FsDir {
        // 初始化测试 metrics
        Master::init_test_metrics();
        
        let conf = ClusterConf::format();
        let journal_conf = JournalConf::default();
        let rt = Arc::new(Runtime::new("test", 1, 1));
        let raft_client = RaftClient::from_conf(rt, &journal_conf);
        let journal_writer = JournalWriter::new(true, raft_client, &journal_conf);
        let ttl_bucket_list = Arc::new(TtlBucketList::new(60000)); // 60 seconds interval
        
        FsDir::new(&conf, journal_writer, ttl_bucket_list).unwrap()
    }

    #[test]
    fn test_hardlink_basic() {
        let mut fs_dir = setup_test_env();
        
        // 创建原始文件
        let original_path = InodePath::resolve(fs_dir.root_ptr(), "/test.txt").unwrap();
        let opts = CreateFileOpts::with_create(false);
        let created_path = fs_dir.create_file(original_path, opts).unwrap();
        
        // 获取原始文件的 inode
        let original_inode = created_path.get_last_inode().unwrap();
        let original_id = original_inode.id();
        let original_nlink = original_inode.as_file_ref().unwrap().nlink;
        assert_eq!(original_nlink, 1, "新创建的文件应该有 1 个链接");
        
        // 创建硬链接
        let hardlink_path = InodePath::resolve(fs_dir.root_ptr(), "/hardlink.txt").unwrap();
        let hardlink_path = fs_dir.hardlink(&created_path, hardlink_path).unwrap();
        
        // 验证硬链接指向同一个 inode
        let hardlink_inode = hardlink_path.get_last_inode().unwrap();
        let hardlink_id = hardlink_inode.id();
        assert_eq!(original_id, hardlink_id, "硬链接应该指向同一个 inode");
        
        // 验证 nlink 计数增加
        let updated_nlink = original_inode.as_file_ref().unwrap().nlink;
        assert_eq!(updated_nlink, 2, "创建硬链接后 nlink 应该是 2");
        
        println!("硬链接测试通过！原始文件 ID: {}, 硬链接 ID: {}, nlink: {}", original_id, hardlink_id, updated_nlink);
    }

    #[test]
    fn test_hardlink_delete() {
        let mut fs_dir = setup_test_env();
        
        // 创建原始文件
        let original_path = InodePath::resolve(fs_dir.root_ptr(), "/test.txt").unwrap();
        let opts = CreateFileOpts::with_create(false);
        let created_path = fs_dir.create_file(original_path, opts).unwrap();
        
        // 创建硬链接
        let hardlink_path = InodePath::resolve(fs_dir.root_ptr(), "/hardlink.txt").unwrap();
        let hardlink_path = fs_dir.hardlink(&created_path, hardlink_path).unwrap();
        
        // 检查创建硬链接后的 nlink 计数
        let original_nlink_after_hardlink = created_path.get_last_inode().unwrap().as_file_ref().unwrap().nlink;
        println!("创建硬链接后，原始文件的 nlink 计数: {}", original_nlink_after_hardlink);
        
        // 删除原始文件
        let del_result = fs_dir.delete(&created_path, false).unwrap();
        assert_eq!(del_result.inodes, 0, "删除硬链接时不应该删除 inode");
        
        // 验证硬链接仍然存在
        let hardlink_inode = hardlink_path.get_last_inode();
        assert!(hardlink_inode.is_some(), "硬链接应该仍然存在");
        
        // 验证 nlink 计数减少
        let hardlink_inode = hardlink_inode.unwrap();
        let hardlink_id = hardlink_inode.id();
        let remaining_nlink = hardlink_inode.as_file_ref().unwrap().nlink;
        println!("删除原始文件后，硬链接的 nlink 计数: {}", remaining_nlink);
        
        // 从存储中重新加载 inode 来验证
        let stored_inode = fs_dir.store.get_inode(hardlink_id).unwrap();
        if let Some(stored_file) = stored_inode {
            println!("从存储中重新加载的 nlink 计数: {}", stored_file.as_file_ref().unwrap().nlink);
        }
        
        assert_eq!(remaining_nlink, 1, "删除一个硬链接后 nlink 应该是 1");
        
        // 删除最后一个硬链接
        let del_result2 = fs_dir.delete(&hardlink_path, false).unwrap();
        assert!(del_result2.inodes > 0, "删除最后一个硬链接应该删除 inode");
        
        println!("硬链接删除测试通过！");
    }
} 