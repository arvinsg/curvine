use std::sync::{Arc, Mutex};
use std::thread;
use log::{error, info, warn};
use curvine_client::file::CurvineFileSystem;
use curvine_client::rpc::JobMasterClient;
use curvine_common::conf::ClusterConf;
use curvine_common::FsResult;
use curvine_common::proto::LoadState;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sync::channel::{BlockingChannel, BlockingReceiver, BlockingSender};
use orpc::sync::FastDashMap;
use orpc::try_option;
use crate::worker::load::LoadTask;
use crate::worker::load::task_runner::TaskRunner;

pub struct TaskManager {
    tasks: Arc<FastDashMap<String, LoadTask>>,
    sender: BlockingSender<LoadTask>,
    receiver: Option<BlockingReceiver<LoadTask>>,
    rt: Arc<Runtime>,
    fs: CurvineFileSystem,
    master_client: JobMasterClient,
    progress_interval_ms: u64,
    task_timeout_ms: u64,
}

impl TaskManager {
    pub fn with_rt(conf: &ClusterConf, rt: Arc<Runtime>) -> FsResult<Self> {
        let (sender, receiver) = BlockingChannel::new(0).split();
        let fs = CurvineFileSystem::with_rt(conf.clone(), rt.clone())?;
        let master_client = JobMasterClient::new(fs.fs_client());

        let mgr = Self {
            tasks: Arc::new(FastDashMap::default()),
            sender,
            receiver: Some(receiver),
            rt,
            fs,
            master_client,
            progress_interval_ms: 500000,
            task_timeout_ms: 500000,
        };
        Ok(mgr)
    }

    pub fn start(&mut self){
        let builder = thread::Builder::new().name("task-manager".to_string());
        let receiver = self.recevier.take().unwrap();
        let tasks = self.tasks.clone();
        let rt = self.rt.clone();
        let fs = self.fs.clone();
        let master_client = self.master_client.clone();
        let progress_interval_ms = self.progress_interval_ms;
        let task_timeout_ms = self.task_timeout_ms;

        builder.spawn(move || {
           Self::run(
                tasks,
                receiver,
                rt,
                fs,
                master_client,
                progress_interval_ms,
                task_timeout_ms,
            )
        }).unwrap();
    }

    fn run(
        tasks: Arc<FastDashMap<String, LoadTask>>,
        mut pending: BlockingReceiver<LoadTask>,
        rt: Arc<Runtime>,
        fs: CurvineFileSystem,
        master_client: JobMasterClient,
        progress_interval_ms: u64,
        task_timeout_ms: u64,
    ) {
        while let Some(task) = pending.recv() {
            let task_id = task.task_id.clone();
            let runner = TaskRunner::new(
                task,
                tasks.clone(),
                fs.clone(),
                master_client.clone(),
                progress_interval_ms,
                task_timeout_ms
            );
            if let Err(e) = rt.block_on(runner.run()) {
                error!("run task {} failed, err: {:?}", task.task_id, e);
            }
            let _ = tasks.remove(&task_id);
        }
    }

    pub fn submit_task(&self, task: LoadTask) -> FsResult<()> {
        if self.tasks.contains_key(&task.task_id) {
            return Ok(())
        }

        info!("submit task {} for job {}", task.task_id, task.job_id);

        let task_id = task.task_id.clone();
        self.pending.send(task)?;
        self.tasks.insert(task_id, task.clone());

        Ok(())
    }

    fn get_all_task(&self, job_id: impl AsRef<str>) -> Vec<LoadTask> {
        let job_id = job_id.as_ref();
        let mut res = vec![];
        for task in self.tasks.iter() {
            if task.key() == job_id {
                res.push(task.value().clone())
            }
        }
        res
    }

    fn cancel_job(&self, job_id: impl AsRef<str>) -> FsResult<()> {
        let job_id = job_id.as_ref();
        let all_task = self.get_all_task(job_id);
        for task in &all_task{
            if let Some(mut task_lock) = self.tasks.get_mut(&task.task_id) {
                task_lock.update_state(LoadState::Canceled, "canceled by user");
            self.tasks.remove(job_id);
        }

        info!("Successfully canceled {} tasks for job {}", all_task.len(), job_id);
        Ok(())
    }

}