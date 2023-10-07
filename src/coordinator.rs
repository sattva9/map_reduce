use crate::task::task_server::TaskServer;
use crate::task::*;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Server;

lazy_static! {
    static ref JOB_COMPLETE_STATUS: AtomicBool = AtomicBool::new(false);
}

pub struct Coordinator;

impl Coordinator {
    async fn server(
        input_files: Vec<String>,
        n_reduce: u32,
        output_path: String,
        server_addr: String,
    ) {
        let address = server_addr
            .parse()
            .expect("Error while parsing server address");
        let task_service = TaskService::new(input_files, n_reduce, output_path);

        Server::builder()
            .add_service(TaskServer::new(task_service))
            .serve_with_shutdown(address, async {
                tokio::spawn(Self::done())
                    .await
                    .expect("Failed to run service shutdown function");
            })
            .await
            .expect("Failed to start RPC server");
    }

    pub async fn done() {
        loop {
            if !JOB_COMPLETE_STATUS.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(2));
            } else {
                break;
            }
        }
    }
}

pub async fn make_coordinator(
    input_files: Vec<String>,
    n_reduce: u32,
    output_path: String,
    server_addr: String,
) {
    Coordinator::server(input_files, n_reduce, output_path, server_addr).await;
}

pub struct TaskService {
    service: Arc<TaskServiceInner>,
    output_path: String,
    n_reduce: u32,
}

impl TaskService {
    fn new(input_files: Vec<String>, n_reduce: u32, output_path: String) -> Self {
        let service = TaskServiceInner::new(input_files);
        Self {
            service,
            output_path,
            n_reduce,
        }
    }

    pub fn get_task_for_worker(&self) -> (Option<TaskData>, TaskType) {
        let (task, task_type) = self.service.get_task_for_worker();
        let data = task.map(|task| TaskData {
            input_files: task.input_files.to_owned(),
            task_id: task.task_id,
            n_reduce: self.n_reduce,
            output_path: self.output_path.to_owned(),
        });
        (data, task_type)
    }

    pub fn mark_map_task_complete(&self, request: MapTaskCompleteRequest) {
        self.service.mark_map_task_complete(request)
    }

    pub fn mark_reduce_task_complete(&self, request: ReduceTaskCompleteRequest) {
        self.service.mark_reduce_task_complete(request)
    }
}

pub struct TaskServiceInner {
    map_tasks: HashMap<u32, Arc<TaskMeta>>,
    reduce_tasks: RwLock<HashMap<u32, Arc<TaskMeta>>>,
    tasks_status: RwLock<MapReduceTaskStatus>,
}

#[derive(Clone, Copy)]
enum MapReduceTaskStatus {
    Map,
    Reduce,
    Complete,
}

pub struct TaskMeta {
    pub task_id: u32,
    pub input_files: Vec<String>,
    pub status: RwLock<TaskStatus>,
}

#[derive(Eq, PartialEq)]
pub enum TaskStatus {
    Idle,
    InProgress(Instant),
    Completed,
}

impl TaskServiceInner {
    fn new(input_files: Vec<String>) -> Arc<Self> {
        let map_tasks = input_files
            .into_iter()
            .enumerate()
            .map(|(id, file)| {
                (
                    id as u32,
                    Arc::new(TaskMeta {
                        task_id: id as u32,
                        input_files: vec![file],
                        status: RwLock::new(TaskStatus::Idle),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let service = Arc::new(Self {
            map_tasks,
            reduce_tasks: RwLock::new(HashMap::new()),
            tasks_status: RwLock::new(MapReduceTaskStatus::Map),
        });
        Self::run_background_thread(service.clone());
        service
    }

    fn run_background_thread(service: Arc<TaskServiceInner>) {
        std::thread::spawn(move || {
            loop {
                // println!("Processing background thread");
                let service_tasks_status = *service.tasks_status.read();
                match service_tasks_status {
                    MapReduceTaskStatus::Map => {
                        let is_task_complete = Self::worker_health_check(&service.map_tasks);
                        if is_task_complete {
                            *service.tasks_status.write() = MapReduceTaskStatus::Reduce;
                        }
                    }
                    MapReduceTaskStatus::Reduce => {
                        let is_task_complete =
                            Self::worker_health_check(&service.reduce_tasks.read());
                        if is_task_complete {
                            *service.tasks_status.write() = MapReduceTaskStatus::Complete;
                        }
                    }
                    MapReduceTaskStatus::Complete => {
                        JOB_COMPLETE_STATUS.store(true, Ordering::Relaxed);
                        println!("Exiting background thread");
                        break;
                    }
                }
                // println!("Background thread sleeping for 10sec");
                std::thread::sleep(std::time::Duration::new(10, 0))
            }
        });
    }

    fn worker_health_check(tasks: &HashMap<u32, Arc<TaskMeta>>) -> bool {
        let mut completed_tasks_count = 0;
        tasks.iter().for_each(|(_id, task)| {
            let expired = match *task.status.read() {
                TaskStatus::InProgress(instant) => instant.elapsed().as_millis() > 10000,
                TaskStatus::Completed => {
                    completed_tasks_count += 1;
                    false
                }
                TaskStatus::Idle => false,
            };
            if expired {
                *task.status.write() = TaskStatus::Idle;
                println!("Reset task {}", task.task_id);
            }
        });
        if completed_tasks_count == tasks.len() {
            true
        } else {
            false
        }
    }

    pub fn get_task_for_worker(&self) -> (Option<Arc<TaskMeta>>, TaskType) {
        let (task, task_type) = self.get_idle_task();
        if let Some(task) = task.as_ref() {
            *task.status.write() = TaskStatus::InProgress(Instant::now());
            println!("Starting {:?} task {}", task_type, task.task_id);
        }
        (task, task_type)
    }

    fn get_idle_task(&self) -> (Option<Arc<TaskMeta>>, TaskType) {
        match *self.tasks_status.read() {
            MapReduceTaskStatus::Map => {
                let task = Self::get_task(&self.map_tasks);
                if task.is_some() {
                    (task, TaskType::Map)
                } else {
                    (task, TaskType::Idle)
                }
            }
            MapReduceTaskStatus::Reduce => {
                let task = Self::get_task(&self.reduce_tasks.read());
                if task.is_some() {
                    (task, TaskType::Reduce)
                } else {
                    (task, TaskType::Idle)
                }
            }
            MapReduceTaskStatus::Complete => (None, TaskType::Exit),
        }
    }

    fn get_task(tasks: &HashMap<u32, Arc<TaskMeta>>) -> Option<Arc<TaskMeta>> {
        tasks
            .iter()
            .find(|(_id, task)| task.status.read().eq(&TaskStatus::Idle))
            .map(|(_id, task)| task.clone())
    }

    fn mark_map_task_complete(&self, request: MapTaskCompleteRequest) {
        if let Some(task) = self.map_tasks.get(&request.task_id) {
            if task.status.read().eq(&TaskStatus::Completed) {
                return;
            }

            let mut tasks = self.reduce_tasks.write();
            for (reduce, file) in request.files {
                let input_files = match tasks.get(&reduce) {
                    Some(task) => {
                        let mut files = task.input_files.to_owned();
                        files.push(file);
                        files
                    }
                    None => vec![file],
                };
                tasks.insert(
                    reduce,
                    Arc::new(TaskMeta {
                        task_id: reduce,
                        input_files,
                        status: RwLock::new(TaskStatus::Idle),
                    }),
                );
            }

            *task.status.write() = TaskStatus::Completed;
            println!("Completed map task {}", task.task_id);
        }
    }

    fn mark_reduce_task_complete(&self, request: ReduceTaskCompleteRequest) {
        if let Some(task) = self.reduce_tasks.write().get(&request.task_id) {
            *task.status.write() = TaskStatus::Completed;
            println!("Completed reduce task {}", task.task_id);
        }
    }
}
