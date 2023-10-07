use crate::coordinator::TaskService;
use crate::task::task_server::Task;
use crate::task::*;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl Task for TaskService {
    async fn get_task(&self, _request: Request<Empty>) -> Result<Response<TaskResponse>, Status> {
        let (data, task_type) = self.get_task_for_worker();
        Ok(Response::new(TaskResponse {
            data,
            task_type: task_type.into(),
        }))
    }

    async fn map_task_complete(
        &self,
        request: Request<MapTaskCompleteRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.mark_map_task_complete(request);
        Ok(Response::new(Empty {}))
    }

    async fn reduce_task_complete(
        &self,
        request: Request<ReduceTaskCompleteRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.mark_reduce_task_complete(request);
        Ok(Response::new(Empty {}))
    }
}
