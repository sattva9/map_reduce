pub mod coordinator;
pub mod models;
pub mod rpc;
pub mod worker;

pub mod task {
    tonic::include_proto!("task");
}
