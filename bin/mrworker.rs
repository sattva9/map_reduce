use clap::Parser;
use map_reduce::models::{MapFunction, ReduceFunction};
use map_reduce::worker::Worker;

#[derive(Parser)]
struct CliArgs {
    #[arg(short, long)]
    app: String,
    #[arg(short, long)]
    server_url: String,
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    let (map, reduce) = load_function(&args.app);
    let worker = Worker::new(map, reduce);
    worker.start(args.server_url).await;
}

fn load_function(app_path: &str) -> (MapFunction, ReduceFunction) {
    unsafe {
        let lib = libloading::Library::new(app_path).expect("Error while loading dynamic library");
        let map = lib
            .get::<MapFunction>(b"map")
            .expect("Error while loading `map` function from dynamic library");
        let reduce = lib
            .get::<ReduceFunction>(b"reduce")
            .expect("Error while loading `reduce` function from dynamic library");
        (*map, *reduce)
    }
}
