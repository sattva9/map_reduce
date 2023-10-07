use clap::Parser;
use map_reduce::coordinator::make_coordinator;

#[derive(Parser, Debug)]
struct CliArgs {
    #[arg(short, long)]
    output_path: String,
    #[arg(short, long)]
    server_addr: String,
    #[arg(short, long, num_args = 1.., value_delimiter = ' ')]
    input: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();
    make_coordinator(args.input, 10, args.output_path, args.server_addr).await;
    println!("Map Reduce task complete. Exiting");
}
