use std::fs;
use clap::Parser;
use map_reduce::models::{KeyValue, MapFunction, ReduceFunction};
use std::io::Write;

#[derive(Parser, Debug)]
struct CliArgs {
    #[arg(short, long)]
    app: String,
    #[arg(short, long)]
    output_path: String,
    #[arg(short, long, num_args = 1.., value_delimiter = ' ')]
    input: Vec<String>,
}

fn main() {
    let args = CliArgs::parse();
    let (map, reduce) = load_function(&args.app);

    let mut intermediate: Vec<KeyValue> = args
        .input
        .into_iter()
        .map(|file| {
            (
                std::fs::read_to_string(&file).expect("Error while reading input file"),
                file,
            )
        })
        .map(|(content, file)| map(file, content))
        .flatten()
        .collect::<Vec<_>>();
    intermediate.sort_by(|k1, k2| k1.key.cmp(&k2.key));

    fs::create_dir_all(&args.output_path).expect("Couldn't create output directory");
    let path = format!("{}/mr-out-0", args.output_path);
    let mut file = std::fs::File::options()
        .write(true)
        .create(true)
        .append(false)
        .open(&path)
        .expect(&format!("Couldn't open file `{path}`"));

    let mut left_idx = 0;
    while left_idx < intermediate.len() {
        let mut right_idx = left_idx + 1;
        while right_idx < intermediate.len()
            && intermediate[left_idx].key.eq(&intermediate[right_idx].key)
        {
            right_idx = right_idx + 1;
        }

        let values: Vec<String> = intermediate[left_idx..right_idx]
            .iter()
            .map(|k| k.value.to_string())
            .collect();
        let key = &intermediate[left_idx].key;
        let reduce_value = reduce(key.to_owned(), values);

        writeln!(file, "{} {}", key, reduce_value).expect("Error while writing to file");

        left_idx = right_idx;
    }
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
