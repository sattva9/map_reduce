use crate::models::{KeyValue, MapFunction, ReduceFunction};
use crate::task::task_client::TaskClient;
use crate::task::*;
use hash32::Hasher;
use std::collections::HashMap;
use std::fs;
use std::hash::Hash;
use std::io::Write;
use tonic::transport::Channel;

pub struct Worker {
    map: MapFunction,
    reduce: ReduceFunction,
}

impl Worker {
    pub fn new(map: MapFunction, reduce: ReduceFunction) -> Self {
        Self { map, reduce }
    }

    pub async fn start(&self, server_url: String) {
        let mut client = TaskClient::connect(server_url)
            .await
            .expect("Couldn't start RPC client");

        loop {
            let request = tonic::Request::new(Empty {});
            let response = client
                .get_task(request)
                .await
                .expect("Error in RPC for getting tasks")
                .into_inner();
            println!("{:?}", response);
            match response.task_type {
                0 => {
                    println!("Sleeping for 2sec");
                    std::thread::sleep(std::time::Duration::new(2, 0));
                }
                1 => {
                    break;
                }
                2 => {
                    self.process_map_task(
                        response.data.expect("Error while running map task"),
                        &mut client,
                    )
                    .await;
                }
                3 => {
                    self.process_reduce_task(
                        response.data.expect("Error while running reduce task"),
                        &mut client,
                    )
                    .await;
                }
                _ => {
                    println!("Sleeping for 2sec");
                    std::thread::sleep(std::time::Duration::new(2, 0));
                }
            }
        }
    }

    async fn process_reduce_task(&self, task: TaskData, client: &mut TaskClient<Channel>) {
        println!("Starting reduce task {}", task.task_id);

        let reduce = self.reduce;
        println!("{:?}", task.input_files);
        let mut intermediate = task
            .input_files
            .into_iter()
            .map(|file| fs::read_to_string(&file).expect("Error intermediate file while read"))
            .map(|content| {
                serde_json::from_str::<Vec<KeyValue>>(&content)
                    .expect("Error while converting string to KeyValue")
            })
            .flatten()
            .collect::<Vec<_>>();
        intermediate.sort_by(|k1, k2| k1.key.cmp(&k2.key));

        let output_path = format!("{}/output", task.output_path);
        let output_tmp_path = format!(
            "{}/tmp-{}",
            output_path,
            chrono::Local::now().timestamp_millis()
        );
        fs::create_dir_all(&output_tmp_path).expect("Error while creating tmp output directory");

        let file_path = format!("{}/mr-out-{}", output_path, task.task_id);
        let tmp_file_path = format!("{}/mr-out-{}", output_tmp_path, task.task_id);
        let mut file = std::fs::File::options()
            .write(true)
            .create(true)
            .append(false)
            .open(&tmp_file_path)
            .expect(&format!("Couldn't open file `{}`", tmp_file_path));

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

        fs::rename(tmp_file_path, file_path).expect("Couldn't rename tmp output path");
        fs::remove_dir(output_tmp_path).expect("Error while removing tmp output directory");
        let request = tonic::Request::new(ReduceTaskCompleteRequest {
            task_id: task.task_id,
        });
        let _response = client
            .reduce_task_complete(request)
            .await
            .expect("Error in RPC call for reduce task completion update")
            .into_inner();
    }

    async fn process_map_task(&self, task: TaskData, client: &mut TaskClient<Channel>) {
        println!(
            "Starting map task {} with reduce {}",
            task.task_id, task.n_reduce
        );

        let map = self.map;
        let mut intermediate_reduce = HashMap::with_capacity(task.n_reduce as usize);

        task.input_files
            .into_iter()
            .map(|file| {
                (
                    std::fs::read_to_string(&file).expect("Error while reading input file"),
                    file,
                )
            })
            .map(|(content, file)| map(file, content))
            .flatten()
            .for_each(|kv| {
                let hash_value = ihash(&kv.key) % task.n_reduce;
                intermediate_reduce
                    .entry(hash_value)
                    .or_insert(Vec::new())
                    .push(kv);
            });

        let mut intermediate_files = HashMap::with_capacity(task.n_reduce as usize);
        let intermediate_path = format!("{}/intermediate", task.output_path);
        let intermediate_tmp_path = format!(
            "{}/intermediate/tmp-{}-{}",
            task.output_path,
            task.task_id,
            chrono::Local::now().timestamp_millis()
        );
        fs::create_dir_all(&intermediate_tmp_path)
            .expect("Error while creating tmp intermediate directoty");

        for (reduce, kv) in intermediate_reduce {
            let file_name = format!("mr-{}-{}", task.task_id, reduce);
            let file_path = format!("{intermediate_tmp_path}/{file_name}");
            let mut file = std::fs::File::options()
                .write(true)
                .create(true)
                .append(false)
                .open(&file_path)
                .expect(&format!("Couldn't open file `{file_path}`"));
            let byte_data = serde_json::to_value(kv)
                .expect("Error while converting KeyValue list to string")
                .to_string();
            file.write_all(byte_data.as_bytes())
                .expect("Error while writing intermediate data to file");
            intermediate_files.insert(reduce, file_name);
        }

        let mut paths = HashMap::with_capacity(intermediate_files.len());
        for (reduce, file_name) in intermediate_files.drain() {
            let tmp_file = format!("{intermediate_tmp_path}/{file_name}");
            let intermediate_file = format!("{intermediate_path}/{file_name}");
            fs::rename(&tmp_file, &intermediate_file)
                .expect("Couldn't rename tmp intermediate path {}");
            paths.insert(reduce, intermediate_file);
        }
        fs::remove_dir(intermediate_tmp_path).expect("Error while removing tmp intermediate file");
        let request = tonic::Request::new(MapTaskCompleteRequest {
            task_id: task.task_id,
            files: paths,
        });
        let _response = client
            .map_task_complete(request)
            .await
            .expect("Error in RPC while map task completion update")
            .into_inner();
    }
}

fn ihash(key: &str) -> u32 {
    let mut hash = hash32::Murmur3Hasher::default();
    key.hash(&mut hash);
    hash.finish32()
}
