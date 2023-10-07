use serde::{Deserialize, Serialize};

pub type MapFunction = fn(String, String) -> Vec<KeyValue>;
pub type ReduceFunction = fn(String, Vec<String>) -> String;

#[derive(Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}
