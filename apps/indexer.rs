use std::collections::HashSet;

#[allow(unused)]
pub struct KeyValue {
    key: String,
    value: String,
}

#[no_mangle]
pub fn map(filename: String, contents: String) -> Vec<KeyValue> {
    contents
        .split(|c: char| !c.is_alphabetic())
        .filter(|key| !key.is_empty())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|key| KeyValue { key: key.to_string(), value: filename.to_owned() })
        .collect()
}

#[no_mangle]
pub fn reduce(_key: String, mut values: Vec<String>) -> String {
    values.sort();
    format!("{} {}", values.len(), values.join(","))
}

