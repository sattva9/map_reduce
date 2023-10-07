
#[allow(unused)]
pub struct KeyValue {
    key: String,
    value: String,
}

#[no_mangle]
pub fn map(_filename: String, contents: String) -> Vec<KeyValue> {
    contents
        .split(|c: char| !c.is_alphabetic())
        .filter(|key| !key.is_empty())
        .map(|key| KeyValue { key: key.to_string(), value: "1".to_string() })
        .collect()
}

#[no_mangle]
pub fn reduce(_key: String, values: Vec<String>) -> String {
    values.len().to_string()
}

#[cfg(test)]
mod mrapps_test {
    use std::collections::HashMap;
    use super::*;

    #[test]
    fn test_map() {
        let c = map("", "there are the contents here passed from the test class");
        let mut map = HashMap::new();
        c
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .for_each(|(key, value)| map.entry(key)
                .or_insert(Vec::new())
                .push(value)
            );
        let result = map.into_iter()
            .map(|(key, value)| (key, reduce(key, &value)))
            .collect::<HashMap<_, _>>();
        println!("{result:?}")
    }
}


