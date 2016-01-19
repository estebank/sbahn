use std::collections::HashMap;
use message::{Key, Value};
use std::sync::Mutex;
use std::fmt::Debug;

pub trait StorageBackend where Self: Debug + Send + Sync {
    fn new() -> Self;
    fn insert(&self, key: Key, value: Value);
    fn get(&self, key: &Key) -> Option<Value>;
}

#[derive(Debug)]
pub struct HashMapBackend {
    hashmap: Mutex<HashMap<Key, Value>>,
}

impl StorageBackend for HashMapBackend {
    fn new() -> HashMapBackend {
        debug!("New HashMapBackend");
        let hm = HashMap::new();
        HashMapBackend { hashmap: Mutex::new(hm) }
    }

    fn insert(&self, key: Key, value: Value) {
        debug!("[HashMapBackend] Going to insert {:?}, {:?}", key, value);
        let lock = self.hashmap.lock();
        let mut map = lock.unwrap();
        map.insert(key, value.clone());
        debug!("[HashMapBackend] inserted {:?}", value);
    }

    fn get(&self, key: &Key) -> Option<Value> {
        debug!("[HashMapBackend] Going to read {:?}", key);
        let lock = self.hashmap.lock();
        let map = lock.unwrap();
        let value = map.get(key);
        debug!("[HashMapBackend] Value read {:?}", value);
        match value {
            Some(x) => Some(x.clone().to_owned()),
            None => None,
        }
    }
}

unsafe impl Sync for HashMapBackend {}
