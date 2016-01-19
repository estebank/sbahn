use std::collections::HashMap;
use message::{Key, Value};
use std::sync::Mutex;
use std::fmt::Debug;

/// A generic storage backend for `StorageNode`s to use as persistence layer.
pub trait StorageBackend where Self: Debug + Send + Sync {
    fn new() -> Self;
    /// Persist `value` under `key`.
    fn insert(&self, key: Key, value: Value);
    /// Get a `Value` for the given `key`.
    fn get(&self, key: &Key) -> Option<Value>;
}

/// A basic `HashMap` based backend for in-memory `StorageNode`s.
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
