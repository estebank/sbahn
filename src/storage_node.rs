use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use client;
use constants::BUFFER_SIZE;
use constants::SHARD_SIZE;
use message::{Action, Key, Value, Message};
use std::collections::HashMap;
use std::hash::{Hash, SipHasher, Hasher};
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use time;


pub type DataMap = HashMap<Key, Value>;


pub struct StorageNode {
    pub shard: usize,
    pub address: String,
    pub map: Arc<Mutex<DataMap>>,
}


fn read_from_map(map: &mut DataMap, key: &Key) -> Action {
    println!("@@@ Reading {:?}", key);
    match map.get(&key) {
        Some(value) => Action::Value {
            key: key.clone().to_owned(),
            value: value.to_owned(),
        },
        None => Action::Value {
            key: key.clone().to_owned(),
            value: Value::None,
        },
    }
}


#[inline]
fn belongs_to_shard(key: &Key, shard: usize) -> u64 {
      key.hash() % (SHARD_SIZE as u64)
}


fn get_now() -> u64 {
    let now = time::get_time();
    let sec = now.sec;
    let nsec = now.nsec;
    // TODO: consider using a Timespec instead.
    ((sec as u64) * 1_000_000)  + (nsec as u64 / 1000)
}


pub fn handle_message(shard: usize, message: Message, map: &mut DataMap, shards: &Vec<String>) -> Action {
    match message.action {
        Action::Read { key } => {
            if belongs_to_shard(&key, shard) == shard as u64 {
                read_from_map(map, &key)
            } else {
                panic!("{:?} doesn't belong to this shard!", key);
            }
        },
        Action::Write {key, value} => {
            if belongs_to_shard(&key, shard) == shard as u64 {
                println!("@@@ Writing {:?}", value);
                let timestamp = get_now();

                let v = match value.get_content() {
                    Some(v) => Value::Value {
                        content: v.clone().to_owned(),
                        timestamp: timestamp,
                    },
                    None => Value::Tombstone {
                        timestamp: timestamp,
                    }
                };
                map.insert(key.clone().to_owned(), v);
                Action::WriteAck {
                    key: key.to_owned(),
                    timestamp: timestamp,
                }
            } else {
                println!("{:?} doesn't belong to this shard!", key);
                Action::Error
            }
        },
        _ => Action::Error,
    }
}


pub fn handle_client(shard: usize, stream: &mut TcpStream, map: &mut DataMap, shards: &Vec<String>) {
    let mut buf = [0; BUFFER_SIZE];
    &stream.read(&mut buf);
    let m = match decode(&buf) {
        Ok(m) => m,
        Err(_) => panic!(),
    };
    println!("@@@ Message received: {:?}", m);
    let response: Action = handle_message(shard, m, map, &shards);
    let encoded = encode( &Message { action: response }, SizeLimit::Infinite);
    match  encoded {
        Ok(b) => stream.write(&b),
        Err(_) => panic!("encoding error!"),
    };
}

impl StorageNode {
    pub fn new(local_address: String, shard_number: usize) -> StorageNode {
        let mut map = Arc::new(Mutex::new(DataMap::new()));
        StorageNode {
            shard: shard_number,
            address: local_address,
            map: map,
        }
    }

    pub fn listen(&mut self, shards: &Vec<String>) {
      let shard = self.shard;
      let address = &*self.address;
      let listener = match TcpListener::bind(address) {
          Ok(l) => l,
          Err(e) => panic!("Error binding this storage node @ {:?}: {:?}", address, e),
      };

      // accept connections and process them, spawning a new thread for each one
      for stream in listener.incoming() {
          match stream {
              Ok(stream) => {
                  //println!("@@@ Starting listener stream: {:?}", stream);
                  let map = self.map.clone();
                  let shards = shards.clone();
                  thread::spawn(move|| {
                      // connection succeeded
                      let mut stream = stream;
                      let mut map = map.lock().unwrap();
                      handle_client(shard, &mut stream, &mut map, &shards);
                  });
              }
              Err(e) => {
                  println!("@@@ connection failed!: {:?}", e);
              }
          }
          let map  = self.map.clone();
          let map = map.lock().unwrap();
          println!("@@@ Contents of shard {:?} map: {:?}", self.address, *map);
      }

      // close the socket server
      drop(listener);
    }
}
