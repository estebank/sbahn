use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use constants::BUFFER_SIZE;
use message::{Key, Value, InternodeRequest, InternodeResponse};
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;


pub type DataMap = HashMap<Key, Value>;


pub struct StorageNode {
    pub shard: usize,
    pub address: String,
    pub map: Arc<Mutex<DataMap>>,
}


fn read_from_map(map: &mut DataMap, key: &Key) -> InternodeResponse {
    debug!("Reading {:?}", key);
    match map.get(&key) {
        Some(value) => {
            InternodeResponse::Value {
                key: key.clone().to_owned(),
                value: value.to_owned(),
            }
        }
        None => {
            InternodeResponse::Value {
                key: key.clone().to_owned(),
                value: Value::None,
            }
        }
    }
}


pub fn handle_message(shard: usize,
                      message: InternodeRequest,
                      map: &mut DataMap,
                      shards: &Vec<Vec<String>>)
                      -> InternodeResponse {
    match message {
        InternodeRequest::Read {key} => {
            if key.shard(shards.len()) == shard {
                read_from_map(map, &key)
            } else {
                let error = format!("{:?} doesn't belong to this shard!", key);
                error!("{}", error);
                InternodeResponse::Error {
                    key: key.clone().to_owned(),
                    message: error,
                }
            }
        }
        InternodeRequest::Write {key, value} => {
            if key.shard(shards.len()) == shard {
                debug!("Writing {:?} -> {:?}", key, value);

                match value {
                    Value::None => {
                        let error = format!("Write operation at {:?} with None. This should have \
                                             been a Tombstone",
                                            key);
                        error!("{}", error);
                        InternodeResponse::Error {
                            key: key.clone().to_owned(),
                            message: error,
                        }
                    }
                    Value::Value {timestamp, ..} | Value::Tombstone {timestamp} => {
                        map.insert(key.clone().to_owned(), value.clone().to_owned());
                        InternodeResponse::WriteAck {
                            key: key.to_owned(),
                            timestamp: timestamp.clone().to_owned(),
                        }
                    }
                }
            } else {
                let error = format!("{:?} doesn't belong to this shard!", key);
                error!("{}", error);
                InternodeResponse::Error {
                    key: key.clone().to_owned(),
                    message: error,
                }
            }
        }
    }
}


pub fn handle_client(shard: usize,
                     stream: &mut TcpStream,
                     map: &mut DataMap,
                     shards: &Vec<Vec<String>>) {
    let mut buf = [0; BUFFER_SIZE];
    &stream.read(&mut buf);
    let m: InternodeRequest = match decode(&buf) {
        Ok(m) => m,
        Err(_) => panic!("decoding error!"),
    };

    debug!("Message received: {:?}", m);
    let response: InternodeResponse = handle_message(shard, m, map, &shards);
    let encoded = encode(&response, SizeLimit::Infinite);
    let _ = match encoded {
        Ok(b) => stream.write(&b),
        Err(_) => panic!("encoding error! {:?}", response),
    };
}

impl StorageNode {
    pub fn new(local_address: String, shard_number: usize) -> StorageNode {
        let map = Arc::new(Mutex::new(DataMap::new()));
        StorageNode {
            shard: shard_number,
            address: local_address,
            map: map,
        }
    }

    pub fn listen(&mut self, shards: &Vec<Vec<String>>) {
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
                    debug!("Starting listener stream: {:?}", stream);
                    let map = self.map.clone();
                    let shards = shards.clone();
                    thread::spawn(move || {
                        // connection succeeded
                        let mut stream = stream;
                        let mut map = map.lock().unwrap();
                        handle_client(shard, &mut stream, &mut map, &shards);
                    });
                }
                Err(e) => {
                    error!("connection failed!: {:?}", e);
                }
            }
            let map = self.map.clone();
            let map = map.lock().unwrap();
            debug!("Contents of shard {:?} @ {:?} map: {:?}",
                     self.shard,
                     self.address,
                     *map);
        }

        // close the socket server
        drop(listener);
    }
}
