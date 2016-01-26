use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use message::{Buffer, Key, Value, InternodeRequest, InternodeResponse};
use network::NetworkRead;
use std::io::Write;
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use storage::StorageBackend;

pub struct StorageNode<Backend: StorageBackend + 'static> {
    pub shard: usize,
    pub shard_count: usize,
    pub address: SocketAddrV4,
    pub map: Arc<Backend>,
}

#[derive(Debug)]
struct ClientHandler<Backend: StorageBackend + 'static> {
    stream: TcpStream,
    shard: usize,
    shard_count: usize,
    map: Arc<Backend>,
}

impl<Backend: StorageBackend + 'static> ClientHandler<Backend> {
    fn new(stream: TcpStream,
           map: Arc<Backend>,
           shard_number: usize,
           shard_count: usize)
           -> ClientHandler<Backend> {
        ClientHandler {
            stream: stream,
            shard: shard_number,
            shard_count: shard_count,
            map: map,
        }
    }

    pub fn handle_client(&mut self) {
        let mut value: Buffer = vec![];
        if self.stream.read_to_message_end(&mut value).is_err() {
            panic!("Couldn't read from stream.");
        }

        let m: InternodeRequest = match decode(&value) {
            Ok(m) => m,
            Err(_) => panic!("decoding error!"),
        };

        debug!("Message received: {:?}", m);
        let response: InternodeResponse = self.handle_message(m);
        let encoded = encode(&response, SizeLimit::Infinite);
        let _ = match encoded {
            Ok(b) => self.stream.write(&b),
            Err(_) => panic!("encoding error! {:?}", response),
        };
    }

    pub fn handle_message(&mut self, message: InternodeRequest) -> InternodeResponse {
        match message {
            InternodeRequest::Read {key} => self.get(key),
            InternodeRequest::Write {key, value} => self.insert(key, value),
        }
    }

    fn get(&mut self, key: Key) -> InternodeResponse {
        debug!("Reading {:?}", key);
        let key_shard = key.shard(self.shard_count.clone());
        let this_shard = self.shard;
        if key_shard == this_shard {

            debug!("get self {:?}", self);
            debug!("get self.map {:?}", self.map);
            let v = self.map.get(&key);
            debug!("get value {:?}", v);
            // let ref mut map: Backend = *match Arc::get_mut(&mut self.map);
            match v {
                Some(value) => {
                    InternodeResponse::Value {
                        key: key.to_owned(),
                        value: value.to_owned(),
                    }
                }
                None => {
                    InternodeResponse::Value {
                        key: key.to_owned(),
                        value: Value::None,
                    }
                }
            }
        } else {
            let error = format!("{:?} doesn't belong to this shard!", key);
            error!("{}", error);
            InternodeResponse::Error {
                key: key.to_owned(),
                message: error,
            }
        }
    }

    fn insert(&mut self, key: Key, value: Value) -> InternodeResponse {
        debug!("Writing {:?} -> {:?}", key, value);
        let key_shard = key.shard(self.shard_count.clone());
        let this_shard = self.shard;
        if key_shard == this_shard {
            match value {
                Value::None => {
                    let error = format!("Write operation at {:?} with None.This should have been \
                                         a Tombstone",
                                        key);
                    error!("{}", error);
                    InternodeResponse::Error {
                        key: key.to_owned(),
                        message: error,
                    }
                }
                Value::Value {timestamp, ..} | Value::Tombstone {timestamp} => {
                    debug!("set self.map {:?}", self.map);
                    debug!("key: {:?}", key);
                    debug!("timestamp: {:?}", timestamp);
                    let map = &self.map;
                    map.insert(key.to_owned(), value.to_owned());
                    InternodeResponse::WriteAck {
                        key: key.to_owned(),
                        timestamp: timestamp.to_owned(),
                    }
                }
            }
        } else {
            let error = format!("{:?} doesn't belong to this shard!", key);
            error!("{}", error);
            InternodeResponse::Error {
                key: key.to_owned(),
                message: error,
            }
        }
    }
}

impl<Backend: StorageBackend + 'static> StorageNode<Backend> {
    pub fn new(local_address: &SocketAddrV4,
               shard_number: usize,
               shard_count: usize)
               -> StorageNode<Backend> {
        let map = Arc::new(Backend::new());
        StorageNode {
            shard: shard_number,
            shard_count: shard_count,
            address: local_address.to_owned(),
            map: map,
        }
    }

    pub fn listen(&mut self) {
        let listener = match TcpListener::bind(self.address) {
            Ok(l) => l,
            Err(e) => {
                panic!("Error binding this storage node @ {:?}: {:?}",
                       self.address,
                       e)
            }
        };

        // accept connections and process them, spawning a new thread for each one
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    debug!("Starting listener stream: {:?}", stream);
                    let shard = self.shard;
                    let shard_count = self.shard_count;
                    let map = self.map.clone();
                    thread::spawn(move || {
                        let mut ch = ClientHandler::new(stream, map, shard, shard_count);
                        // connection succeeded
                        ch.handle_client();
                    });
                }
                Err(e) => {
                    error!("connection failed!: {:?}", e);
                }
            }
            debug!("Contents of shard {:?} @ {:?} map: {:?}",
                   self.shard,
                   self.address,
                   self.map);
        }

        // close the socket server
        drop(listener);
    }
}
