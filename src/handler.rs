use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use client;
use eventual::*;
use message::*;
use network::NetworkRead;
use std::fmt::Debug;
use std::io::Write;
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;
use time;


/// Current Unix timestamp
fn get_now() -> u64 {
    let now = time::get_time();
    let sec = now.sec;
    let nsec = now.nsec;
    // Consider using the Timespec directly instead.
    ((sec as u64) * 1_000_000) + (nsec as u64 / 1000)
}

/// Obtain one (any) valid response from all the shard responses.
fn read_one(key: &Key, responses: Vec<Future<InternodeResponse, Error>>) -> client::MessageResult {
    debug!("Reading one");


    Ok(sequence(responses)
           .await()
           .unwrap()
           .iter()
           .next()
           .map(|&(ref m, _)| {
               ResponseMessage {
                   message: m.clone().to_response(),
                   consistency: Consistency::One,
               }
           })
           .unwrap_or(ResponseMessage {
               message: Response::Error {
                   key: key.to_owned(),
                   message: "All the storage nodes replied with errors.".to_string(),
               },
               consistency: Consistency::One,
           }))
}

/// Obtain the newest `Value` among those stored in this shard's `StorageNode`s.
fn read_latest(key: &Key,
               responses: Vec<Future<InternodeResponse, Error>>)
               -> client::MessageResult {
    debug!("Reading latest");
    let responses_needed = responses.len() / 2;
    let ((_, success_count), latest) =
        sequence(responses)
            .reduce(((0, 0), None), |last, r| {
                let ((max_timestamp, success_count), max_response) = last;
                debug!("Max timestamp so far: {:?}", max_timestamp);
                debug!("Max max_response so far: {:?}", max_response);
                match r.get_timestamp() {
                    Some(ts) => {
                        if ts >= max_timestamp {
                            ((ts, success_count + 1), Some(r.clone()))
                        } else {
                            ((max_timestamp, success_count + 1), max_response)
                        }
                    }
                    None => ((max_timestamp, success_count), max_response),
                }
            })
            .await()
            .unwrap();

    debug!("Quorum read final response: {:?}", latest);
    debug!("Nodes responed successfully: {:?}", success_count);
    debug!("Nodes needed for succesfull read: {:?}", responses_needed);

    if success_count <= responses_needed {
        info!("Not enough storage nodes succeeded: {:?} of at least {:?}",
              success_count,
              responses_needed);
        Ok(ResponseMessage {
            message: Response::Error {
                key: key.to_owned(),
                message: "Not enough storage nodes succeeded to give a response".to_string(),
            },
            consistency: Consistency::Latest,
        })
    } else {
        match latest {
            Some(m) => {
                Ok(ResponseMessage {
                    message: m.to_response(),
                    consistency: Consistency::Latest,
                })
            }
            None => {
                Ok(ResponseMessage {
                    message: Response::Error {
                        key: key.to_owned(),
                        message: "?".to_string(),
                    },
                    consistency: Consistency::Latest,
                })
            }
        }
    }
}

/// Read from all nodes for this `Key`'s shard, and use `consistency` to
/// collate the `StorageNode`'s responses.
fn read(shards: &Vec<SocketAddrV4>, key: &Key, consistency: &Consistency) -> client::MessageResult {
    debug!("Read {:?} with {:?} consistency.", key, consistency);
    let mut responses: Vec<Future<InternodeResponse, Error>> = vec![];
    for shard in shards {
        let response = read_from_other_storage_node(&shard, &key);
        responses.push(response);
    }
    match consistency {
        &Consistency::One => read_one(key, responses),
        &Consistency::Latest => read_latest(key, responses),
    }
}

/// Write to all nodes for this `Key`'s shard, and use `consistency` to
/// determine when to acknowledge the write to the client.
fn write(shards: &Vec<SocketAddrV4>,
         key: &Key,
         value: &Value,
         consistency: &Consistency)
         -> client::MessageResult {
    let mut responses: Vec<Future<InternodeResponse, Error>> = vec![];
    for shard in shards {
        let response = write_to_other_storage_node(&shard, &key, &value);
        debug!("Write response for {:?}, {:?} @ Shard {:?}: {:?}",
               key,
               value,
               shard,
               response);
        responses.push(response);
    }
    let mut message = Response::Error {
        key: key.to_owned(),
        message: "Quorum write could not be accomplished.".to_string(),
    };

    let mut write_count = 0;
    for response_result in responses {
        match response_result.await() {
            Ok(response) => {
                match response {
                    InternodeResponse::WriteAck {key, timestamp} => {
                        write_count += 1;
                        if write_count >= (shards.len() / 2) + 1 {
                            debug!("Successful write to mayority of shards for {:?}", key);
                            message = Response::WriteAck {
                                key: key,
                                timestamp: timestamp,
                            };
                        }
                    }
                    _ => (),
                }
            }
            Err(_) => (),
        }
    }
    let r = ResponseMessage {
        message: message,
        consistency: consistency.to_owned(),
    };

    Ok(r)
}


fn read_from_other_storage_node(target: &SocketAddrV4,
                                key: &Key)
                                -> Future<InternodeResponse, Error> {
    debug!("Forwarding read request for {:?} to shard at {:?}.",
           key,
           target);
    let content = InternodeRequest::Read { key: key.to_owned() };
    client::Client::send_to_node(target, &content)
}

fn write_to_other_storage_node(target: &SocketAddrV4,
                               key: &Key,
                               value: &Value)
                               -> Future<InternodeResponse, Error> {
    debug!("Forwarding write request for {:?} to shard at {:?}.",
           key,
           target);
    let request = InternodeRequest::Write {
        key: key.to_owned(),
        value: value.to_owned(),
    };
    client::Client::send_to_node(target, &request)
}

/// Perform a client's `Request` in the appropriate shard and respond to the
/// client with a ResponseMessage.
pub fn handle_client(stream: &mut TcpStream, shards: &Vec<Vec<SocketAddrV4>>) {
    let mut value: Buffer = vec![];

    if stream.read_to_message_end(&mut value).is_err() {
        panic!("Couldn't read from stream.");
    }

    let request: Request = match decode(&value) {
        Ok(m) => m,
        Err(e) => panic!("Message decoding error! {:?}", e),
    };

    debug!("Message received: {:?}", request);

    let timestamp = get_now();
    let r = match request.action {
        Action::Read {key} => {
            let msg_shard = key.shard(shards.len());
            read(&shards[msg_shard], &key, &request.consistency)
        }
        Action::Write {key, content} => {
            let value = Value::Value {
                content: content.to_owned(),
                timestamp: timestamp,
            };
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &value, &request.consistency)
        }
        Action::Delete {key} => {
            let value = Value::Tombstone { timestamp: timestamp };
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &value, &request.consistency)
        }
    };
    debug!("Response to be sent: {:?}", r);
    let _ = match r {
        Ok(message) => {
            let encoded = encode(&message, SizeLimit::Infinite);
            match encoded {
                Ok(b) => {
                    match stream.write(&b) {
                        Ok(size) => debug!("Response sent (size: {})", size),
                        Err(e) => error!("Error sending message: {:?}, {:?}", message, e),
                    }
                }
                Err(e) => panic!("Message encoding error! {:?}", e),
            }
        }
        Err(e) => panic!("Communication error! {:?}", e),
    };
}


trait ClientHandler where Self: Debug {
    fn handle(&mut self, shards: &Vec<Vec<SocketAddrV4>>);
}

/// An sbahn aware stream
impl ClientHandler for TcpStream {
    fn handle(&mut self, shards: &Vec<Vec<SocketAddrV4>>) {
        debug!("Starting listener stream: {:?}", self);
        handle_client(self, &shards);
    }
}

/// Listen on `address` for incoming client requests, and perform them on the appropriate shards.
pub fn listen(address: &SocketAddrV4, shards: &Vec<Vec<SocketAddrV4>>) -> Future<(), ()> {
    let address = address.to_owned();
    let shards = shards.clone();

    let read_timeout = Some(Duration::from_millis(300));
    let write_timeout = Some(Duration::from_millis(300));

    Future::spawn(move || {
        match TcpListener::bind(&address) {
            Ok(listener) => {
                // Accept connections and process them, spawning a new thread for each one.
                for stream in listener.incoming() {
                    let shards = shards.to_owned();
                    match stream {
                        Ok(stream) => {
                            let _ = stream.set_read_timeout(read_timeout);
                            let _ = stream.set_write_timeout(write_timeout);
                            thread::spawn(move || {
                                // connection succeeded
                                let mut stream = stream;
                                stream.handle(&shards);
                            });
                        }
                        Err(e) => error!("Connection failed!: {:?}", e),
                    }
                }
            }
            Err(e) => error!("Could not bind handler to {:?}: {:?}", address, e),
        }
    })
}
