use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use client;
use constants::BUFFER_SIZE;
use eventual::*;
use message::*;
use std::fmt::Debug;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread;
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
fn read_one(key: &Key,
            responses: Vec<Future<Result<InternodeResponse>, ()>>)
            -> client::MessageResult {
    debug!("Reading one");

    let responses_future: Future<Vec<Result<InternodeResponse>>, ()> = sequence(responses)
                                                                           .filter(|response| {
                                                                               debug!("Future res\
                                                                                       ponse {:?}",
                                                                                      response);
                                                                               response.is_ok()
                                                                           })
                                                                           .collect();

    let error_message = ResponseMessage {
        message: Response::Error {
            key: key.to_owned(),
            message: "All the storage nodes replied with errors.".to_string(),
        },
        consistency: Consistency::One,
    };

    Ok(responses_future.await().unwrap().pop().map(|response|
        response.map(|m| ResponseMessage {
            message: m.to_response(),
            consistency: Consistency::One,
        }).unwrap_or(error_message.to_owned()))
        .unwrap_or(error_message.to_owned()))
}

/// Obtain the newest `Value` among those stored in this shard's `StorageNode`s.
fn read_latest(key: &Key,
               responses: Vec<Future<Result<InternodeResponse>, ()>>)
               -> client::MessageResult {
    debug!("Reading quorum");

    let latest: Option<InternodeResponse> = sequence(responses)
                                                .reduce((0, None), |last, response| {
                                                    let (max_timestamp, max_response) = last;
                                                    debug!("Max timestamp so far: {:?}",
                                                           max_timestamp);
                                                    debug!("Max max_response so far: {:?}",
                                                           max_response);
                                                    match response {
                                                        Ok(r) => {
                                                            let ts = r.get_timestamp().unwrap();
                                                            if ts >= max_timestamp {
                                                                (ts, Some(r.clone()))
                                                            } else {
                                                                (max_timestamp, max_response)
                                                            }
                                                        }
                                                        Err(_) => (max_timestamp, max_response),
                                                    }
                                                })
                                                .await()
                                                .unwrap()
                                                .1;

    debug!("Quorum read final response: {:?}", latest);

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

/// Read from all nodes for this `Key`'s shard, and use `consistency` to
/// collate the `StorageNode`'s responses.
fn read(shards: &Vec<String>, key: &Key, consistency: &Consistency) -> client::MessageResult {
    debug!("Read {:?} with {:?} consistency.", key, consistency);
    let mut responses: Vec<Future<Result<InternodeResponse>, ()>> = vec![];
    for shard in shards {
        let response = read_from_other_storage_node(&*shard, &key);
        responses.push(response);
    }
    match consistency {
        &Consistency::One => read_one(key, responses),
        &Consistency::Latest => read_latest(key, responses),
    }
}

/// Write to all nodes for this `Key`'s shard, and use `consistency` to
/// determine when to acknowledge the write to the client.
fn write(shards: &Vec<String>,
         key: &Key,
         value: &Value,
         consistency: &Consistency)
         -> client::MessageResult {
    let mut responses: Vec<Future<Result<InternodeResponse>, ()>> = vec![];
    for shard in shards {
        let response = write_to_other_storage_node(&*shard, &key, &value);
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
        let response_result = response_result.await().ok().unwrap();
        match response_result {
            Ok(response) => {
                match response {
                    InternodeResponse::WriteAck {key, timestamp} => {
                        write_count += 1;
                        if write_count >= (shards.len() / 2) + 1 {
                            debug!("Successfull write to mayority of shards for {:?}", key);
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


fn read_from_other_storage_node(target: &str, key: &Key) -> Future<Result<InternodeResponse>, ()> {
    debug!("Forwarding read request for {:?} to shard at {:?}.",
           key,
           target);
    let content = InternodeRequest::Read { key: key.to_owned() };
    client::Client::send_to_node(target, &content)
}

fn write_to_other_storage_node(target: &str,
                               key: &Key,
                               value: &Value)
                               -> Future<Result<InternodeResponse>, ()> {
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
pub fn handle_client(stream: &mut TcpStream, shards: &Vec<Vec<String>>) {
    let mut buf = [0; BUFFER_SIZE];
    &stream.read(&mut buf);

    let m: Request = match decode(&buf) {
        Ok(m) => m,
        Err(e) => panic!("Message decoding error! {:?}", e),
    };

    debug!("Message received: {:?}", m);

    let timestamp = get_now();
    let r = match m.action {
        Action::Read {key} => {
            let msg_shard = key.shard(shards.len());
            read(&shards[msg_shard], &key, &m.consistency)
        }
        Action::Write {key, content} => {
            let value = Value::Value {
                content: content.to_owned(),
                timestamp: timestamp,
            };
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &value, &m.consistency)
        }
        Action::Delete {key} => {
            let value = Value::Tombstone { timestamp: timestamp };
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &value, &m.consistency)
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
    fn handle(&mut self, shards: &Vec<Vec<String>>);
}

/// An sbahn aware stream
impl ClientHandler for TcpStream {
    fn handle(&mut self, shards: &Vec<Vec<String>>) {
        debug!("Starting listener stream: {:?}", self);
        handle_client(self, &shards);
    }
}

/// Listen on `address` for incoming client requests, and perform them on the appropriate shards.
pub fn listen(address: &str, shards: &Vec<Vec<String>>) -> Future<(), ()> {
    let address = address.to_owned();
    let shards = shards.clone();

    Future::spawn(move || {
        let listener = TcpListener::bind(&*address).unwrap();

        // Accept connections and process them, spawning a new thread for each one.
        for stream in listener.incoming() {
            let shards = shards.to_owned();
            match stream {
                Ok(stream) => {
                    thread::spawn(move || {
                        // connection succeeded
                        let mut stream = stream;
                        stream.handle(&shards);
                    });
                }
                Err(e) => error!("Connection failed!: {:?}", e),
            }
        }
        // close the socket server
        drop(listener);
    })
}
