use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use client;
use constants::BUFFER_SIZE;
use message::*;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::result;
use std::thread;
use time;


fn get_now() -> u64 {
    let now = time::get_time();
    let sec = now.sec;
    let nsec = now.nsec;
    // Consider using the Timespec directly instead.
    ((sec as u64) * 1_000_000) + (nsec as u64 / 1000)
}

fn read_one(responses: &mut Vec<InternodeResponse>) -> client::MessageResult {
    debug!("Reading one");
    let message = responses.pop().unwrap().to_response();
    let r = ResponseMessage {
        message: message,
        consistency: Consistency::One,
    };
    Ok(r)
}

fn read_latest(key: &Key, responses: &mut Vec<InternodeResponse>) -> client::MessageResult {
    debug!("Reading quorum");

    let mut latest: Option<InternodeResponse> = None;

    for response in responses {
        debug!("Latest response so far: {:?}", latest);
        debug!("Internode response: {:?}", response);
        match response.get_timestamp() {
            None => (),
            Some(t) => {
                let latest_timestamp = match latest {
                    None => Some(0),
                    Some(ref l) => l.get_timestamp(),
                };
                if Some(t) > latest_timestamp {
                    latest = Some(response.clone().to_owned());
                };
            }
        }
    }

    let message = match latest {
        Some(ref latest) => latest.clone().to_owned().to_response(),
        None => {
            Response::Value {
                key: key.clone().to_owned(),
                value: Value::None,
            }
        }
    };
    let r = ResponseMessage {
        message: message,
        consistency: Consistency::Latest,
    };
    Ok(r)
}

fn read(shards: &Vec<String>, key: &Key, consistency: &Consistency) -> client::MessageResult {
    let mut responses: Vec<InternodeResponse> = vec![];
    for shard in shards {
        let response = read_from_other_storage_node(&*shard, &key);
        match response {
            Ok(response) => responses.push(response),
            Err(_) => (),
        }
    }
    match consistency {
        &Consistency::One => read_one(&mut responses),
        &Consistency::Latest => read_latest(key, &mut responses),
    }
}

fn write(shards: &Vec<String>,
         key: &Key,
         value: &Value,
         consistency: &Consistency)
         -> client::MessageResult {
    let mut responses: Vec<result::Result<InternodeResponse, Error>> = vec![];
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
        key: key.clone().to_owned(),
        message: "Quorum write could not be accomplished.".to_string(),
    };

    let mut write_count = 0;
    for response_result in responses {
        match response_result {
            Ok(response) => {
                match response {
                    InternodeResponse::WriteAck {key, timestamp} => {
                        write_count += 1;
                        if write_count >= shards.len() / 2 {
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
        consistency: consistency.clone().to_owned(),
    };

    Ok(r)
}


fn read_from_other_storage_node(target: &str,
                                key: &Key)
                                -> result::Result<InternodeResponse, Error> {
    debug!("Forwarding read request for {:?} to shard at {:?}.",
           key,
           target);
    let content = InternodeRequest::Read { key: key.to_owned() };
    client::Client::send_internode(target, &content)
}

fn write_to_other_storage_node(target: &str,
                               key: &Key,
                               value: &Value)
                               -> result::Result<InternodeResponse, Error> {
    debug!("Forwarding write request for {:?} to shard at {:?}.",
           key,
           target);
    let request = InternodeRequest::Write {
        key: key.clone().to_owned(),
        value: value.clone().to_owned(),
    };
    client::Client::send_internode(target, &request)
}


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
        Action::Read { key } => {
            let msg_shard = key.shard(shards.len());
            read(&shards[msg_shard], &key, &m.consistency)
        }
        Action::Write {key, content} => {
            let value = Value::Value {
                content: content.clone().to_owned(),
                timestamp: timestamp,
            };
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &value, &m.consistency)
        }
        Action::Delete {key} => {
            let value = Value::Tombstone {
                timestamp: timestamp,
            };
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


pub fn listen(address: &str, shards: &Vec<Vec<String>>) {
    let listener = TcpListener::bind(address).unwrap();

    // Accept connections and process them, spawning a new thread for each one.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                debug!("Starting listener stream: {:?}", stream);
                let shards = shards.clone();
                thread::spawn(move || {
                    // connection succeeded
                    let mut stream = stream;
                    handle_client(&mut stream, &shards);
                });
            }
            Err(e) => {
                error!("Connection failed!: {:?}", e);
            }
        }
    }
    // close the socket server
    drop(listener);
}
