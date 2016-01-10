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
    // Consider using a Timespec instead.
    ((sec as u64) * 1_000_000) + (nsec as u64 / 1000)
}

fn read_quorum(responses: &mut Vec<result::Result<InternodeResponse, Error>>)
               -> client::MessageResult {
    debug!("Reading quorum");

    let message = responses.pop().unwrap().ok().unwrap().to_response();

    let r = ResponseMessage {
        message: message,
        consistency: Consistency::Quorum,
    };
    Ok(r)
}

fn read(shards: &Vec<String>, key: &Key, consistency: &Consistency) -> client::MessageResult {
    let mut responses: Vec<result::Result<InternodeResponse, Error>> = vec![];
    for shard in shards {
        let response = read_from_other_storage_node(&*shard, &key);
        responses.push(response);
    }
    match consistency {
        &Consistency::One => read_quorum(&mut responses),
        &Consistency::Quorum => read_quorum(&mut responses),
        &Consistency::Latest => read_quorum(&mut responses),
        &Consistency::All => read_quorum(&mut responses),
    }
}


fn write(shards: &Vec<String>,
         key: &Key,
         content: &Buffer,
         consistency: &Consistency)
         -> client::MessageResult {
    let mut responses: Vec<result::Result<InternodeResponse, Error>> = vec![];
    for shard in shards {
        let response = write_to_other_storage_node(&*shard, &key, &content);
        responses.push(response);
    }
    let message = responses.pop().unwrap().ok().unwrap().to_response();

    let r = ResponseMessage {
        message: message,
        consistency: consistency.clone().to_owned(),
    };
    Ok(r)
}


fn read_from_other_storage_node(target: &str,
                                key: &Key)
                                -> result::Result<InternodeResponse, Error> {
    debug!("Forwarding read request for {:?} to shard at {:?}.", key, target);
    let content = InternodeRequest::Read { key: key.to_owned() };
    client::Client::send_internode(target, &content)
}

fn write_to_other_storage_node(target: &str,
                               key: &Key,
                               content: &Buffer)
                               -> result::Result<InternodeResponse, Error> {
    debug!("Forwarding write request for {:?} to shard at {:?}.", key, target);
    let request = InternodeRequest::Write {
        key: key.clone().to_owned(),
        value: Value::Value {
            content: content.clone().to_owned(),
            timestamp: get_now(),
        },
    };
    client::Client::send_internode(target, &request)
}


pub fn handle_client(stream: &mut TcpStream, shards: &Vec<Vec<String>>) {
    let mut buf = [0; BUFFER_SIZE];
    &stream.read(&mut buf);

    let m: Request = match decode(&buf) {
        Ok(m) => m,
        Err(_) => panic!("Message decoding error!"),
    };

    debug!("Message received: {:?}", m);

    let r = match m.action {
        Action::Read { key } => {
            let msg_shard = key.shard(shards.len());
            read(&shards[msg_shard], &key, &m.consistency)
        }
        Action::Write {key, content} => {
            let msg_shard = key.shard(shards.len());
            write(&shards[msg_shard], &key, &content, &m.consistency)
        }
    };
    let _ = match r {
        Ok(message) => {
            let encoded = encode(&message, SizeLimit::Infinite);
            match encoded {
                Ok(b) => stream.write(&b),
                Err(_) => panic!("encoding error!"),
            }
        }
        Err(_) => panic!("error!"),
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
                error!("connection failed!: {:?}", e);
            }
        }
    }
    // close the socket server
    drop(listener);
}
