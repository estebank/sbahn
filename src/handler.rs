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
use std::thread;


fn read_from_other_storage_node(target: &str, key: &Key) -> client::MessageResult {
    println!("### Forwarding to shard at {:?}.", target);
    let content = Message { action: Action::Read { key: key.to_owned() } };
    client::Client::send_to_node(target, content)
}

fn write_to_other_storage_node(target: &str, key: &Key, value: &Value) -> client::MessageResult {
    println!("### Forwarding to shard at {:?}.", target);
    let content = Message {
        action: Action::Write {
            key: key.clone().to_owned(),
            value: value.clone().to_owned(),
        },
    };
    client::Client::send_to_node(target, content)
}


pub fn handle_client(stream: &mut TcpStream, shards: &Vec<String>) {
    let mut buf = [0; BUFFER_SIZE];
    &stream.read(&mut buf);

    let m = match decode(&buf) {
        Ok(m) => m,
        Err(_) => panic!("Mesage decoding error!"),
    };

    println!("### Message received: {:?}", m);

    let r = match m {
        Action::Read { key } => {
            let msg_shard = key.shard(SHARD_SIZE);
            read_from_other_storage_node(&*shards[msg_shard], &key)
        }
        Action::Write {key, value} => {
            let msg_shard = key.shard(SHARD_SIZE);
            write_to_other_storage_node(&*shards[msg_shard], &key, &value)
        }
        _ => panic!("Invalid operation!"),

    };
    match r {
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


pub fn listen(address: &str, shards: &Vec<String>) {
    let listener = TcpListener::bind(address).unwrap();

    // accept connections and process them, spawning a new thread for each one
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // println!("### Starting listener stream: {:?}", stream);
                let shards = shards.clone();
                thread::spawn(move || {
                    // connection succeeded
                    let mut stream = stream;
                    handle_client(&mut stream, &shards);
                });
            }
            Err(e) => {
                println!("### connection failed!: {:?}", e);
            }
        }
    }
    // close the socket server
    drop(listener);
}
