extern crate eventual;
extern crate sbahn;

use eventual::*;
use sbahn::client;
use sbahn::handler;
use sbahn::message::*;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::thread;
use std::time::Duration;

// Milis to wait before trying to connect to any node.
static DELAY: u64 = 100;

static mut PORT: u16 = 1100;
/// Obtain an open port
fn get_port() -> u16 {
    let mut port = 0;
    loop {
        unsafe {
          PORT += 1;
          port = PORT;
        }
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
        if TcpListener::bind(&addr).is_ok() {
            // Check wether the port is open, and only return it if it is.
            return port;
        }
    }
}

fn get_storage_node(pos: usize, shard_count: usize) -> SocketAddrV4 {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), get_port());
    let mut sn: StorageNode<HashMapBackend> = StorageNode::new(&addr, pos, shard_count);
    thread::spawn(move || {
        &sn.listen();
    });
    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening
    addr
}

fn setup_handler_node(shards: &Vec<Vec<SocketAddrV4>>) -> SocketAddrV4 {
    let shards = shards.clone();
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), get_port());
    thread::spawn(move || {
        let _ = handler::listen(&addr, &shards);
    });
    thread::sleep(Duration::from_millis(DELAY));  // Wait for handler node to start listening
    addr
}

/// Listen on `address` for incoming client requests, and do nothing.
pub fn dead_node(address: &SocketAddrV4) -> Future<(), ()> {
    let address = address.to_owned();

    let read_timeout = Some(Duration::from_millis(300));
    let write_timeout = Some(Duration::from_millis(300));

    Future::spawn(move || {
        match TcpListener::bind(&address) {
            Ok(listener) => {
                // Accept connections and process them, spawning a new thread for each one.
                for stream in listener.incoming() {
                    match stream {
                        Ok(stream) => {
                            let _ = stream.set_read_timeout(read_timeout);
                            let _ = stream.set_write_timeout(write_timeout);
                            thread::spawn(|| {
                                thread::sleep(Duration::from_millis(500));
                            });
                        }
                        Err(e) => panic!("Connection failed!: {:?}", e),
                    }
                }
            }
            Err(e) => panic!("Error while binding to {:?}: {:?}", address, e),
        }
    })
}

fn get_dead_storage_node() -> SocketAddrV4 {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), get_port());
    thread::spawn(move || {
        let _ = dead_node(&addr);
    });
    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening
    addr
}

fn setup_cluster() -> (SocketAddrV4, Vec<Vec<SocketAddrV4>>) {
    setup_bad_cluster(0)
}

fn setup_bad_cluster(bad_nodes: u32) -> (SocketAddrV4, Vec<Vec<SocketAddrV4>>) {
    let mut shards: Vec<Vec<SocketAddrV4>> = vec![];
    for i in 0..3 {
        let mut shard: Vec<SocketAddrV4> = vec![];
        for _ in 0..bad_nodes {
            shard.push(get_dead_storage_node());
        }
        for _ in bad_nodes..3 {
            shard.push(get_storage_node(i, 3));
        }
        shards.push(shard);
    }

    (setup_handler_node(&shards), shards)
}

fn write_to_storage_node(target: &SocketAddrV4, key: &Key, value: &Vec<u8>, timestamp: u64) {
    let request = InternodeRequest::Write {
        key: key.to_owned(),
        value: Value::Value {
            content: value.to_owned(),
            timestamp: timestamp,
        },
    };
    let r: Future<InternodeResponse, Error> = client::Client::send_to_node(target, &request);
    let _ = r.await();
}

fn key_and_value() -> (Key, Vec<u8>) {
    let key = Key {
        dataset: vec![1, 2, 3],
        pkey: vec![4, 5, 6],
        lkey: vec![7, 8, 9],
    };
    let value: Vec<u8> = vec![9, 8, 7];
    (key, value)
}

#[test]
fn read_what_you_insert() {
    let (handler_addr, _) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    let client = client::Client::new(vec![handler_addr]);
    // Should succeed
    let r = client.insert(&local_key, &local_value);
    let r = r.await().unwrap();
    match r.message {
        Response::WriteAck {key, ..} => assert_eq!(key, local_key),
        _ => assert!(false),
    }
    // Should succeed
    let r = client.get(&local_key);
    let r = r.await().unwrap();
    match r.message {
        Response::Value {key, value} => {
            assert_eq!(key, local_key);
            match value {
                Value::Value {content, ..} => assert_eq!(content, local_value),
                _ => assert!(false),
            }
        },
        _ => assert!(false),
    }
}

#[test]
fn read_consistency_one_all_nodes_available() {
    let (handler_addr, shards) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    for shard in shards {
        for node in shard {
            write_to_storage_node(&node, &local_key, &local_value, 100000);
        }
    }
    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::One);

    // Should succeed
    let r = client.get(&local_key);
    let r = r.await().unwrap();
    match r.message {
        Response::Value {key, value} => {
            assert_eq!(key, local_key);
            match value {
                Value::Value {content, ..} => assert_eq!(content, local_value),
                _ => assert!(false),
            }
        },
        _ => assert!(false),
    }
}

#[test]
fn read_consistency_one_one_node_available() {
    let (handler_addr, shards) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    // Write to only one node (local_key corresponds to shard 2).
    write_to_storage_node(&shards[2][0], &local_key, &local_value, 100000);

    thread::sleep(Duration::from_millis(DELAY*3));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::One);

    // Should succeed
    let r = client.get(&local_key);
    let r = r.await().unwrap();
    match r.message {
        Response::Value {key, value} => {
            assert_eq!(key, local_key);
            match value {
                Value::Value {content, ..} => assert_eq!(content, local_value),
                _ => assert!(false),
            }
        },
        _ => assert!(false),
    }
}

#[test]
fn read_consistency_latest_all_same() {
    // Should succeed
    let (handler_addr, shards) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    for shard in shards {
        for node in shard {
            write_to_storage_node(&node, &local_key, &local_value, 100000);
        }
    }
    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should succeed
    let r = client.get(&local_key);
    let r = r.await().unwrap();
    match r.message {
        Response::Value {key, value} => {
            assert_eq!(key, local_key);
            match value {
                Value::Value {content, ..} => assert_eq!(content, local_value),
                _ => assert!(false),
            }
        },
        _ => assert!(false),
    }
}

#[test]
fn read_consistency_latest_all_different() {
    // Should succeed
}

#[test]
fn read_consistency_latest_one_node_available() {
    let (handler_addr, shards) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    // Write to only one node (local_key corresponds to shard 2).
    write_to_storage_node(&shards[2][0], &local_key, &local_value, 100000);

    thread::sleep(Duration::from_millis(DELAY*3));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should fail
    match client.get(&local_key).await() {
        Ok(r) => {
            match r.message {
                Response::Error {key, ..} => {
                    assert_eq!(key, local_key);
                }
                _ => assert!(false),
            }
        }
        Err(_) => assert!(true),
    }
}

#[test]
fn write_consistency_one_all_available() {
    let (handler_addr, _) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should succeed
    let r = client.insert(&local_key, &local_value);
    let r = r.await().unwrap();
    match r.message {
        Response::WriteAck {key, timestamp} => {
            assert_eq!(key, local_key);
            assert!(timestamp > 0);
        },
        _ => assert!(false),
    }
}

#[test]
fn write_consistency_one_none_available() {
    let (handler_addr, _) = setup_bad_cluster(3);
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::One);

    // Should fail
    match client.insert(&local_key, &local_value).await() {
        Ok(r) => {
            match r.message {
                Response::Error {key, message} => {
                    assert_eq!(key, local_key);
                    assert_eq!("Quorum write could not be accomplished.".to_string(), message);
                },
                _ => assert!(false),
            }
        }
        Err(_) => assert!(true),
    }
}

#[test]
fn write_consistency_latest_all_available() {
    let (handler_addr, _) = setup_cluster();
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should succeed
    let r = client.insert(&local_key, &local_value);
    let r = r.await().unwrap();
    match r.message {
        Response::WriteAck {key, timestamp} => {
            assert_eq!(key, local_key);
            assert!(timestamp > 0);
        },
        _ => assert!(false),
    }
}

#[test]
fn write_consistency_latest_quorum_available() {
    let (handler_addr, _) = setup_bad_cluster(1);
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should succeed
    let r = client.insert(&local_key, &local_value);
    let r = r.await().unwrap();
    match r.message {
        Response::WriteAck {key, timestamp} => {
            assert_eq!(key, local_key);
            assert!(timestamp > 0);
        },
        _ => assert!(false),
    }
}

#[test]
fn write_consistency_latest_one_available() {
    let (handler_addr, _) = setup_bad_cluster(2);
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should fail
    match client.insert(&local_key, &local_value).await() {
        Ok(r) => {
            match r.message {
                Response::Error {key, message} => {
                    assert_eq!(local_key, key);
                    assert_eq!("Quorum write could not be accomplished.".to_string(), message);
                }
                _ => assert!(false),
            }
        }
        Err(_) => assert!(true),
    }
}

#[test]
fn write_consistency_latest_none_available() {
    let (handler_addr, _) = setup_bad_cluster(3);
    let (local_key, local_value) = key_and_value();

    thread::sleep(Duration::from_millis(DELAY));  // Wait for storage node to start listening

    let client = client::Client::with_consistency(vec![handler_addr], Consistency::Latest);

    // Should fail
    match client.insert(&local_key, &local_value).await() {
        Ok(r) => {
            match r.message {
                Response::Error {key, message} => {
                    assert_eq!(local_key, key);
                    assert_eq!("Quorum write could not be accomplished.".to_string(), message);
                }
                _ => assert!(false),
            }
        }
        Err(_) => assert!(true),
    }
}

#[test]
fn single_node() {
    let addr = get_storage_node(0, 1);
    let (insert_key, _) = key_and_value();

    {
        let content = InternodeRequest::Write {
            key: insert_key.to_owned(),
            value: Value::Value {
                content: vec![1],
                timestamp: 10000000,
            },
        };
        let addr = &addr.to_owned();
        let r: Future<InternodeResponse, Error> = client::Client::send_to_node(addr, &content);
        match r.await().unwrap() {
            InternodeResponse::WriteAck {key, timestamp} => {
                assert_eq!(key, insert_key);
                assert_eq!(timestamp, 10000000);
            },
            e => panic!("{:?}", e),
        }
    }
    {
        let content = InternodeRequest::Read {
            key: insert_key.to_owned(),
        };
        let addr = &addr.to_owned();
        let r: Future<InternodeResponse, Error> = client::Client::send_to_node(addr, &content);
        let r = r.await();
        match r {
            Ok(r) => match r {
                InternodeResponse::Value {key, value} => {
                    assert_eq!(key, insert_key);
                    match value {
                        Value::Value {content, timestamp} => {
                            assert_eq!(&content[..], &[1][..]);
                            assert_eq!(timestamp, 10000000);
                        },
                        _ => panic!(),
                    }
                },
                _ => panic!(),
            },
            Err(_) => panic!(),
        }
    }
}
