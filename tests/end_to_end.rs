extern crate eventual;
extern crate sbahn;

use eventual::*;
use sbahn::client;
use sbahn::handler;
use sbahn::message::*;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::Duration;


static mut port: u16 = 1400;
fn get_port() -> u16 {
    let p;
    unsafe {
        port += 1;
        p = port;
    }
    p
}

fn get_storage_node<'a>(pos: usize, shard_count: usize) -> SocketAddrV4 {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), get_port());
    let mut sn: StorageNode<HashMapBackend> = StorageNode::new(&addr, pos, shard_count);
    thread::spawn(move || {
        &sn.listen();
    });
    thread::sleep(Duration::from_millis(100));  // Wait for storage node to start listening
    addr
}

#[test]
fn end_to_end() {
    let mut shards: Vec<Vec<SocketAddrV4>> = vec![];
    for i in 0..3 {
        let mut shard: Vec<SocketAddrV4> = vec![];
        for _ in 0..3 {
            shard.push(get_storage_node(i, 3));
        }
        shards.push(shard);
    }

    let e = &shards;
    let z = e.clone();
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), get_port());
    thread::spawn(move || {
        let _shards = &z.to_owned();
        let _ = handler::listen(&addr, &_shards);
    });
    thread::sleep(Duration::from_millis(100));  // Wait for handler node to start listening

    {
        let insert_key = Key {
            dataset: vec![1, 2, 3],
            pkey: vec![4, 5, 6],
            lkey: vec![7, 8, 9],
        };
        let client = client::Client::new(vec![addr]);
        {
            let content = Request {
                action: Action::Write {
                    key: insert_key.to_owned(),
                    content: vec![1],
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content).await().unwrap();
            match r.message {
                Response::WriteAck {key, ..} => assert_eq!(key, insert_key),
                _ => panic!(),
            }
        }
        {
            let content = Request {
                action: Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content).await().unwrap();
            match r.message {
                Response::Value {key, value} => {
                    assert_eq!(key, insert_key);
                    match value {
                        Value::Value {content, ..} => assert_eq!(&content[..], &vec![1][..]),
                        _ => panic!(),
                    }
                },
                _ => panic!(),
            }
        }
        {
            let content = Request {
                action: Action::Delete {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content).await().unwrap();
            match r.message {
                Response::WriteAck {key, ..} => assert_eq!(key, insert_key),
                _ => panic!(),
            }
        }
        {
            let content = Request {
                action: Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content).await().unwrap();
            match r.message {
                Response::Value {key, value} => {
                    assert_eq!(key, insert_key);
                    match value {
                        Value::Tombstone {..} => assert!(true),
                        _ => panic!(),
                    }
                },
                _ => panic!(),
            }
        }
    }
}
