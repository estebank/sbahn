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


static mut port: u16 = 1200;
fn get_port() -> u16 {
    let mut p = 0;
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
    thread::sleep_ms(100);  // Wait for storage node to start listening
    addr
}

#[test]
fn single_node() {
    let addr = get_storage_node(0, 1);

    let insert_key = Key {
        dataset: vec![1, 2, 3],
        pkey: vec![4, 5, 6],
        lkey: vec![7, 8, 9],
    };
    {
        let content = InternodeRequest::Write {
            key: insert_key.to_owned(),
            value: Value::Value {
                content: vec![1],
                timestamp: 10000000,
            },
        };
        let addr = &addr.to_owned();
        let r: Future<Result<InternodeResponse>, ()> = client::Client::send_to_node(addr, &content);
        let r = r.await().unwrap();
        match r {
            Ok(r) => match r {
                InternodeResponse::WriteAck {key, timestamp} => {
                    assert_eq!(key, insert_key);
                    assert_eq!(timestamp, 10000000);
                },
                e => {println!("{:?}", e); assert!(false)},
            },
            Err(_) => assert!(false),
        }
    }
    {
        let content = InternodeRequest::Read {
            key: insert_key.to_owned(),
        };
        let addr = &addr.to_owned();
        let r: Future<Result<InternodeResponse>, ()> = client::Client::send_to_node(addr, &content);
        let r = r.await().unwrap();
        match r {
            Ok(r) => match r {
                InternodeResponse::Value {key, value} => {
                    assert_eq!(key, insert_key);
                    match value {
                        Value::Value {content, timestamp} => {
                            assert_eq!(&content[..], &[1][..]);
                            assert_eq!(timestamp, 10000000);
                        },
                        _ => assert!(false),
                    }
                },
                _ => assert!(false),
            },
            Err(_) => assert!(false),
        }
    }
}

#[test]
fn read_my_writes() {
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
    thread::sleep_ms(100);  // Wait for handler node to start listening

    {
        let insert_key = Key {
            dataset: vec![1, 2, 3],
            pkey: vec![4, 5, 6],
            lkey: vec![7, 8, 9],
        };
        let client = client::Client { handlers: vec![addr] };
        {
            let content = Request {
                action: Action::Write {
                    key: insert_key.to_owned(),
                    content: vec![1],
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content);
            let r = r.await().unwrap();
            match r {
                Ok(r) => match r.message {
                    Response::WriteAck {key, ..} => assert_eq!(key, insert_key),
                    _ => assert!(false),
                },
                Err(_) => assert!(false),
            }
        }
        {
            let content = Request {
                action: Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content);
            let r = r.await().unwrap();
            match r {
                Ok(r) => match r.message {
                    Response::Value {key, value} => {
                        assert_eq!(key, insert_key);
                        match value {
                            Value::Value {content, ..} => assert_eq!(&content[..], &vec![1][..]),
                            _ => assert!(false),
                        }
                    },
                    _ => assert!(false),
                },
                Err(_) => assert!(false),
            }
        }
        {
            let content = Request {
                action: Action::Delete {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content);
            let r = r.await().unwrap();
            match r {
                Ok(r) => match r.message {
                    Response::WriteAck {key, ..} => assert_eq!(key, insert_key),
                    _ => assert!(false),
                },
                Err(_) => assert!(false),
            }
        }
        {
            let content = Request {
                action: Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: Consistency::Latest,
            };
            let r = client.send(&content);
            let r = r.await().unwrap();
            match r {
                Ok(r) => match r.message {
                    Response::Value {key, value} => {
                        assert_eq!(key, insert_key);
                        match value {
                            Value::Tombstone {..} => assert!(true),
                            _ => assert!(false),
                        }
                    },
                    _ => assert!(false),
                },
                Err(_) => assert!(false),
            }
        }
    }
}
