extern crate sbahn;
extern crate eventual;
extern crate rand;

use eventual::*;
use sbahn::client;
use sbahn::handler;
use sbahn::message;
use sbahn::message::*;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use rand::random;


fn get_storage_node<'a>(shard_count: usize) -> SocketAddrV4 {
    let port: u8 = random();
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), (port as u16 + 1000));
    thread::spawn(move || {
        let pos = 0;
        let mut sn: StorageNode<HashMapBackend> = StorageNode::new(&addr, pos, shard_count);
        &sn.listen();
    });
    thread::sleep_ms(500);  // Wait for storage node to start listening
    addr
}


#[test]
fn single_node() {
    let addr = get_storage_node(1);

    let insert_key = message::Key {
        dataset: vec![1, 2, 3],
        pkey: vec![4, 5, 6],
        lkey: vec![7, 8, 9],
    };
    {
        let content = message::InternodeRequest::Write {
            key: insert_key.to_owned(),
            value: Value::Value {
                content: vec![1],
                timestamp: 10000000,
            },
        };
        let addr = &addr.to_owned();
        let r: Future<Result<message::InternodeResponse>, ()> = client::Client::send_to_node(addr, &content);
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
        let content = message::InternodeRequest::Read {
            key: insert_key.to_owned(),
        };
        let addr = &addr.to_owned();
        let r: Future<Result<message::InternodeResponse>, ()> = client::Client::send_to_node(addr, &content);
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
    let shards: Vec<Vec<SocketAddrV4>> = vec![
        vec![SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1024), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1025), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1026)],
        vec![SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1027), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1028), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1029)],
        vec![SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1030), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1031), SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1032)],
    ];

    let e = &shards;
    let z = e.clone();
    thread::spawn(move || {
        let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1100);
        let _shards = &z.to_owned();
        let _ = handler::listen(&addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();
    for (pos, addresses) in x.enumerate() {
        for addr in addresses {
            let addr = addr.to_owned();
            let shard_count = shards.len();
            thread::spawn(move || {
                let mut sn: StorageNode<HashMapBackend> = StorageNode::new(&addr, pos, shard_count);
                &sn.listen();
            });
        }
    }

    {
        let target = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1100);
        let insert_key = message::Key {
            dataset: vec![1, 2, 3],
            pkey: vec![4, 5, 6],
            lkey: vec![7, 8, 9],
        };
        let client = client::Client { handlers: vec![target] };
        {
            let content = message::Request {
                action: message::Action::Write {
                    key: insert_key.to_owned(),
                    content: vec![1],
                },
                consistency: message::Consistency::Latest,
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
            let content = message::Request {
                action: message::Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: message::Consistency::Latest,
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
            let content = message::Request {
                action: message::Action::Delete {
                    key: insert_key.to_owned(),
                },
                consistency: message::Consistency::Latest,
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
            let content = message::Request {
                action: message::Action::Read {
                    key: insert_key.to_owned(),
                },
                consistency: message::Consistency::Latest,
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
