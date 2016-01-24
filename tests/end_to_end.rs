extern crate eventual;
extern crate sbahn;

use eventual::*;
use sbahn::client;
use sbahn::handler;
use sbahn::message::*;
use sbahn::message;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;

#[test]
fn end_to_end() {
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
                let mut sn: StorageNode<HashMapBackend>= StorageNode::new(&addr, pos, shard_count);
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
            let r = client.send(&content).await().unwrap();
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
            let r = client.send(&content).await().unwrap();
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
            let r = client.send(&content).await().unwrap();
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
            let r = client.send(&content).await().unwrap();
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
