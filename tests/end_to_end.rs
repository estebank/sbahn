extern crate sbahn;

use sbahn::client;
use sbahn::handler;
use sbahn::message;
use sbahn::message::*;
use sbahn::storage_node;
use std::thread;

#[test]
fn end_to_end() {
    let shards: Vec<Vec<String>> = vec![
        vec!["127.0.0.1:1024".to_string(), "127.0.0.1:1025".to_string(), "127.0.0.1:1026".to_string()],
        vec!["127.0.0.1:1027".to_string(), "127.0.0.1:1028".to_string(), "127.0.0.1:1029".to_string()],
        vec!["127.0.0.1:1030".to_string(), "127.0.0.1:1031".to_string(), "127.0.0.1:1032".to_string()],
    ];

    let e = &shards;
    let z = e.clone();
    thread::spawn(move || {
        let addr = "127.0.0.1:1100";
        let _shards = &z.to_owned();
        handler::listen(addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();
    for (pos, addresses) in x.enumerate() {
        for addr in addresses {
            let addr = addr.clone().to_owned();
            let shards_clone = &shards.clone();
            let shards = shards_clone.to_owned();
            thread::spawn(move || {
                let mut sn = storage_node::StorageNode::new(addr, pos, shards.len());
                &sn.listen();
            });
        }
    }

    {
        let target = "127.0.0.1:1100".to_string();
        let insert_key = message::Key {
            dataset: vec![1, 2, 3],
            pkey: vec![4, 5, 6],
            lkey: vec![7, 8, 9],
        };
        let client = client::Client { storage_nodes: vec![target] };
        {
            let content = message::Request {
                action: message::Action::Write {
                    key: insert_key.clone().to_owned(),
                    content: vec![1],
                },
                consistency: message::Consistency::Latest,
            };
            let r = client.send(content);
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
                    key: insert_key.clone().to_owned(),
                },
                consistency: message::Consistency::Latest,
            };
            let r = client.send(content);
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
                    key: insert_key.clone().to_owned(),
                },
                consistency: message::Consistency::Latest,
            };
            let r = client.send(content);
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
                    key: insert_key.clone().to_owned(),
                },
                consistency: message::Consistency::Latest,
            };
            let r = client.send(content);
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
