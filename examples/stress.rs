extern crate env_logger;
extern crate eventual;
extern crate sbahn;

use eventual::*;
use sbahn::client;
use sbahn::message::*;
use sbahn::message;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::thread;

fn main() {
    let _ = env_logger::init();
    thread::spawn(move || {
        let pos = 0;
        let addr = "127.0.0.1:1050".to_string();
        let mut sn: StorageNode<HashMapBackend>= StorageNode::new(addr, pos, 1);
        &sn.listen();
    });
    thread::sleep_ms(500);
    for i in 0..255 {
        let insert_key = message::Key {
            dataset: vec![1, 2, 3],
            pkey: vec![4, 5, 6],
            lkey: vec![7, 8, 9],
        };
        {
            let content = message::InternodeRequest::Write {
                key: insert_key.clone().to_owned(),
                value: Value::Value {
                    content: vec![i],
                    timestamp: 10000000,
                },
            };
            let r = client::Client::send_to_node("127.0.0.1:1050", &content).await().unwrap();
            match r {
                Ok(r) => match r {
                    InternodeResponse::WriteAck {key, timestamp} => {
                        assert_eq!(key, insert_key);
                        assert_eq!(timestamp, 10000000);
                    },
                    _ => assert!(false),
                },
                Err(e) => println!("####{:?}", e),
            }
        }
    }
}