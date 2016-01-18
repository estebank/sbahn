extern crate sbahn;
extern crate eventual;
#[macro_use]
extern crate log;
extern crate env_logger;

use sbahn::client;
use sbahn::message;
use eventual::*;


fn main() {
    let _ = env_logger::init();

    let target = "127.0.0.1:1100".to_string();
    let key = message::Key {
        dataset: vec![1, 2, 3],
        pkey: vec![4, 5, 6],
        lkey: vec![7, 8, 9],
    };
    let key2 = message::Key {
        dataset: vec![1, 2, 3],
        pkey: vec![4, 5, 0],
        lkey: vec![7, 8, 9],
    };
    let messages = vec![
        message::Action::Write {
            key: key.clone().to_owned(),
            content: vec![1],
        },
        message::Action::Write {
            key: key.clone().to_owned(),
            content: vec![2],
        },
        message::Action::Read {
            key: key.clone().to_owned(),
        },
        message::Action::Read {
            key: key2.clone().to_owned(),
        },
        message::Action::Write {
            key: key2.clone().to_owned(),
            content: vec![101],
        },
        message::Action::Read {
            key: key2.clone().to_owned(),
        },
        message::Action::Delete {
            key: key.clone().to_owned(),
        },
        message::Action::Read {
            key: key2.clone().to_owned(),
        },
    ];

    let client = client::Client { storage_nodes: vec![target] };

    for m in messages {
        let content = message::Request {
            action: m,
            consistency: message::Consistency::Latest,
        };

        let r = client.send(content).await().unwrap();
        println!("Response: {:?}", r);
    }
}
