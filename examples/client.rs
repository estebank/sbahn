extern crate env_logger;
extern crate eventual;
#[macro_use]
extern crate log;
extern crate sbahn;

use sbahn::client;
use sbahn::message;
use eventual::*;
use std::net::{Ipv4Addr, SocketAddrV4};


fn main() {
    let _ = env_logger::init();

    let target = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1100);
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
            key: key.to_owned(),
            content: vec![1, 2, 3],
        },
        message::Action::Write {
            key: key.to_owned(),
            content: vec![4, 5, 6],
        },
        message::Action::Read {
            key: key.to_owned(),
        },
        message::Action::Read {
            key: key2.to_owned(),
        },
        message::Action::Write {
            key: key2.to_owned(),
            content: vec![0; 2048],
        },
        message::Action::Read {
            key: key2.to_owned(),
        },
        message::Action::Delete {
            key: key2.to_owned(),
        },
        message::Action::Read {
            key: key2.to_owned(),
        },
    ];

    let client = client::Client::new(vec![target]);

    for m in messages {
        let content = message::Request {
            action: m,
            consistency: message::Consistency::One,
        };

        let r = client.send(&content).await().unwrap();
        println!("Response: {:?}", r);
    }
}
