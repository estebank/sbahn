extern crate sbahn;
use sbahn::client;
use sbahn::message;


fn main() {
    let target = "127.0.0.1:1035".to_string();
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
            value: message::Value::Unpersisted {
                content: vec![1],
            }
        },
        message::Action::Write {
            key: key.clone().to_owned(),
            value: message::Value::Unpersisted {
                content: vec![2],
            }
        },
        message::Action::Read {
            key: key.clone().to_owned(),
        },
        message::Action::Read {
            key: key2.clone().to_owned(),
        },
        message::Action::Write {
            key: key2.clone().to_owned(),
            value: message::Value::Unpersisted {
                content: vec![101],
            }
        },
        message::Action::Read {
            key: key2.clone().to_owned(),
        },
    ];

    let client = client::Client { storage_nodes: vec![target] };

    for m in messages {
        // let content: Vec<u8> = encode(&m, bincode::SizeLimit::Infinite).unwrap();
        let content = message::Message { action: m };

        let r = client.send(content);
        println!("Response: {:?}", r);
    }
}
