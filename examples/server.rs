extern crate sbahn;
#[macro_use]
extern crate log;
extern crate env_logger;

use sbahn::handler;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::thread;

fn main() {
    let _ = env_logger::init();

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
        println!("Handler Node @ {:?}", &addr);
        let _ = handler::listen(addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();


    // Create SHARD_SIZE storage nodes.
    for (pos, addresses) in x.enumerate() {
        for addr in addresses {
            let addr = addr.clone().to_owned();
            let shard_count = shards.len();
            thread::spawn(move || {
                println!("Storage Node {:?} @ {:?}", &pos, &addr);
                let mut sn: StorageNode<HashMapBackend>= StorageNode::new(addr, pos, shard_count);
                &sn.listen();
            });
        }
    }
    loop {}
}
