extern crate sbahn;

use sbahn::handler;
use sbahn::storage_node;
use std::thread;

fn main() {
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
        handler::listen(addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();


    // Create SHARD_SIZE storage nodes.
    for (pos, addresses) in x.enumerate() {
        for addr in addresses {
            let addr = addr.clone().to_owned();
            let shards_clone = &shards.clone();
            let shards = shards_clone.to_owned();
            thread::spawn(move || {
                println!("Storage Node {:?} @ {:?}", &pos, &addr);
                let mut sn = storage_node::StorageNode::new(addr, pos);
                &sn.listen(&shards);
            });
        }
    }
    loop {}
}
