extern crate sbahn;

use sbahn::constants::SHARD_SIZE;
use sbahn::handler;
use sbahn::storage_node;
use std::thread;

const PORT_START: usize = 1024;

fn main() {
    let mut shards: Vec<String> = vec![];

    for i in 0..SHARD_SIZE {
        let mut x = "127.0.0.1:".to_string();
        let port = (i + PORT_START).to_string();
        x.push_str(&port);
        let addr = x.clone();
        shards.push(addr);
    }
    let e = &shards;
    let z = e.clone();

    thread::spawn(move|| {
        let mut x = "127.0.0.1:".to_string();
        let port = (PORT_START + SHARD_SIZE + 1).to_string();
        x.push_str(&port);
        let addr = &*x.clone();
        let _shards = &z.to_owned();
        println!("Handler Node @ {:?}", addr);
        handler::listen(addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();


    //// Create SHARD_SIZE storage nodes.
    for (pos, addr) in x.enumerate() {
        let addr = addr.clone().to_owned();
        let shards_clone = &shards.clone();
        let shards = shards_clone.to_owned();
        thread::spawn(move|| {
            println!("Storage Node @ {:?}", &addr);
            let mut sn = storage_node::StorageNode::new(addr, pos);
            &sn.listen(&shards);
        });
    }
    loop{}
}
