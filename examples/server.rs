extern crate sbahn;
#[macro_use]
extern crate log;
extern crate env_logger;

use sbahn::handler;
use sbahn::storage::HashMapBackend;
use sbahn::storage_node::StorageNode;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;

fn main() {
    let _ = env_logger::init();

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
        println!("Handler Node @ {:?}", &addr);
        let _ = handler::listen(&addr, &_shards);
    });

    let y = &shards.clone();
    let x = y.iter();


    // Create SHARD_SIZE storage nodes.
    for (pos, addresses) in x.enumerate() {
        for addr in addresses {
            let addr = addr.to_owned();
            let shard_count = shards.len();
            thread::spawn(move || {
                println!("Storage Node {:?} @ {:?}", &pos, &addr);
                let mut sn: StorageNode<HashMapBackend>= StorageNode::new(&addr, pos, shard_count);
                &sn.listen();
            });
        }
    }
    loop {}
}
