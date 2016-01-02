use std::io::prelude::*;
use std::result;
use std::net::TcpStream;
use constants::BUFFER_SIZE;
use message::Message;
use bincode::rustc_serialize::{encode, decode};
use bincode::SizeLimit;


pub struct Client {
    pub storage_nodes: Vec<String>,
}


#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Error {
    EncodeError,
    DecodeError,
    ConnectionError,
}


pub type MessageResult = result::Result<Message, Error>;


impl Client {
    pub fn send(&self, message: Message) -> MessageResult {
        let target = &self.storage_nodes[0];
        Self::send_to_node(target, message)
    }

    pub fn send_to_node(target: &str, message: Message) -> MessageResult {
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                match TcpStream::connect(target) {
                    Ok(stream) => {
                        let mut stream = stream;
                        match stream.write(&content) {
                            Err(_) => return Err(Error::ConnectionError),
                            _ => (),
                        }
                        let mut buf = [0; BUFFER_SIZE];
                        match stream.read(&mut buf) {
                            Err(_) => return Err(Error::ConnectionError),
                            _ => (),
                        }
                        match decode(&buf) {
                            Ok(x) => Ok(x),
                            Err(_) => Err(Error::DecodeError),
                        }
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        Err(Error::ConnectionError)
                    }
                }
            }
            Err(_) => Err(Error::EncodeError),
        }
    }
}
