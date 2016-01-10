use std::io::prelude::*;
use std::result;
use std::net::TcpStream;
use constants::BUFFER_SIZE;
use message::{Error, Request, ResponseMessage, InternodeRequest, InternodeResponse};
use bincode::rustc_serialize::{encode, decode};
use bincode::SizeLimit;


pub struct Client {
    pub storage_nodes: Vec<String>,
}


pub type MessageResult = result::Result<ResponseMessage, Error>;
pub type InternodeResult = result::Result<InternodeResponse, Error>;


impl Client {
    pub fn send(&self, message: Request) -> MessageResult {
        let target = &self.storage_nodes[0];
        Self::send_to_node(target, &message)
    }

    pub fn send_to_node(target: &str, message: &Request) -> MessageResult {
        println!("    send_to_node: {:?}", message);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                match Self::send_buffer(target, &content) {
                    Ok(x) => {
                        match decode(&x) {
                            Ok(m) => Ok(m),
                            Err(_) => Err(Error::DecodeError),
                        }
                    }
                    Err(_) => Err(Error::DecodeError),
                }
            }
            Err(_) => Err(Error::EncodeError),
        }
    }

    pub fn send_internode(target: &str,
                          message: &InternodeRequest)
                          -> result::Result<InternodeResponse, Error> {
        println!("    send_to_node: {:?}", message);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                match Self::send_buffer(target, &content) {
                    Ok(x) => {
                        match decode(&x) {
                            Ok(m) => Ok(m),
                            Err(_) => Err(Error::DecodeError),
                        }
                    }
                    Err(_) => Err(Error::DecodeError),
                }
            }
            Err(_) => Err(Error::EncodeError),
        }
    }

    pub fn send_buffer(target: &str, message: &Vec<u8>) -> result::Result<Vec<u8>, Error> {
        println!("    {:?}", message);
        match TcpStream::connect(target) {
            Ok(stream) => {
                let mut stream = stream;
                match stream.write(&message) {
                    Err(_) => return Err(Error::ConnectionError),
                    _ => (),
                }
                let mut buf = [0; BUFFER_SIZE];
                match stream.read(&mut buf) {
                    Err(_) => return Err(Error::ConnectionError),
                    _ => (),
                }
                Ok(buf.iter().cloned().collect())
            }
            Err(e) => {
                println!("{:?}", e);
                Err(Error::ConnectionError)
            }
        }
    }
}
