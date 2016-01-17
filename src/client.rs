use std::io::prelude::*;
use std::net::TcpStream;
use constants::BUFFER_SIZE;
use message::{Error, Request, Result, ResponseMessage, InternodeRequest, InternodeResponse};
use bincode::rustc_serialize::{encode, decode};
use bincode::SizeLimit;


pub struct Client {
    pub storage_nodes: Vec<String>,
}


pub type MessageResult = Result<ResponseMessage>;
pub type InternodeResult = Result<InternodeResponse>;


impl Client {
    pub fn send(&self, message: Request) -> MessageResult {
        let target = &self.storage_nodes[0];
        Self::send_to_node(target, &message)
    }

    pub fn send_to_node(target: &str, message: &Request) -> MessageResult {
        debug!("sending message {:?} to node {:?}", message, target);
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
                          -> Result<InternodeResponse> {
        debug!("Sending internode message: {:?}", message);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                match Self::send_buffer(target, &content) {
                    Ok(x) => {
                        match decode(&x) {
                            Ok(m) => Ok(m),
                            Err(_) => Err(Error::DecodeError),
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(_) => Err(Error::EncodeError),
        }
    }

    pub fn send_buffer(target: &str, message: &Vec<u8>) -> Result<Vec<u8>> {
        debug!("Buffer being sent: {:?}", message);
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
                error!("{:?}", e);
                Err(Error::ConnectionError)
            }
        }
    }
}
