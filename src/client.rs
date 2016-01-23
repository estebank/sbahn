use std::fmt::Debug;
use std::io::prelude::*;
use std::net::TcpStream;
use constants::BUFFER_SIZE;
use eventual::*;
use message::{Error, Request, Result, ResponseMessage};
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};
use bincode::SizeLimit;

/// An sbahn client.
pub struct Client {
    /// List of addresses to frontend request handlers
    pub handlers: Vec<String>,
}

pub type MessageResult = Result<ResponseMessage>;

impl Client {
    pub fn new(handlers: Vec<String>) -> Client {
        Client { handlers: handlers }
    }

    pub fn send(&self, message: &Request) -> Future<MessageResult, ()> {
        let target = &self.handlers[0];
        Self::send_to_node(target, &message)
    }

    /// Sends a message that can be binary encoded to the Storage Node at `target`.
    pub fn send_to_node<T, K>(target: &str, message: &T) -> Future<Result<K>, ()>
        where T: Debug + Encodable,
              K: Debug + Decodable + Send
    {

        debug!("sending message {:?} to node {:?}", message, target);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                Self::send_buffer(target, content).map(|buffer| {
                    match buffer {
                        Ok(x) => {
                            match decode(&x) {
                                Ok(m) => Ok(m),
                                Err(_) => Err(Error::DecodeError),
                            }
                        }
                        Err(_) => Err(Error::DecodeError),
                    }
                })
            }
            Err(_) => Future::of(Err(Error::EncodeError)),
        }
    }

    /// Sends a binary encoded message to the Storage Node at `target`.
    pub fn send_buffer(target: &str, message: Vec<u8>) -> Future<Result<Vec<u8>>, ()> {
        let target = target.to_owned();
        Future::spawn(move || {
            match TcpStream::connect(&*target) {
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
                    let val = buf.iter().cloned().collect();
                    debug!("Response from {:?}: {:?}", target, val);
                    Ok(val)
                }
                Err(e) => {
                    error!("{:?}", e);
                    Err(Error::ConnectionError)
                }
            }
        })
    }
}
