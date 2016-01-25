use std::fmt::Debug;
use std::io::prelude::*;
use std::net::{SocketAddrV4, TcpStream};
use eventual::*;
use message::{Buffer, Error, Request, Result, ResponseMessage};
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};
use bincode::SizeLimit;

/// An sbahn client.
pub struct Client {
    /// List of addresses to frontend request handlers
    pub handlers: Vec<SocketAddrV4>,
}

pub type MessageResult = Result<ResponseMessage>;

impl Client {
    pub fn new(handlers: Vec<SocketAddrV4>) -> Client {
        Client { handlers: handlers }
    }

    pub fn send(&self, message: &Request) -> Future<ResponseMessage, Error> {
        let target = &self.handlers[0];
        Self::send_to_node(target, &message)
    }

    /// Sends a message that can be binary encoded to the Storage Node at `target`.
    pub fn send_to_node<T, K>(target: &SocketAddrV4, message: &T) -> Future<K, Error>
        where T: Debug + Encodable,
              K: Debug + Decodable + Send
    {

        debug!("sending message {:?} to node {:?}", message, target);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                Self::send_buffer(target, content).and_then(|x| {
                  match decode(&x) {
                      Ok(m) => Ok(m),
                      Err(_) => Err(Error::DecodeError),
                  }
                })
            }
            Err(_) => Future::error(Error::EncodeError),
        }
    }

    /// Sends a binary encoded message to the Storage Node at `target`.
    pub fn send_buffer(target: &SocketAddrV4, message: Vec<u8>) -> Future<Vec<u8>, Error> {
        let target = target.to_owned();
        Future::lazy(move || {
            match TcpStream::connect(target) {
                Ok(stream) => {
                    let mut stream = stream;
                    if stream.write(&message).is_err() {
                        return Err(Error::ConnectionError);
                    }
                    let mut val: Buffer = vec![];
                    if stream.read_to_end(&mut val).is_err() {
                        return Err(Error::ConnectionError);
                    }
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
