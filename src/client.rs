use std::fmt::Debug;
use std::io::prelude::*;
use std::net::{SocketAddrV4, TcpStream};
use std::time::Duration;
use eventual::*;
use message::{Action, Buffer, Consistency, Error, Key, Request, Result, ResponseMessage};
use bincode::rustc_serialize::{encode, decode};
use rustc_serialize::{Encodable, Decodable};
use bincode::SizeLimit;

/// An sbahn client.
pub struct Client {
    /// List of addresses to frontend request handlers
    pub handlers: Vec<SocketAddrV4>,
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub consistency: Consistency,
}

pub type MessageResult = Result<ResponseMessage>;

impl Client {
    pub fn new(handlers: Vec<SocketAddrV4>) -> Client {
        Client {
            handlers: handlers,
            read_timeout: Some(Duration::from_millis(300)),
            write_timeout: Some(Duration::from_millis(300)),
            consistency: Consistency::Latest,
        }
    }

    pub fn with_timeouts(handlers: Vec<SocketAddrV4>,
                         read_timeout: Duration,
                         write_timeout: Duration)
                         -> Client {
        Client {
            handlers: handlers,
            read_timeout: Some(read_timeout),
            write_timeout: Some(write_timeout),
            consistency: Consistency::Latest,
        }
    }

    pub fn with_consistency(handlers: Vec<SocketAddrV4>, consistency: Consistency) -> Client {
        Client {
            handlers: handlers,
            read_timeout: Some(Duration::from_millis(300)),
            write_timeout: Some(Duration::from_millis(300)),
            consistency: consistency,
        }
    }

    pub fn insert(&self, key: &Key, value: &Buffer) -> Future<ResponseMessage, Error> {
        let content = Request {
            action: Action::Write {
                key: key.to_owned(),
                content: value.to_owned(),
            },
            consistency: self.consistency.clone(),
        };
        self.send(&content)
    }

    pub fn get(&self, key: &Key) -> Future<ResponseMessage, Error> {
        let content = Request {
            action: Action::Read { key: key.to_owned() },
            consistency: self.consistency.clone(),
        };
        self.send(&content)
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
        Self::send_to_node_with_timeout(target, message, None)
    }

    /// Sends a message that can be binary encoded to the Storage Node at `target`.
    pub fn send_to_node_with_timeout<T, K>(target: &SocketAddrV4,
                                           message: &T,
                                           timeout: Option<Duration>)
                                           -> Future<K, Error>
        where T: Debug + Encodable,
              K: Debug + Decodable + Send
    {
        debug!("sending message {:?} to node {:?}", message, target);
        match encode(&message, SizeLimit::Infinite) {
            Ok(content) => {
                Self::send_buffer(target, content, timeout).and_then(|x| {
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
    pub fn send_buffer(target: &SocketAddrV4,
                       message: Vec<u8>,
                       timeout: Option<Duration>)
                       -> Future<Vec<u8>, Error> {
        let target = target.to_owned();
        Future::lazy(move || {
            match TcpStream::connect(target) {
                Ok(stream) => {
                    let _ = stream.set_read_timeout(timeout);
                    let _ = stream.set_write_timeout(timeout);

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

    pub fn set_timeouts(&mut self, timeout: Duration) {
        self.read_timeout = Some(timeout);
        self.write_timeout = Some(timeout);
    }
}
