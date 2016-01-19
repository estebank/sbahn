use std::result;
use std::hash::{Hash, SipHasher, Hasher};


pub type Buffer = Vec<u8>;


/// Client request.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub struct Request {
    pub action: Action,
    pub consistency: Consistency,
}

/// `Request`'s possible actions to be performed by a `handler`.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Action {
    /// Read the given `Key` and receive a `Response::Value`.
    Read {
        key: Key,
    },
    /// Write the given `Value` for `Key` and receive a `Response::WriteAck`.
    Write {
        key: Key,
        content: Buffer,
    },
    /// Delete the given `Key` and receive a `Response::WriteAck`.
    Delete {
        key: Key,
    },
}

/// A `Request`'s `Response` message envelope.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub struct ResponseMessage {
    pub message: Response,
    pub consistency: Consistency,
}

/// A `Request`'s response.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Response {
    /// An stored value, stored in the shard's `StorageNode`s, according to the
    /// required `Consistency`.
    Value {
        key: Key,
        value: Value,
    },
    /// The `Request`ed write for a `Value` has been stored in the `Key`'s
    /// shard's `StorageNode`s, according to the required `Consistency`.
    WriteAck {
        key: Key,
        timestamp: u64,
    },
    /// There was an error performing the operation on `Key`.
    Error {
        key: Key,
        message: String,
    },
}

/// The `Key` used to lookup a given `Value`.
#[derive(Debug, Hash, Clone, PartialEq, Eq, RustcEncodable, RustcDecodable)]
pub struct Key {
    pub dataset: Buffer,
    pub pkey: Buffer,
    pub lkey: Buffer,
}

impl Key {
    /// Return the ring hash for this `Key`.
    pub fn hash(&self) -> u64 {
        let mut s = SipHasher::new();
        &self.pkey.hash(&mut s);
        s.finish()
    }

    /// Return the corresponding shard for this key, given a `shard_count`
    /// amount of shards.
    pub fn shard(&self, shard_count: usize) -> usize {
        (self.hash() % (shard_count as u64)) as usize
    }
}

/// Any of the possible stored values on a `StorageNode`.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Value {
    /// There's no value stored for the given `Key` in the `StorageNode`s.
    None,
    /// The value stored for the given `Key` in the `StorageNode`s.
    Value {
        content: Buffer,
        timestamp: u64,
    },
    /// A deleted value for the given `Key` in the `StorageNode`s.
    Tombstone {
        timestamp: u64,
    },
}

/// Request operations performed by a `handler` to the `StorageNode`s.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum InternodeRequest {
    Read {
        key: Key,
    },
    Write {
        key: Key,
        value: Value,
    },
}

/// Request Response for a `handler` from a `StorageNode`.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum InternodeResponse {
    Value {
        key: Key,
        value: Value,
    },
    WriteAck {
        key: Key,
        timestamp: u64,
    },
    Error {
        key: Key,
        message: String,
    },
}

impl InternodeResponse {
    /// If the `Value` has a timestamp, return it.
    pub fn get_timestamp(&self) -> Option<u64> {
        match self {
            &InternodeResponse::Value {ref value, ..} => {
                match *value {
                    Value::None => None,
                    Value::Value {timestamp, ..} => Some(timestamp),
                    Value::Tombstone {timestamp} => Some(timestamp),
                }
            }
            &InternodeResponse::WriteAck {ref timestamp, ..} => Some(*timestamp),
            &InternodeResponse::Error {..} => None,
        }
    }

    /// Cast this `InternodeResponse` into a `handler` -> `Client` `Response`.
    pub fn to_response(self) -> Response {
        match self {
            InternodeResponse::Value {key, value} => {
                Response::Value {
                    key: key,
                    value: value,
                }
            }
            InternodeResponse::WriteAck {key, timestamp} => {
                Response::WriteAck {
                    key: key,
                    timestamp: timestamp,
                }
            }
            InternodeResponse::Error {key, message} => {
                Response::Error {
                    key: key,
                    message: message,
                }
            }
        }
    }
}

/// Consistency level for the `Request` or `Response`. Determines the conflict
/// resolution when `StorageNode`s for a given shard diverge and the persistence
/// assurance when writing.
#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Consistency {
    /// Only wait for one `StorageNode` to successfully reply before responding.
    One,
    /// Wait for all `StorageNode`s to reply and send a `Response` with the
    /// newest `Value`.
    Latest,
}

#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Error {
    /// Error when binary encoding a message.
    EncodeError,
    /// Error when decoding a binary into a message.
    DecodeError,
    /// Connection error.
    ConnectionError,
}

pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hash() {
        let key = Key {
            dataset: vec![1],
            pkey: vec![1],
            lkey: vec![1],
        };
        assert_eq!(8934463522374858327, key.hash());
        assert_eq!(0, key.shard(1 as usize));
    }
}
