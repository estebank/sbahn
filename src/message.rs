use std::result;
use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use std::hash::{Hash, SipHasher, Hasher};


pub type Buffer = Vec<u8>;


#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub struct Request {
    pub action: Action,
    pub consistency: Consistency,
}

#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub struct ResponseMessage {
    pub message: Response,
    pub consistency: Consistency,
}

#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Response {
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

#[derive(Debug, Hash, Clone, PartialEq, Eq, RustcEncodable, RustcDecodable)]
pub struct Key {
    pub dataset: Buffer,
    pub pkey: Buffer,
    pub lkey: Buffer,
}

impl Key {
    pub fn hash(&self) -> u64 {
        let mut s = SipHasher::new();
        &self.pkey.hash(&mut s);
        s.finish()
    }

    pub fn shard(&self, shard_count: usize) -> usize {
        (self.hash() % (shard_count as u64)) as usize
    }
}

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

#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Value {
    None,
    Value {
        content: Buffer,
        timestamp: u64,
    },
    Tombstone {
        timestamp: u64,
    },
}

#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Action {
    Read {
        key: Key,
    },
    Write {
        key: Key,
        content: Buffer,
    },
    Delete {
        key: Key,
    }
}


#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Consistency {
    One,
    Latest,
}


#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Error {
    EncodeError,
    DecodeError,
    ConnectionError,
}

pub type Result<T> = result::Result<T, Error>;


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_wire_encoded() {
        let d = vec![1, 2, 3];
        let p = vec![4, 5, 6];
        let l = vec![7, 8, 9];
        let v = vec![100, 101];
        {
            let m = Action::Read {
                key: Key {
                    dataset: d.clone(),
                    pkey: p.clone(),
                    lkey: l.clone(),
                },
            };

            let encoded = &m.to_buffer();

            let decoded = match Action::from_buffer(encoded) {
                Ok(m) => m,
                Err(e) => panic!(),
            };
            assert_eq!(decoded, m);
        }
        {
            let m = Action::Write {
                key: Key {
                    dataset: d.clone(),
                    pkey: p.clone(),
                    lkey: l.clone(),
                },
                value: Value::Unpersisted(v.clone()),
            };
            let encoded = &m.to_buffer();

            let decoded = match Action::from_buffer(encoded) {
                Ok(m) => m,
                Err(e) => panic!(),
            };
            assert_eq!(decoded, m);
        }
    }
}
