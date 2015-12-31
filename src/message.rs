use std::result;
use std::slice::Iter;
use bincode::SizeLimit;
use bincode::rustc_serialize::{encode, decode};
use std::hash::{Hash, SipHasher, Hasher};


pub type Buffer = Vec<u8>;


#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub struct Message {
    pub action: Action
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
pub enum Value {
    None,
    Unpersisted {
        content: Buffer,
    },
    Value {
        content: Buffer,
        timestamp: u64,
    },
    Tombstone {
        timestamp: u64,
    },
}

impl Value {
    pub fn get_content(&self) -> Option<Buffer> {
        let s = self.clone().to_owned();
        match s {
            Value::Unpersisted { content } | Value::Value { content, ..} => Some(content),
            _ => None,
        }
    }

    pub fn get_timestamp(&self) -> Option<u64> {
        let s = self.clone().to_owned();
        match s {
            Value::Value { timestamp, ..} | Value::Tombstone { timestamp } => Some(timestamp),
            _ => None,
        }
    }
}


#[derive(Debug, Hash, Clone, PartialEq, RustcEncodable, RustcDecodable)]
pub enum Action {
    Read {
        key: Key,
    },
    Write {
        key: Key,
        value: Value,
    },
    Value {
        key: Key,
        value: Value,
    },
    WriteAck {
        key: Key,
        timestamp: u64,
    },
    Error,
 }


#[derive(Debug, Hash, Clone, PartialEq)]
pub enum Error {
    DecodingError,
    CommunicationError,
}


pub type Result<T> = result::Result<T, Error>;


impl Action {
    pub fn from_buffer(buffer: &Buffer) -> Result<Action> {
        match decode(&buffer) {
            Ok(m) => Ok(m),
            Err(_) => Err(Error::DecodingError),
        }
    }

    pub fn to_buffer(&self) -> Buffer {
        encode(&self, SizeLimit::Infinite).unwrap()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_wire_encoded() {
        let d = vec![1,2,3];
        let p = vec![4,5,6];
        let l = vec![7,8,9];
        let v = vec![100, 101];
        {
            let m = Action::Read {
                key: Key {
                    dataset: d.clone(),
                    pkey: p.clone(),
                    lkey: l.clone(),
                }
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
                Err(e) => panic!()
            };
            assert_eq!(decoded, m);
        }
    }
}
