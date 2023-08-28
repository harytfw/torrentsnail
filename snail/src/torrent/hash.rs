use crate::{Error, Result};
use serde::{de, ser};
use std::{
    convert,
    fmt::{Debug, Display},
    ops,
    sync::Arc, hash::Hash,
};

fn valid_byte(ch: u8) -> bool {
    matches!(ch, b'A'..=b'Z' | b'a' ..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'(' | b')' )
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HashId {
    V1([u8; 20]),
    V2([u8; 32]),
}

impl Default for HashId {
    fn default() -> Self {
        Self::ZERO_V1
    }
}

impl HashId {
    pub const ZERO_V1: HashId = Self::V1([0u8; 20]);
    pub const ZERO_V2: HashId = Self::V2([0u8; 32]);

    pub fn is_zero(&self) -> bool {
        self == &Self::ZERO_V1 || self == &Self::ZERO_V2
    }

    pub fn is_v1(&self) -> bool {
        matches!(self, Self::V1(_))
    }

    pub fn is_v2(&self) -> bool {
        matches!(self, Self::V2(_))
    }

    pub fn distance(&self, other: &HashId) -> HashId {
        if self.len() != other.len() {
            panic!("try to compare v1 with v2");
        }

        let mut res = *self;
        for i in 0..res.len() {
            res[i] ^= other[i];
        }
        res
    }

    pub fn hex(&self) -> String {
        hex::encode(self)
    }

    pub fn from_hex(s: impl AsRef<str>) -> Result<Self> {
        let data = hex::decode(s.as_ref()).map_err(|err| Error::InvalidInput(err.to_string()))?;
        Self::from_slice(data.as_slice())
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() == Self::ZERO_V1.len() {
            let id: [u8; 20] = slice.try_into().unwrap();
            Ok(Self::V1(id))
        } else if slice.len() == Self::ZERO_V2.len() {
            let id: [u8; 32] = slice.try_into().unwrap();
            Ok(Self::V2(id))
        } else {
            Err(Error::BytesToHashId)
        }
    }

    pub fn url_encoded(&self) -> String {
        let mut buf = String::new();
        for &b in self.as_ref() {
            if valid_byte(b) {
                buf.push(b as char);
            } else {
                buf.push_str(&format!("%{b:02x}"))
            }
        }
        buf
    }
}

impl Debug for HashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl Display for HashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

impl From<[u8; 20]> for HashId {
    fn from(value: [u8; 20]) -> Self {
        Self::V1(value)
    }
}

impl From<[u8; 32]> for HashId {
    fn from(value: [u8; 32]) -> Self {
        Self::V2(value)
    }
}

impl<'a, T> From<&'a T> for HashId
where
    HashId: From<T>,
    T: Clone,
{
    fn from(value: &'a T) -> Self {
        Self::from(value.clone())
    }
}

impl TryFrom<&[u8]> for HashId {
    type Error = Error;
    fn try_from(b: &[u8]) -> std::result::Result<Self, Self::Error> {
        HashId::from_slice(b)
    }
}

impl ops::Deref for HashId {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        match self {
            Self::V1(v1) => v1,
            Self::V2(v2) => v2,
        }
    }
}

impl ops::DerefMut for HashId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::V1(v1) => v1,
            Self::V2(v2) => v2,
        }
    }
}

impl convert::AsRef<[u8]> for HashId {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq<Arc<HashId>> for HashId {
    fn eq(&self, other: &Arc<HashId>) -> bool {
        self.eq(other.as_ref())
    }
}

impl PartialEq<HashId> for Arc<HashId> {
    fn eq(&self, other: &HashId) -> bool {
        other.eq(self)
    }
}

impl<'de> de::Deserialize<'de> for HashId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IdVisitor;
        impl<'de> de::Visitor<'de> for IdVisitor {
            type Value = HashId;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "Hash Id")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let id = HashId::from_slice(v).map_err(|e| de::Error::custom(e.to_string()))?;
                Ok(id)
            }

            fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_bytes(v.as_bytes())
            }
        }

        deserializer.deserialize_bytes(IdVisitor)
    }
}

impl ser::Serialize for HashId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self)
    }
}

pub type ArcHashId = Arc<HashId>;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    pub fn primitive_eq_rc() {
        let a = HashId::ZERO_V1;
        let b = Arc::new(HashId::ZERO_V1);
        let _ = a.eq(&b);
    }

    #[test]
    pub fn hash_id() {
        assert!(HashId::ZERO_V1.is_v1());
        assert!(HashId::ZERO_V2.is_v2());

        let v1 = HashId::from_hex("08ada5a7a6183aae1e09d831df6748d566095a10").unwrap();
        let v2 =
            HashId::from_hex("d66919d15e1d90ead86302c9a1ee9ef73b446be261d65b8d8d78c589ae04cdc0")
                .unwrap();
        assert!(v1.is_v1());
        assert!(v2.is_v2());

        assert!(v1 == v1);
    }

    #[test]
    pub fn url_encoded() {
        {
            let id = HashId::ZERO_V1;
            let s = id.url_encoded();
            assert_eq!(s.len(), 60);
        }
        {
            let id = HashId::from_hex("274e6a57eae79b2ba5bb8caf28cf847a12a65ed9").unwrap();
            assert_eq!(
                id.url_encoded(),
                "%27NjW%ea%e7%9b%2b%a5%bb%8c%af(%cf%84z%12%a6%5e%d9"
            )
        }
    }
}
