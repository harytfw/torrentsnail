use super::{Error, Result};
use serde::{de, ser};
use std::{
    fmt::{Debug, Display},
    ops, convert,
};

fn valid_byte(ch: u8) -> bool {
    matches!(ch, b'A'..=b'Z' | b'a' ..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'(' | b')' )
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HashId([u8; 20]);

impl HashId {
    pub fn is_zero(&self) -> bool {
        self == &Self::zero()
    }

    pub const fn zero() -> Self {
        Self([0u8; 20])
    }

    pub fn is_same(&self, other: &Self) -> bool {
        self == other
    }

    pub fn distance(&self, other: &HashId) -> HashId {
        let mut res = HashId::zero();
        for i in 0..res.len() {
            res[i] = self[i] ^ other[i];
        }
        res
    }

    pub fn hex(&self) -> String {
        hex::encode(self)
    }

    pub fn from_hex(s: impl AsRef<str>) -> Result<Self> {
        let data = hex::decode(s.as_ref()).map_err(|err| Error::InvalidInput(err.to_string()))?;
        Self::try_from(data.as_slice()).map_err(|err| Error::InvalidInput(err.to_string()))
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self> {
        let id: [u8; 20] = slice.try_into().map_err(|_| Error::BytesToHashId)?;
        Ok(Self(id))
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
        Self(value)
    }
}

impl From<&[u8; 20]> for HashId {
    fn from(value: &[u8; 20]) -> Self {
        Self(value.to_owned())
    }
}

impl TryFrom<&[u8]> for HashId {
    type Error = Error;
    fn try_from(b: &[u8]) -> std::result::Result<Self, Self::Error> {
        HashId::from_slice(b)
    }
}

impl ops::Deref for HashId {
    type Target = [u8; 20];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for HashId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl convert::AsRef<[u8]> for HashId {
    fn as_ref(&self) -> &[u8] {
        &self.0
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
                write!(formatter, "Hash Id with 20 bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let id = HashId::try_from(v).map_err(|e| de::Error::custom(e.to_string()))?;
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
        serializer.serialize_bytes(self.as_ref())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn url_encoded() {
        {
            let id = HashId::zero();
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
