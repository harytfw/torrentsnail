use crate::torrent::HashId;
use crate::{Error, Result};
use serde::{de, ser, Deserialize, Serialize};
use std::{fmt::Debug, ops::Deref};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    ops::DerefMut,
};

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SocketAddrBytes(SocketAddr);

impl SocketAddrBytes {
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        match b.len() {
            6 => {
                let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                let port = u16::from_be_bytes([b[4], b[5]]);
                Ok(SocketAddrBytes::from(SocketAddrV4::new(ip, port)))
            }
            18 => {
                let buf = <[u8; 16]>::try_from(&b[..16]).unwrap();
                let ip = Ipv6Addr::from(buf);
                let port = u16::from_be_bytes([b[16], b[17]]);
                Ok(SocketAddrV6::new(ip, port, 0, 0).into())
            }
            _ => Err(de::Error::custom("not valid socket addr")),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(6);
        let ip: IpAddrBytes = self.ip().into();
        let port = self.port();
        match ip.deref() {
            IpAddr::V4(v4) => buf.extend(v4.octets()),
            IpAddr::V6(v6) => buf.extend(v6.octets()),
        };
        buf.extend(port.to_be_bytes());
        buf
    }
}

impl Deref for SocketAddrBytes {
    type Target = SocketAddr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SocketAddrBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<SocketAddr> for SocketAddrBytes {
    fn from(value: SocketAddr) -> Self {
        Self(value)
    }
}

impl From<SocketAddrV4> for SocketAddrBytes {
    fn from(value: SocketAddrV4) -> Self {
        Self(value.into())
    }
}

impl From<SocketAddrV6> for SocketAddrBytes {
    fn from(value: SocketAddrV6) -> Self {
        Self(value.into())
    }
}

impl From<&SocketAddrBytes> for SocketAddr {
    fn from(value: &SocketAddrBytes) -> Self {
        value.0
    }
}

impl Debug for SocketAddrBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Serialize for SocketAddrBytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for SocketAddrBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MyVisitor;
        impl<'de> de::Visitor<'de> for MyVisitor {
            type Value = SocketAddrBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "SocketAddrBytes")
            }

            fn visit_bytes<E>(self, b: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SocketAddrBytes::from_bytes(b).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_bytes(MyVisitor)
    }
}

#[derive(Clone)]
pub struct IpAddrBytes(IpAddr);

impl Debug for IpAddrBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Deref for IpAddrBytes {
    type Target = IpAddr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IpAddrBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Into<IpAddr>> From<T> for IpAddrBytes {
    fn from(ip: T) -> Self {
        Self(ip.into())
    }
}

impl Serialize for IpAddrBytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.deref() {
            IpAddr::V4(v4) => serializer.serialize_bytes(&v4.octets()),
            IpAddr::V6(v6) => serializer.serialize_bytes(&v6.octets()),
        }
    }
}

impl<'de> Deserialize<'de> for IpAddrBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct MyVisitor;
        impl<'de> de::Visitor<'de> for MyVisitor {
            type Value = IpAddrBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "IpAddrBytes")
            }

            fn visit_bytes<E>(self, b: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let ip: IpAddrBytes = match b.len() {
                    4 => Ipv4Addr::new(b[0], b[1], b[2], b[3]).into(),
                    16 => {
                        let buf = <[u8; 16]>::try_from(b).unwrap();
                        Ipv6Addr::from(buf).into()
                    }
                    _ => return Err(de::Error::custom("not valid ip addr")),
                };
                Ok(ip)
            }
        }

        deserializer.deserialize_bytes(MyVisitor)
    }
}

impl IpAddrBytes {
    pub fn as_v4(&self) -> Option<Ipv4Addr> {
        match self.0 {
            IpAddr::V4(v4) => Some(v4),
            _ => None,
        }
    }

    pub fn as_v6(&self) -> Option<Ipv6Addr> {
        match self.0 {
            IpAddr::V6(v6) => Some(v6),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactPeer(Vec<SocketAddrBytes>);

impl FromIterator<SocketAddrBytes> for CompactPeer {
    fn from_iter<T: IntoIterator<Item = SocketAddrBytes>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl Deref for CompactPeer {
    type Target = Vec<SocketAddrBytes>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompactPeer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct CompactPeerV4Visitor;

impl<'de> de::Visitor<'de> for CompactPeerV4Visitor {
    type Value = CompactPeer;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "CompactNodeInfoList")
    }

    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(v.as_bytes())
    }

    fn visit_bytes<E>(self, mut v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        let mut list: Vec<SocketAddrBytes> = vec![];

        while !v.is_empty() {
            let info =
                SocketAddrBytes::from_bytes(v).map_err(|e| de::Error::custom(e.to_string()))?;
            list.push(info);
            v = &v[6..]
        }

        Ok(CompactPeer(list))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CompactPeer(vec![]))
    }
}

impl<'de> de::Deserialize<'de> for CompactPeer {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(CompactPeerV4Visitor)
    }
}

impl ser::Serialize for CompactPeer {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = Vec::with_capacity(6 * self.0.len());
        for info in &self.0 {
            buf.append(&mut info.to_bytes());
        }
        serializer.serialize_bytes(&buf)
    }
}

#[derive(Debug, Clone)]
pub struct SocketAddrWithId {
    id: HashId,
    addr: SocketAddrBytes,
}

impl SocketAddrWithId {
    pub fn new(id: &HashId, addr: &SocketAddr) -> Self {
        Self {
            id: *id,
            addr: SocketAddrBytes::from(*addr),
        }
    }

    pub fn from_bytes(v: &[u8]) -> Result<Self> {
        let (id, v) = v.split_at(20);
        let info = SocketAddrWithId {
            id: id.try_into()?,
            addr: SocketAddrBytes::from_bytes(v)?,
        };
        Ok(info)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend(*self.id);
        buf.extend(self.addr.to_bytes());
        buf
    }

    pub fn get_id(&self) -> &HashId {
        &self.id
    }

    pub fn get_addr(&self) -> &SocketAddr {
        self.addr.deref()
    }

    pub fn get_size(&self) -> usize {
        20 + match self.addr.deref() {
            SocketAddr::V4(_) => 4,
            SocketAddr::V6(_) => 16,
        }
    }
}

impl Serialize for SocketAddrWithId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(
            [self.id.as_slice(), self.addr.to_bytes().as_slice()]
                .concat()
                .as_slice(),
        )
    }
}

struct CompactNodeInfoVisitor;

impl<'de> de::Visitor<'de> for CompactNodeInfoVisitor {
    type Value = SocketAddrWithId;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "CompactNodeInfo")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        SocketAddrWithId::from_bytes(v).map_err(|e| de::Error::custom(format!("{e}")))
    }
}

impl<'de> Deserialize<'de> for SocketAddrWithId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(CompactNodeInfoVisitor)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompactNodesV4(Vec<SocketAddrWithId>);

impl CompactNodesV4 {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn from_v4(v: Vec<SocketAddrWithId>) -> Result<Self> {
        if v.iter().any(|item| item.addr.is_ipv6()) {
            return Err(Error::Generic("unexpected ipv6 addr".into()));
        }
        Ok(Self(v))
    }
}

impl Deref for CompactNodesV4 {
    type Target = Vec<SocketAddrWithId>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompactNodesV4 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct CompactNodesV4Visitor;

impl<'de> de::Visitor<'de> for CompactNodesV4Visitor {
    type Value = CompactNodesV4;
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "CompactNodeInfoList")
    }

    fn visit_bytes<E>(self, mut v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        const SIZE: usize = 20 + 4 + 2;
        let mut list: Vec<SocketAddrWithId> = vec![];
        while !v.is_empty() {
            let info = SocketAddrWithId::from_bytes(&v[..SIZE])
                .map_err(|e| de::Error::custom(format!("{e}")))?;
            v = &v[SIZE..];
            list.push(info);
        }

        Ok(CompactNodesV4(list))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CompactNodesV4(vec![]))
    }
}

impl<'de> de::Deserialize<'de> for CompactNodesV4 {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(CompactNodesV4Visitor)
    }
}

impl ser::Serialize for CompactNodesV4 {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = vec![];
        for info in &self.0 {
            buf.append(&mut info.to_bytes());
        }
        serializer.serialize_bytes(&buf)
    }
}
