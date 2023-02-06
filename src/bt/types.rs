use crate::addr::IpAddrBytes;
use crate::bencode;
use crate::torrent::HashId;
use crate::{Error, Result};
use byteorder::{NetworkEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::{collections::BTreeMap, fmt::Debug};

#[derive(Clone)]
pub struct PieceData {
    pub index: u32,
    pub begin: u32,
    pub fragment: Vec<u8>,
}

impl Debug for PieceData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"index", &self.index)
            .entry(&"begin", &self.begin)
            .finish()
    }
}

impl PieceData {
    pub fn sha1(&self) -> Result<[u8; 20]> {
        let info_hash =
            ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &self.fragment);
        let hash = info_hash.as_ref().try_into().unwrap();
        Ok(hash)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        [
            &self.index.to_be_bytes(),
            &self.begin.to_be_bytes(),
            self.fragment.as_slice(),
        ]
        .concat()
    }
}

#[derive(Debug, Clone)]
pub struct PieceInfo {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl PieceInfo {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        buf.write_u32::<NetworkEndian>(self.index).unwrap();
        buf.write_u32::<NetworkEndian>(self.begin).unwrap();
        buf.write_u32::<NetworkEndian>(self.length).unwrap();
        buf
    }
}

#[derive(Default, Clone)]
pub struct BTExtension([u8; 8]);

impl std::ops::Deref for BTExtension {
    type Target = [u8; 8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for BTExtension {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Debug for BTExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entry(&"ext_handshake", &self.get_ext_handshake())
            .finish()
    }
}

impl BTExtension {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_ext_handshake(&mut self, flag: bool) -> &mut Self {
        if flag {
            self.0[5] |= 1 << 4;
        } else {
            self.0[5] &= !(1 << 4);
        }
        self
    }

    pub fn get_ext_handshake(&self) -> bool {
        self.0[5] & (1 << 4) != 0
    }
}

#[derive(Default, Clone)]
pub struct BTHandshake {
    pub p: [u8; 1],
    pub protocol: [u8; 19],
    pub extension: BTExtension,
    pub info_hash: HashId,
    pub peer_id: HashId,
    pub ext_handshake: Option<ExtHandshake>,
}

impl Debug for BTHandshake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handshake")
            .field("info_hash", &self.info_hash)
            .field("peer_id", &self.peer_id)
            .field("ext_handshake", &self.ext_handshake)
            .finish()
    }
}

impl BTHandshake {
    pub fn new(peer_id: &HashId, info_hash: &HashId) -> Self {
        static VERSION_STRING: [u8; 19] = *b"BitTorrent protocol";

        Self {
            p: [VERSION_STRING.len() as u8],
            protocol: VERSION_STRING,
            extension: BTExtension::new(),
            info_hash: *info_hash,
            peer_id: *peer_id,
            ext_handshake: None,
        }
    }

    pub fn set_ext_handshake(&mut self, b: bool) -> &mut Self {
        self.extension.set_ext_handshake(b);
        if b && self.ext_handshake.is_none() {
            self.ext_handshake = Some(ExtHandshake::new())
        }
        self
    }

    pub const fn message_len() -> usize {
        1 + 19 + 8 + 20 + 20
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        [
            self.p.as_slice(),
            self.protocol.as_slice(),
            self.extension.as_slice(),
            self.info_hash.as_slice(),
            self.peer_id.as_slice(),
        ]
        .concat()
    }

    pub async fn from_reader_async<R>(r: &mut R) -> Result<Self>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        use tokio::io::AsyncReadExt;

        let mut s = Self {
            ..Default::default()
        };

        r.read_exact(&mut s.p).await?;
        r.read_exact(&mut s.protocol).await?;
        r.read_exact(s.extension.as_mut()).await?;
        r.read_exact(s.info_hash.as_mut()).await?;
        r.read_exact(s.peer_id.as_mut()).await?;

        Ok(s)
    }
}

#[derive(Clone)]
pub enum BTMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    BitField(Vec<u8>),
    Request(PieceInfo),
    Piece(PieceData),
    Cancel(PieceInfo),
    Ping,
    Ext(BTExtMessage),
}

impl Debug for BTMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("BTMessage");
        let tmp = ds.field("type", &self.msg_name());
        let tmp = match self {
            Self::Have(n) => tmp.field("index", &n),
            Self::BitField(bits) => tmp.field("bits", &bits),
            Self::Request(info) => tmp.field("info", &info),
            Self::Piece(data) => tmp.field("data", &data),
            Self::Cancel(info) => tmp.field("info", &info),
            _ => tmp,
        };
        tmp.finish()
    }
}

impl BTMessage {
    fn msg_id(&self) -> u8 {
        match self {
            Self::Choke => 0,
            Self::Unchoke => 1,
            Self::Interested => 2,
            Self::NotInterested => 3,
            Self::Have(_) => 4,
            Self::BitField(_) => 5,
            Self::Request(_) => 6,
            Self::Piece(_) => 7,
            Self::Cancel(_) => 8,
            Self::Ext(_) => 20,
            Self::Ping => unreachable!(),
        }
    }

    fn msg_name(&self) -> &'static str {
        match self {
            Self::Ping => "Ping",
            Self::Choke => "Choke(0)",
            Self::Unchoke => "Unchoke(1)",
            Self::Interested => "Interested(2)",
            Self::NotInterested => "NotInterested(3)",
            Self::Have(_) => "Have(4)",
            Self::BitField(_) => "BitField(5)",
            Self::Request(_) => "Request(6)",
            Self::Piece(_) => "Piece(7)",
            Self::Cancel(_) => "Cancel(8)",
            Self::Ext(_) => "Ext(20)",
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // TODO: use bytes crate to reduce bytes copied
        let mut buf = Vec::with_capacity(16);
        buf.extend([0, 0, 0, 0]);

        if matches!(self, Self::Ping) {
            return buf;
        }

        buf.push(self.msg_id());
        match self {
            Self::Have(index) => {
                buf.write_u32::<NetworkEndian>(*index).unwrap();
            }
            Self::BitField(bits) => {
                buf.extend(bits);
            }
            Self::Request(info) => {
                buf.append(&mut info.to_bytes());
            }
            Self::Piece(data) => {
                buf.append(&mut data.to_bytes());
            }
            Self::Cancel(info) => {
                buf.append(&mut info.to_bytes());
            }
            Self::Ext(msg) => {
                buf.append(&mut msg.to_bytes());
            }
            _ => {}
        }
        let msg_len = buf.len() as u32 - 4;
        buf[..4].copy_from_slice(msg_len.to_be_bytes().as_ref());
        buf
    }

    pub async fn from_reader_async<R>(r: &mut R) -> Result<Self>
    where
        R: tokio::io::AsyncRead + ?Sized + Unpin,
    {
        use tokio::io::AsyncReadExt;

        let len = r.read_u32().await? as usize;
        if len == 0 {
            return Ok(Self::Ping);
        }

        let payload_len = len - 1;

        let typ = r.read_u8().await?;
        let msg = match typ {
            0 => Self::Choke,
            1 => Self::Unchoke,
            2 => Self::Interested,
            3 => Self::NotInterested,
            4 => {
                let index = r.read_u32().await?;
                Self::Have(index)
            }
            5 => {
                let mut bits = vec![0; payload_len];
                r.read_exact(&mut bits).await?;
                Self::BitField(bits)
            }
            typ @ 6 | typ @ 8 => {
                let index = r.read_u32().await?;
                let begin = r.read_u32().await?;
                let length = r.read_u32().await?;
                let info = PieceInfo {
                    index,
                    begin,
                    length,
                };
                match typ {
                    6 => Self::Request(info),
                    8 => Self::Cancel(info),
                    _ => unreachable!(),
                }
            }
            7 => {
                let index = r.read_u32().await?;
                let begin = r.read_u32().await?;
                let fragment_len = payload_len - 4 - 4;
                let mut data = PieceData {
                    index,
                    begin,
                    fragment: vec![0; fragment_len],
                };
                r.read_exact(&mut data.fragment).await?;
                Self::Piece(data)
            }
            20 => {
                let ext_id = r.read_u8().await?;
                let ext_len = payload_len - 1;
                let mut ext_payload = vec![0; ext_len];
                r.read_exact(&mut ext_payload).await?;
                Self::Ext(BTExtMessage::new(ext_id, ext_payload))
            }
            val => {
                return Err(Error::UnknownMessageType(val));
            }
        };
        Ok(msg)
    }
}

#[derive(Clone)]
pub struct BTExtMessage {
    pub id: u8,
    pub payload: Vec<u8>,
}

impl BTExtMessage {
    pub fn new(id: u8, payload: Vec<u8>) -> Self {
        Self { id, payload }
    }

    pub fn into_handshake(self) -> Result<ExtHandshake> {
        if self.id != 0 {
            return Err(Error::Handshake("ext msg id != 0".into()));
        }
        let v: ExtHandshake = bencode::from_bytes(&self.payload)?;
        Ok(v)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + self.payload.len());
        buf.extend(self.id.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}

impl<T: Into<BTExtMessage>> From<T> for BTMessage {
    fn from(t: T) -> Self {
        Self::Ext(t.into())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtHandshake {
    m: BTreeMap<String, u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    p: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    v: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    yourip: Option<IpAddrBytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ipv6: Option<IpAddrBytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ipv4: Option<IpAddrBytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reqq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata_size: Option<usize>,
}

impl From<ExtHandshake> for BTExtMessage {
    fn from(value: ExtHandshake) -> Self {
        Self::new(
            0,
            bencode::to_bytes(&value).expect("ext handshake to bencode"),
        )
    }
}

impl ExtHandshake {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn set_m(&mut self, m: &BTreeMap<String, u8>) -> &mut Self {
        self.m = m.to_owned();
        self
    }

    pub fn get_m(&self) -> &BTreeMap<String, u8> {
        &self.m
    }

    pub fn set_version(&mut self, ver: &str) -> &mut Self {
        self.v = Some(ver.to_string());
        self
    }

    pub fn set_youip(&mut self, ip: IpAddrBytes) -> &mut Self {
        self.yourip = Some(ip);
        self
    }

    pub fn get_youip(&self) -> Option<IpAddrBytes> {
        self.yourip.clone()
    }

    pub fn set_ip(&mut self, ip: IpAddr) -> &mut Self {
        match ip {
            IpAddr::V4(v4) => self.set_ipv4(v4),
            IpAddr::V6(v6) => self.set_ipv6(v6),
        }
    }

    pub fn set_ipv6(&mut self, ipv6: Ipv6Addr) -> &mut Self {
        self.ipv6 = Some(ipv6.into());
        self
    }

    pub fn set_ipv4(&mut self, ipv4: Ipv4Addr) -> &mut Self {
        self.ipv4 = Some(ipv4.into());
        self
    }

    pub fn get_metadata_size(&self) -> Option<usize> {
        self.metadata_size
    }

    pub fn get_msg_id(&self, name: &str) -> Option<u8> {
        self.get_m().get(name).copied()
    }

    pub fn get_msg_name(&self, id: u8) -> Option<&str> {
        for (k, v) in self.get_m().iter() {
            if *v == id {
                return Some(k);
            }
        }
        None
    }

    pub fn set_msg(&mut self, id: u8, name: &str) -> &mut Self {
        self.m.insert(name.to_owned(), id);
        self
    }
}

pub struct UTMetadataPieceData {
    pub piece: usize,
    pub total_size: usize,
    pub payload: Vec<u8>,
}

impl Debug for UTMetadataPieceData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entry(&self.piece)
            .entry(&self.total_size)
            .entry(&self.payload.len())
            .finish()
    }
}

#[derive(Debug)]
pub enum UTMetadataMessage {
    Request(usize),
    Data(UTMetadataPieceData),
    Reject(usize),
}

impl From<(u8, UTMetadataMessage)> for BTExtMessage {
    fn from((id, msg): (u8, UTMetadataMessage)) -> Self {
        Self::new(id, msg.to_bytes())
    }
}

impl UTMetadataMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut val: BTreeMap<String, bencode::Value> = Default::default();
        let mut payload = None;
        match self {
            Self::Request(piece) => {
                val.insert("msg_type".to_owned(), 0.into());
                val.insert("piece".to_owned(), piece.into());
            }
            Self::Data(piece) => {
                val.insert("msg_type".to_owned(), 1.into());
                val.insert("total_size".to_owned(), piece.total_size.into());
                val.insert("piece".to_owned(), piece.piece.into());
                payload = Some(&piece.payload);
            }
            Self::Reject(piece) => {
                val.insert("msg_type".to_owned(), 2.into());
                val.insert("piece".to_owned(), piece.into());
            }
        }
        let mut buf = bencode::to_bytes(&val).unwrap();
        if let Some(payload) = payload {
            buf.extend(payload);
        }
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let (dict, remain) = bencode::from_bytes_with_remain::<bencode::Value, _>(buf)?;

        let msg_type = dict
            .dict_get("msg_type")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| Error::Generic("require msg_type".into()))?;

        let piece = dict
            .dict_get("piece")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| Error::Generic("require piece".into()))? as usize;

        let msg = match msg_type {
            0 => Self::Request(piece),
            1 => {
                let total_size = dict
                    .dict_get("total_size")
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| Error::Generic("require total_size".into()))?
                    as usize;

                let payload = remain;

                Self::Data(UTMetadataPieceData {
                    piece,
                    total_size,
                    payload,
                })
            }
            2 => Self::Reject(piece),
            _ => return Err(Error::Generic("unknown msg type".into())),
        };

        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    

    use super::{BTExtension};

    #[test]
    fn extension() {
        let mut ext = BTExtension::new();
        assert!(!ext.get_ext_handshake());
        ext.set_ext_handshake(true);
        assert!(ext.get_ext_handshake());
        ext.set_ext_handshake(false);
        assert!(!ext.get_ext_handshake());
    }
}