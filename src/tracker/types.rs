use crate::addr::CompactPeerV4;
use crate::torrent::HashId;
use crate::{Error, Result};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde;
use serde::de;
use serde::{ser, Deserialize, Deserializer, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Read};
use std::net::{Ipv4Addr, SocketAddr};

#[derive(Clone, Default, PartialEq, Eq)]
pub enum Action {
    #[default]
    Connect,
    Announce,
    Scrape,
    Error,
    Unknown(u32),
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Connect => write!(f, "connect"),
            Action::Announce => write!(f, "announce"),
            Action::Error => write!(f, "error"),
            Action::Scrape => write!(f, "scrape"),
            Action::Unknown(val) => write!(f, "unknown({val})"),
        }
    }
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<u32> for Action {
    fn from(value: u32) -> Self {
        match value {
            0 => Action::Connect,
            1 => Action::Announce,
            2 => Action::Scrape,
            3 => Action::Error,
            val => Action::Unknown(val),
        }
    }
}

impl From<Action> for u32 {
    fn from(v: Action) -> Self {
        match v {
            Action::Connect => 0,
            Action::Announce => 1,
            Action::Scrape => 2,
            Action::Error => 3,
            Action::Unknown(val) => val,
        }
    }
}

impl From<Action> for Error {
    fn from(action: Action) -> Self {
        Self::Generic(action.to_string())
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub enum Event {
    #[default]
    None = 0,
    Completed,
    Started,
    Stopped,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Event::None => "none",
            Event::Completed => "completed",
            Event::Started => "started",
            Event::Stopped => "stopped",
        };
        write!(f, "{name}")
    }
}

impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl TryFrom<u32> for Event {
    type Error = Error;

    fn try_from(value: u32) -> std::result::Result<Self, Error> {
        match value {
            0 => Ok(Event::None),
            1 => Ok(Event::Completed),
            2 => Ok(Event::Started),
            3 => Ok(Event::Stopped),
            _ => Err(Error::BadEventValue),
        }
    }
}

impl From<Event> for u32 {
    fn from(v: Event) -> Self {
        match v {
            Event::None => 0,
            Event::Completed => 1,
            Event::Started => 2,
            Event::Stopped => 3,
        }
    }
}

///
///
/// Offset  Size            Name            Value
/// 0       64-bit integer  protocol_id     0x41727101980 // magic constant
/// 8       32-bit integer  action          0 // connect
/// 12      32-bit integer  transaction_id
/// 16
///
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectRequest {
    pub protocol_id: u64,
    pub action: Action,
    pub transaction_id: u32,
}

impl ConnectRequest {
    pub fn new() -> Self {
        Self {
            protocol_id: 0x41727101980,
            transaction_id: 0,
            action: Action::Connect,
        }
    }

    pub fn set_transaction_id(&mut self, id: u32) -> &mut Self {
        self.transaction_id = id;
        self
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let protocol = rdr.read_u64::<BigEndian>()?;
        let action: Action = rdr.read_u32::<BigEndian>()?.into();
        let transaction_id = rdr.read_u32::<BigEndian>()?;

        let req = Self {
            protocol_id: protocol,
            action,
            transaction_id,
        };

        Ok(req)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut data = Vec::with_capacity(16);
        data.write_u64::<BigEndian>(self.protocol_id)?;
        data.write_u32::<BigEndian>(self.action.clone().into())?;
        data.write_u32::<BigEndian>(self.transaction_id)?;
        Ok(data)
    }

    pub const fn min_packet_size() -> usize {
        12
    }
}

/// Offset  Size            Name            Value
/// 0       32-bit integer  action          0 // connect
/// 4       32-bit integer  transaction_id
/// 8       64-bit integer  connection_id
/// 16
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConnectResponse {
    pub action: Action,
    pub transaction_id: u32,
    pub connection_id: u64,
}

impl ConnectResponse {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let action: Action = rdr.read_u32::<BigEndian>()?.into();
        if action != Action::Connect {
            return Err(Error::Generic(action.to_string()));
        }

        let transaction_id = rdr.read_u32::<BigEndian>()?;
        let connection_id = rdr.read_u64::<BigEndian>()?;

        let res = ConnectResponse {
            action,
            transaction_id,
            connection_id,
        };

        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(16);
        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;
        buf.write_u64::<BigEndian>(self.connection_id)?;
        Ok(buf)
    }

    pub const fn min_packet_size() -> usize {
        16
    }
}

///
/// Offset  Size    Name    Value
/// 0       64-bit integer  connection_id
/// 8       32-bit integer  action          1 // announce
/// 12      32-bit integer  transaction_id
/// 16      20-byte string  info_hash
/// 36      20-byte string  peer_id
/// 56      64-bit integer  downloaded
/// 64      64-bit integer  left
/// 72      64-bit integer  uploaded
/// 80      32-bit integer  event           0 // 0: none; 1: completed; 2: started; 3: stopped
/// 84      32-bit integer  IP address      0 // default
/// 88      32-bit integer  key
/// 92      32-bit integer  num_want        -1 // default
/// 96      16-bit integer  port
/// 98
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnounceRequest {
    pub connection_id: u64,
    pub action: Action,
    pub transaction_id: u32,
    pub info_hash: HashId,
    pub peer_id: HashId,
    pub downloaded: u64,
    pub left: u64,
    pub uploaded: u64,
    pub event: Event,
    pub ip_address: u32,
    pub key: u32,
    pub num_want: i32,
    pub port: u16,
    pub no_peer_id: bool,
    pub supportcrypto: bool,
    pub compact: bool,
}

impl AnnounceRequest {
    pub fn new() -> Self {
        Self {
            num_want: -1,
            action: Action::Announce,
            ..Default::default()
        }
    }

    pub fn set_info_hash(&mut self, hash: &HashId) -> &mut Self {
        self.info_hash = *hash;
        self
    }

    pub fn set_peer_id(&mut self, hash: &HashId) -> &mut Self {
        self.peer_id = *hash;
        self
    }

    pub fn set_downloaded(&mut self, val: u64) -> &mut Self {
        self.downloaded = val;
        self
    }

    pub fn set_left(&mut self, val: u64) -> &mut Self {
        self.left = val;
        self
    }

    pub fn set_uploaded(&mut self, val: u64) -> &mut Self {
        self.uploaded = val;
        self
    }

    pub fn set_event(&mut self, event: Event) -> &mut Self {
        self.event = event;
        self
    }

    pub fn set_key(&mut self, val: u32) -> &mut Self {
        self.key = val;
        self
    }

    pub fn set_num_want(&mut self, val: i32) -> &mut Self {
        self.num_want = val;
        self
    }

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    pub fn set_no_peer_id(&mut self, flag: bool) -> &mut Self {
        self.no_peer_id = flag;
        self
    }

    pub fn set_ip_address(&mut self, ip_address: &SocketAddr) -> &mut Self {
        match ip_address {
            SocketAddr::V4(v4) => {
                self.ip_address = u32::from_be_bytes(v4.ip().octets());
                self.port = v4.port();
            }
            SocketAddr::V6(_) => todo!(),
        }
        self
    }

    pub fn set_compact(&mut self, compact: bool) -> &mut Self {
        self.compact = compact;
        self
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let connection_id = rdr.read_u64::<BigEndian>()?;
        let action = rdr.read_u32::<BigEndian>()?.into();
        let transaction_id = rdr.read_u32::<BigEndian>()?;

        let mut info_hash = [0u8; 20];
        rdr.read_exact(&mut info_hash)?;
        let mut peer_id = [0u8; 20];
        rdr.read_exact(&mut peer_id)?;

        let downloaded = rdr.read_u64::<BigEndian>()?;
        let left = rdr.read_u64::<BigEndian>()?;
        let uploaded = rdr.read_u64::<BigEndian>()?;
        let event = rdr.read_u32::<BigEndian>()?.try_into()?;
        let ip_address = rdr.read_u32::<BigEndian>()?;
        let key = rdr.read_u32::<BigEndian>()?;
        let num_want = rdr.read_i32::<BigEndian>()?;
        let port = rdr.read_u16::<BigEndian>()?;

        let req = AnnounceRequest {
            connection_id,
            action,
            transaction_id,
            info_hash: info_hash.into(),
            peer_id: peer_id.into(),
            downloaded,
            left,
            uploaded,
            event,
            ip_address,
            key,
            num_want,
            port,
            no_peer_id: false,
            compact: false,
            supportcrypto: false,
        };

        Ok(req)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(96);
        buf.write_u64::<BigEndian>(self.connection_id)?;
        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;
        buf.extend_from_slice(&self.info_hash);
        buf.extend_from_slice(&self.peer_id);
        buf.write_u64::<BigEndian>(self.downloaded)?;
        buf.write_u64::<BigEndian>(self.left)?;
        buf.write_u64::<BigEndian>(self.uploaded)?;
        buf.write_u32::<BigEndian>(self.event.clone().into())?;
        buf.write_u32::<BigEndian>(self.ip_address)?;
        buf.write_u32::<BigEndian>(self.key)?;
        buf.write_i32::<BigEndian>(self.num_want)?;
        buf.write_u16::<BigEndian>(self.port)?;
        Ok(buf)
    }

    pub const fn min_packet_size() -> usize {
        98
    }

    fn to_raw_query(&self) -> BTreeMap<&'static str, String> {
        let mut m: BTreeMap<&'static str, String> = Default::default();
        // m.insert("connection_id", self.connection_id.to_string());
        // m.insert("action", u32::from(self.action.clone()).to_string());
        // m.insert("transaction_id", self.transaction_id.to_string());
        m.insert("info_hash", self.info_hash.url_encoded());
        m.insert("peer_id", self.peer_id.url_encoded());
        m.insert("port", self.port.to_string());
        m.insert("uploaded", self.left.to_string());
        m.insert("downloaded", self.downloaded.to_string());
        m.insert("left", self.left.to_string());
        m.insert("corrupt", 0.to_string());
        m.insert("compact", (self.compact as u8).to_string());
        m.insert("event", self.event.to_string());
        m.insert("key", hex::encode(self.key.to_be_bytes()).to_uppercase());
        m.insert("numwant", self.num_want.to_string());
        m.insert("no_peer_id", u8::from(self.no_peer_id).to_string());
        m.insert("supportcrypto", 1.to_string());
        m.insert("redundant", 1.to_string());
        m
    }

    pub fn to_query_string(&self) -> String {
        let m = self.to_raw_query();
        let mut buf = String::new();
        assert!(!m.is_empty());
        buf.push('?');
        for (i, (k, v)) in m.into_iter().enumerate() {
            if i > 0 {
                buf.push('&')
            }
            // is it possible k or v contains invalid character which should not appear in query string?
            buf.push_str(&format!("{k}={v}"))
        }
        buf
    }
}

/// Offset      Size            Name            Value
/// 0           32-bit integer  action          1 // announce
/// 4           32-bit integer  transaction_id
/// 8           32-bit integer  interval
/// 12          32-bit integer  leechers
/// 16          32-bit integer  seeders
/// 20 + 6 * n  32-bit integer  IP address
/// 24 + 6 * n  16-bit integer  TCP port
/// 20 + 6 * N
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnounceResponseV4 {
    pub action: Action,
    pub transaction_id: u32,
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub seeders_list: Vec<(Ipv4Addr, u16)>,
}

impl AnnounceResponseV4 {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);
        let action: Action = rdr.read_u32::<BigEndian>()?.into();
        if action != Action::Announce {
            return Err(Error::from(action));
        }
        let transaction_id = rdr.read_u32::<BigEndian>()?;
        let interval = rdr.read_u32::<BigEndian>()?;
        let leechers = rdr.read_u32::<BigEndian>()?;
        let seeders = rdr.read_u32::<BigEndian>()?;

        let mut list = vec![];

        while rdr.position() < data.len() as u64 {
            let addr: Ipv4Addr = rdr.read_u32::<BigEndian>()?.into();
            let port = rdr.read_u16::<BigEndian>()?;
            list.push((addr, port));
        }

        let res = AnnounceResponseV4 {
            action,
            transaction_id,
            interval,
            leechers,
            seeders,
            seeders_list: list,
        };

        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(20 + 6 * self.seeders_list.len());
        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;
        buf.write_u32::<BigEndian>(self.interval)?;
        buf.write_u32::<BigEndian>(self.leechers)?;
        buf.write_u32::<BigEndian>(self.seeders)?;

        for (ip, port) in self.seeders_list.iter() {
            buf.extend(ip.octets());
            buf.extend(port.to_be_bytes());
        }

        Ok(buf)
    }

    pub const fn min_packet_size() -> usize {
        20
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AnnounceResponseHttp {
    #[serde(default)]
    pub complete: u64,
    #[serde(default)]
    pub downloaded: u64,
    #[serde(default)]
    pub incomplete: u64,
    #[serde(default)]
    pub interval: u64,
    #[serde(default, rename = "min interval")]
    pub min_interval: u64,
    // bep 0024
    #[serde(
        default,
        with = "serde_bytes",
        rename = "external ip",
        skip_serializing_if = "Option::is_none"
    )]
    pub external_ip: Option<Vec<u8>>,

    #[serde(default)]
    pub peers: CompactPeerV4,

    // bep-007
    #[serde(default)]
    pub peers6: CompactPeerV4,

    #[serde(default)]
    pub failure: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AnnounceResponse {
    V4(AnnounceResponseV4),
    Http(AnnounceResponseHttp),
}

impl AnnounceResponse {
    pub fn peers(&self) -> Vec<SocketAddr> {
        match self {
            Self::V4(v4) => v4
                .seeders_list
                .iter()
                .cloned()
                .map(|(ip, port)| SocketAddr::new(ip.into(), port))
                .collect(),
            Self::Http(http) => http.peers.iter().map(SocketAddr::from).collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AnnounceRequestV6 {}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct AnnounceResponseV6 {}

/// Offset          Size            Name            Value
/// 0               64-bit integer  connection_id
/// 8               32-bit integer  action          2 // scrape
/// 12              32-bit integer  transaction_id
/// 16 + 20 * n     20-byte string  info_hash
/// 16 + 20 * N
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeRequest {
    pub connection_id: u64,
    pub action: Action,
    pub transaction_id: u32,
    pub hash_info: Vec<HashId>,
}

impl ScrapeRequest {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let connection_id = rdr.read_u64::<BigEndian>()?;
        let action = rdr.read_u32::<BigEndian>()?.into();
        let transaction_id = rdr.read_u32::<BigEndian>()?;

        let mut hash_info: Vec<HashId> = vec![];
        while rdr.position() < data.len() as u64 {
            let mut hash = [0u8; 20];
            rdr.read_exact(&mut hash)?;
            hash_info.push(hash.into());
        }

        let req = ScrapeRequest {
            connection_id,
            action,
            transaction_id,
            hash_info,
        };

        Ok(req)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        buf.write_u64::<BigEndian>(self.connection_id)?;
        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;

        for hash in self.hash_info.iter() {
            buf.extend(hash.iter());
        }

        Ok(buf)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeItem {
    pub complete: u32,
    pub downloaded: u32,
    pub incomplete: u32,
}

impl ScrapeItem {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let complete = rdr.read_u32::<BigEndian>()?;
        let downloaded = rdr.read_u32::<BigEndian>()?;
        let incomplete = rdr.read_u32::<BigEndian>()?;

        Ok(Self {
            complete,
            downloaded,
            incomplete,
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(12);
        buf.write_u32::<BigEndian>(self.complete)?;
        buf.write_u32::<BigEndian>(self.downloaded)?;
        buf.write_u32::<BigEndian>(self.incomplete)?;
        Ok(buf)
    }
}

/// Offset      Size            Name            Value
/// 0           32-bit integer  action          2 // scrape
/// 4           32-bit integer  transaction_id
/// 8 + 12 * n  32-bit integer  seeders
/// 12 + 12 * n 32-bit integer  completed
/// 16 + 12 * n 32-bit integer  leechers
/// 8 + 12 * N
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScrapeResponse {
    pub action: Action,
    pub transaction_id: u32,
    pub items: Vec<ScrapeItem>,
}

impl ScrapeResponse {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let action = rdr.read_u32::<BigEndian>()?.into();
        if action != Action::Scrape {
            return Err(Error::from(action));
        }
        let transaction_id = rdr.read_u32::<BigEndian>()?;
        let mut items = vec![];

        while rdr.position() < data.len() as u64 {
            let mut item_buf = [0u8; 12];
            rdr.read_exact(&mut item_buf)?;
            items.push(ScrapeItem::from_bytes(&item_buf)?);
        }

        let res = ScrapeResponse {
            action,
            transaction_id,
            items,
        };

        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];

        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;

        for item in self.items.iter() {
            buf.extend(item.to_bytes()?);
        }

        Ok(buf)
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct TrackerError {
    pub action: Action,
    pub transaction_id: u32,
    pub message: String,
}

impl std::fmt::Display for TrackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerError")
            .field("transaction_id", &self.transaction_id)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for TrackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self, f)
    }
}

impl From<TrackerError> for Error {
    fn from(e: TrackerError) -> Self {
        Self::Generic(e.to_string())
    }
}

impl From<Box<TrackerError>> for Error {
    fn from(e: Box<TrackerError>) -> Self {
        Self::Generic(e.to_string())
    }
}

impl TrackerError {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut rdr = Cursor::new(data);

        let mut res = TrackerError {
            ..Default::default()
        };

        res.action = rdr.read_u32::<BigEndian>()?.into();
        if res.action != Action::Error {
            return Err(Error::from(res.action));
        }
        res.transaction_id = rdr.read_u32::<BigEndian>()?;
        rdr.read_to_string(&mut res.message)?;
        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        buf.write_u32::<BigEndian>(self.action.clone().into())?;
        buf.write_u32::<BigEndian>(self.transaction_id)?;
        buf.extend(self.message.as_bytes());
        Ok(buf)
    }
}

#[derive(Debug, Clone)]
enum Request {
    Connect(Box<ConnectRequest>),
    AnnounceV4(Box<AnnounceRequest>),
    Scrape(Box<ScrapeRequest>),
}

impl Request {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let action: Action = {
            let mut rdr = Cursor::new(data);
            let _connection_id = rdr.read_u64::<BigEndian>()?;
            rdr.read_u32::<BigEndian>()?.into()
        };

        let res = match action {
            Action::Connect => Self::Connect(ConnectRequest::from_bytes(data)?.into()),
            Action::Announce => Self::AnnounceV4(AnnounceRequest::from_bytes(data)?.into()),
            Action::Scrape => Self::Scrape(ScrapeRequest::from_bytes(data)?.into()),
            act => return Err(Error::from(act)),
        };

        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::Connect(req) => req.to_bytes(),
            Self::AnnounceV4(req) => req.to_bytes(),
            Self::Scrape(req) => req.to_bytes(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    Connect(ConnectResponse),
    AnnounceV4(AnnounceResponseV4),
    Scrape(ScrapeResponse),
    Error(TrackerError),
}

impl Response {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let action: Action = {
            let mut rdr = Cursor::new(data);
            let _id = rdr.read_u64::<BigEndian>()?;
            rdr.read_u32::<BigEndian>()?.into()
        };

        let res = match action {
            Action::Connect => Self::Connect(ConnectResponse::from_bytes(data)?),
            Action::Announce => Self::AnnounceV4(AnnounceResponseV4::from_bytes(data)?),
            Action::Scrape => Self::Scrape(ScrapeResponse::from_bytes(data)?),
            Action::Error => Self::Error(TrackerError::from_bytes(data)?),
            act => return Err(Error::from(act)),
        };

        Ok(res)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Response::Connect(c) => c.to_bytes(),
            Response::AnnounceV4(a) => a.to_bytes(),
            Response::Scrape(s) => s.to_bytes(),
            Response::Error(e) => e.to_bytes(),
        }
    }

    pub fn transaction_id(&self) -> u32 {
        match self {
            Self::Connect(rsp) => rsp.transaction_id,
            Self::AnnounceV4(rsp) => rsp.transaction_id,
            Self::Scrape(rsp) => rsp.transaction_id,
            Self::Error(rsp) => rsp.transaction_id,
        }
    }
}

pub struct Authentication {
    pub len: u8,
    pub username: u8,
    pub passwd_hash: u64,
}

pub struct RequestString {
    pub len: u8,
    pub string: String,
}

macro_rules! impl_ser {
    ($typ:ty) => {
        impl Serialize for $typ {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let bytes = self.to_bytes().map_err(ser::Error::custom)?;
                serializer.serialize_bytes(&bytes)
            }
        }
    };
	($($typ:ty),+ $(,)?) => {
		$(
			impl_ser!($typ);
		)*
	}
}
macro_rules! impl_de {
    ($typ:ty) => {
		impl<'de> Deserialize<'de> for $typ {
			fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
			where
				D: Deserializer<'de>,
			{
				struct BytesVisitor;

				impl<'de> de::Visitor<'de> for BytesVisitor {
					type Value = $typ;

					fn expecting(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
						fmt.write_str(stringify!($typ))
					}

					fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
					where
						E: serde::de::Error,
					{
						<$typ>::from_bytes(v).map_err(|e| de::Error::custom(e))
					}
				}

				deserializer.deserialize_bytes(BytesVisitor)
			}
		}
    };
	($($typ:ty),+ $(,)?) => {
		$(
			impl_de!($typ);
		)*
	}
}

macro_rules! impl_serde {
	($typ:ty) => {
		impl_ser!($typ);
		impl_de!($typ);
	};

	($($typ:ty),+ $(,)?) => {
		$(
			impl_serde!($typ);
		)*
	}
}

impl_serde!(
    ConnectRequest,
    ConnectResponse,
    AnnounceRequest,
    AnnounceResponseV4,
    ScrapeRequest,
    ScrapeResponse,
    Request,
    Response
);

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;

    #[test]
    fn test_connect_request() -> Result<()> {
        let req = ConnectRequest {
            protocol_id: 1,
            action: Action::Connect,
            transaction_id: 3,
        };

        assert_eq!(req.to_bytes()?.len(), 16);
        let req = ConnectRequest::from_bytes(&req.to_bytes()?)?;

        assert_eq!(req.protocol_id, 1);
        assert_eq!(req.action, Action::Connect);
        assert_eq!(req.transaction_id, 3);

        Ok(())
    }
    #[test]
    fn test_connect_response() -> Result<()> {
        let res0 = ConnectResponse {
            action: Action::Connect,
            transaction_id: 1,
            connection_id: 2,
        };
        assert_eq!(res0.to_bytes()?.len(), 16);

        let res = ConnectResponse::from_bytes(res0.to_bytes()?.as_ref())?;
        assert_eq!(res, res0);
        Ok(())
    }

    #[test]
    fn test_announce_request_v4() -> Result<()> {
        let req0 = AnnounceRequest {
            action: Action::Announce,
            transaction_id: 1,
            connection_id: 2,
            ..Default::default()
        };

        let req = AnnounceRequest::from_bytes(req0.to_bytes()?.as_ref())?;

        assert_eq!(req, req0);

        Ok(())
    }

    #[test]
    fn test_announce_response_v4() -> Result<()> {
        let res0 = AnnounceResponseV4 {
            action: Action::Announce,
            transaction_id: 1,
            seeders: 0,
            ..Default::default()
        };

        let res = AnnounceResponseV4::from_bytes(res0.to_bytes()?.as_ref())?;
        assert_eq!(res, res0);

        Ok(())
    }

    #[test]
    fn test_scrape_request() -> Result<()> {
        let req0 = ScrapeRequest {
            action: Action::Scrape,
            transaction_id: 1,
            connection_id: 2,
            ..Default::default()
        };

        let req = ScrapeRequest::from_bytes(req0.to_bytes()?.as_ref())?;

        assert_eq!(req, req0);
        Ok(())
    }

    #[test]
    fn test_scrape_response() -> Result<()> {
        let res0 = ScrapeResponse {
            action: Action::Scrape,
            transaction_id: 1,
            items: vec![ScrapeItem {
                complete: 1,
                downloaded: 2,
                incomplete: 3,
            }],
        };

        let res = ScrapeResponse::from_bytes(res0.to_bytes()?.as_ref())?;

        assert_eq!(res, res0);

        Ok(())
    }

    #[test]
    fn test_error_response() -> Result<()> {
        let res0 = TrackerError {
            action: Action::Error,
            transaction_id: 1,
            message: "some error".to_string(),
        };

        let res = TrackerError::from_bytes(res0.to_bytes()?.as_ref())?;

        assert_eq!(res, res0);

        Ok(())
    }

    #[test]
    fn test_announce_to_query_string() {
        let q = AnnounceRequest::new();
        assert!(!q.to_query_string().is_empty());
    }

}
