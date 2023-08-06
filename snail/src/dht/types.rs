use crate::addr::{CompactNodesV4, SocketAddrBytes, SocketAddrWithId};
use crate::torrent::HashId;
use crate::{Error, Result};
use bencode::from_bytes;
use serde::{Deserialize, Serialize};

use std::fmt::Debug;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PingRequestA {
    id: HashId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PingQuery {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    #[serde(with = "serde_bytes")]
    q: Vec<u8>,
    a: PingRequestA,
}

impl PingQuery {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "pn".into(),
            y: "q".into(),
            q: "ping".into(),
            a: PingRequestA { id: *id },
        }
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_id(&self) -> &HashId {
        &self.a.id
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PingResponseR {
    id: HashId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PingResponse {
    #[serde(default, with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    y: Vec<u8>,
    r: PingResponseR,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "serde_bytes")]
    v: Option<Vec<u8>>,
}

impl PingResponse {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "pn".into(),
            y: "r".into(),
            r: PingResponseR { id: *id },
            v: None,
        }
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_v(&mut self, v: &str) {
        self.v = Some(v.into());
    }

    pub fn get_v(&self) -> Option<&[u8]> {
        self.v.as_deref()
    }

    pub fn get_id(&self) -> &HashId {
        &self.r.id
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FindNodeRequestA {
    id: HashId,
    target: HashId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FindNodeQuery {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    #[serde(with = "serde_bytes")]
    q: Vec<u8>,
    a: FindNodeRequestA,
}

impl FindNodeQuery {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: b"fn".to_vec(),
            a: FindNodeRequestA {
                id: *id,
                target: Default::default(),
            },
            y: "q".into(),
            q: "find_node".into(),
        }
    }

    pub fn set_target(&mut self, target: HashId) -> &mut Self {
        self.a.target = target;
        self
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_id(&self) -> &HashId {
        &self.a.id
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FindNodeResponseR {
    id: HashId,
    #[serde(default, skip_serializing_if = "CompactNodesV4::is_empty")]
    nodes: CompactNodesV4,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FindNodeResponse {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    r: FindNodeResponseR,
}

impl FindNodeResponse {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "fn".into(),
            y: "r".into(),
            r: FindNodeResponseR {
                id: *id,
                nodes: Default::default(),
            },
        }
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_id(&self) -> &HashId {
        &self.r.id
    }

    pub fn nodes(&self) -> &[SocketAddrWithId] {
        self.r.nodes.as_slice()
    }

    pub fn set_nodes(&mut self, nodes: CompactNodesV4) -> &mut Self {
        self.r.nodes = nodes;
        self
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetPeersRequestA {
    id: HashId,
    info_hash: HashId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetPeersQuery {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    #[serde(with = "serde_bytes")]
    q: Vec<u8>,
    a: GetPeersRequestA,
}

impl GetPeersQuery {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "gp".into(),
            y: "q".into(),
            q: "get_peers".into(),
            a: GetPeersRequestA {
                id: *id,
                info_hash: Default::default(),
            },
        }
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_info_hash(&self) -> &HashId {
        &self.a.info_hash
    }

    pub fn set_info_hash(&mut self, hash: &HashId) -> &mut Self {
        self.a.info_hash = *hash;
        self
    }

    pub fn get_id(&self) -> &HashId {
        &self.a.id
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetPeersResponseR {
    id: HashId,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<SocketAddrBytes>,
    #[serde(default, skip_serializing_if = "CompactNodesV4::is_empty")]
    nodes: CompactNodesV4,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GetPeersResponse {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    r: GetPeersResponseR,
}

impl GetPeersResponse {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "gp".into(),
            y: "r".into(),
            r: GetPeersResponseR {
                id: *id,
                token: Default::default(),
                values: Default::default(),
                nodes: Default::default(),
            },
        }
    }

    pub fn get_id(&self) -> &HashId {
        &self.r.id
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_nodes(&self) -> &CompactNodesV4 {
        &self.r.nodes
    }

    pub fn set_nodes(&mut self, nodes: CompactNodesV4) -> &mut Self {
        self.r.nodes = nodes;
        self
    }

    pub fn get_values(&self) -> &[SocketAddrBytes] {
        self.r.values.as_ref()
    }

    pub fn set_values(&mut self, values: Vec<SocketAddrBytes>) -> &mut Self {
        self.r.values = values;
        self
    }

    pub fn get_token(&self) -> &[u8] {
        &self.r.token
    }

    pub fn set_token(&mut self, token: &[u8]) -> &mut Self {
        self.r.token = token.to_owned();
        self
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnouncePeerRequestA {
    id: HashId,
    info_hash: HashId,
    port: u16,
    #[serde(default)]
    implied_port: u8,
    #[serde(with = "serde_bytes")]
    token: Vec<u8>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnouncePeerQuery {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    #[serde(with = "serde_bytes")]
    q: Vec<u8>,
    a: AnnouncePeerRequestA,
}

impl AnnouncePeerQuery {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: "ap".into(),
            y: "q".into(),
            q: "announce_peer".into(),
            a: AnnouncePeerRequestA {
                id: *id,
                info_hash: Default::default(),
                port: 0,
                implied_port: 0,
                token: Default::default(),
            },
        }
    }

    pub fn get_id(&self) -> &HashId {
        &self.a.id
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_owned();
        self
    }

    pub fn get_info_hash(&self) -> &HashId {
        &self.a.info_hash
    }

    pub fn set_info_hash(&mut self, hash: &HashId) -> &mut Self {
        self.a.info_hash = *hash;
        self
    }

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.a.port = port;
        self
    }

    pub fn set_implied_port(&mut self, implied_port: bool) -> &mut Self {
        self.a.implied_port = implied_port as u8;
        self
    }

    pub fn get_token(&self) -> &[u8] {
        &self.a.token
    }

    pub fn set_token(&mut self, token: &[u8]) -> &mut Self {
        self.a.token = token.to_owned();
        self
    }

    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnouncePeerResponseR {
    id: HashId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AnnouncePeerResponse {
    #[serde(with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(with = "serde_bytes")]
    y: Vec<u8>,
    r: AnnouncePeerResponseR,
}

impl AnnouncePeerResponse {
    pub fn new(id: &HashId) -> Self {
        Self {
            t: Default::default(),
            y: Default::default(),
            r: AnnouncePeerResponseR { id: *id },
        }
    }

    pub fn get_t(&self) -> &[u8] {
        &self.t
    }

    pub fn set_t(&mut self, t: &[u8]) -> &mut Self {
        self.t = t.to_vec();
        self
    }

    pub fn get_id(&self) -> &HashId {
        &self.r.id
    }
}

#[derive(Debug, serde::Deserialize)]
struct Inspector {
    #[serde(default, with = "serde_bytes")]
    t: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    y: Vec<u8>,
    #[serde(default, with = "serde_bytes")]
    q: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum Query {
    Ping(PingQuery),
    FindNode(FindNodeQuery),
    GetPeers(GetPeersQuery),
    AnnouncePeer(AnnouncePeerQuery),
}

impl Query {
    fn from_bytes(q: &[u8], buf: &[u8]) -> Result<Self> {
        let req = match q {
            b"ping" => Self::Ping(from_bytes(buf)?),
            b"find_node" => Self::FindNode(from_bytes(buf)?),
            b"get_peers" => Self::GetPeers(from_bytes(buf)?),
            b"announce_peer" => Self::AnnouncePeer(from_bytes(buf)?),
            q => {
                return Err(Error::Generic(format!(
                    "bad method {}",
                    String::from_utf8_lossy(q)
                )))
            }
        };
        Ok(req)
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    Ping(PingResponse),
    FindNode(FindNodeResponse),
    GetPeers(GetPeersResponse),
    AnnouncePeer(AnnouncePeerResponse),
}

impl Response {
    fn from_bytes(t: &[u8], buf: &[u8]) -> Result<Self> {
        if t.is_empty() {
            return Err(Error::Generic("missing transaction id".into()));
        }
        let end = 2.min(t.len());
        let req = match &t[0..end] {
            b"pn" => Self::Ping(from_bytes(buf)?),
            b"fn" => Self::FindNode(from_bytes(buf)?),
            b"gp" => Self::GetPeers(from_bytes(buf)?),
            b"ap" => Self::AnnouncePeer(from_bytes(buf)?),
            q => {
                return Err(Error::Generic(format!(
                    "unrecognized transaction id `{}`",
                    String::from_utf8_lossy(q)
                )))
            }
        };
        Ok(req)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DHTError {
    e: (i64, String),
    y: String,
}

impl DHTError {
    pub fn new(code: i64, message: &str) -> Self {
        Self {
            y: "e".into(),
            e: (code, message.into()),
        }
    }

    pub fn get_code(&self) -> i64 {
        self.e.0
    }

    pub fn set_code(&mut self, code: i64) -> &mut Self {
        self.e.0 = code;
        self
    }

    pub fn get_message(&self) -> &str {
        &self.e.1
    }

    pub fn set_message(&mut self, message: &str) -> &mut Self {
        self.e.1 = message.to_string();
        self
    }
}

#[derive(Debug, Clone)]
pub enum QueryResponse {
    Query(Query),
    Response(Response),
    Err(DHTError),
}

impl QueryResponse {
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        let r: Inspector = from_bytes(buf)?;

        let v = match r.y.as_slice() {
            b"e" => Self::Err(from_bytes(buf)?),
            b"q" => Self::Query(Query::from_bytes(&r.q, buf)?),
            b"r" => Self::Response(Response::from_bytes(&r.t, buf)?),
            unknown => {
                return Err(Error::Generic(format!(
                    "unknown type of message: `{}`",
                    String::from_utf8_lossy(unknown)
                )))
            }
        };

        Ok(v)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    macro_rules! test_packet {
        ($typ:ty, $packet:expr) => {{
            let obj: $typ = bencode::from_str($packet)?;
            let ser = bencode::to_string(&obj)?;
            assert_eq!(ser, $packet);
            obj
        }};
    }

    #[test]
    fn ping() -> Result<()> {
        {
            let req = test_packet!(
                PingQuery,
                "d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"
            );

            assert_eq!(req.y, b"q");
            assert_eq!(req.q, b"ping");
        }
        {
            let rsp = test_packet!(
                PingResponse,
                "d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re"
            );
            assert_eq!(rsp.y, b"r");
        }
        Ok(())
    }

    #[test]
    fn find_node() -> Result<()> {
        {
            let req = test_packet!(FindNodeQuery,"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe");
            assert_eq!(req.y, b"q");
            assert_eq!(req.q, b"find_node");
        }
        Ok(())
    }

    #[test]
    fn get_peers() -> Result<()> {
        {
            let _req = test_packet!(GetPeersQuery, "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe");
        }
        Ok(())
    }

    #[test]
    fn error() -> Result<()> {
        {
            let err = test_packet!(DHTError, "d1:eli0e7:unknowne1:y1:ee");
            assert_eq!(err.get_code(), 0);
            assert_eq!(err.get_message(), "unknown");
        }
        {
            test_packet!(DHTError, "d1:eli99e7:unknowne1:y1:ee");
        }

        Ok(())
    }
}
