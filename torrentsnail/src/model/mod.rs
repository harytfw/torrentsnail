use anyhow::{Error, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use snail::{
    session::{Peer, TorrentSession, TorrentSessionStatus},
    torrent::HashId,
};
use std::{fmt::Debug, net::SocketAddr};

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping {
    pub timestamp_ms: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Pong {
    timestamp: i64,
}

impl Pong {
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now().timestamp(),
        }
    }
}

#[derive(Debug)]
pub struct HexHashId(HashId);

impl Serialize for HexHashId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.hex())
    }
}

impl std::fmt::Display for HexHashId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.hex())
    }
}

#[derive(Debug, Serialize)]
pub struct PeerInfo {
    peer_id: HexHashId,
    addr: String,
}

impl PeerInfo {
    pub fn from_peer(peer: &Peer) -> Self {
        Self {
            peer_id: HexHashId(peer.peer_id),
            addr: peer.addr.to_string(),
        }
    }
}

pub struct TrackerInfo {
    url: String,
    status: (),
}

#[derive(Serialize, Debug)]
pub struct TorrentSessionModel {
    pub(crate) name: String,
    pub(crate) info_hash: HexHashId,
    pub(crate) torrent: (),
    pub(crate) peers: Vec<PeerInfo>,
    pub(crate) status: TorrentSessionStatus,
    pub(crate) size: u64,
    pub(crate) progress: u64,
    pub(crate) seeders: u64,
    pub(crate) files: String,
}

impl TorrentSessionModel {
    pub fn from_session(session: &TorrentSession) -> Self {
        Self {
            name: session.name.to_string(),
            info_hash: HexHashId(*session.info_hash),
            peers: session
                .peers()
                .iter()
                .map(|peer| PeerInfo {
                    peer_id: HexHashId(peer.peer_id),
                    addr: peer.addr.to_string(),
                })
                .collect(),
            status: session.status(),
            torrent: (),
            size: 0,
            progress: 0,
            seeders: 0,
            files: "".to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateTorrentSessionRequest {
    pub uri: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateTorrentSessionRequest {
    pub info_hash: HashId,
    pub status: Option<TorrentSessionStatus>,
}

#[derive(Debug, Deserialize)]
pub struct AddSessionPeerRequest {
    pub peers: String,
}

impl AddSessionPeerRequest {
    pub fn parse_peers(&self) -> Result<Vec<SocketAddr>> {
        let mut peers = Vec::new();
        for peer in self.peers.split('\n') {
            let mut parts = peer.split(':');
            let ip = parts.next().unwrap();
            let port = parts.next().unwrap();
            peers.push(SocketAddr::new(
                ip.parse()?,
                port.parse::<u16>().map_err(|e| e)?,
            ));
        }
        Ok(peers)
    }
}

#[derive(Serialize)]
pub struct CursorPagination<T: Serialize> {
    value: T,
    next_cursor: Option<String>,
}

impl<T: Serialize> CursorPagination<T> {
    pub fn new(value: T, next_cursor: Option<String>) -> Self {
        Self { value, next_cursor }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    error: ErrorResponseItem,
}

impl ErrorResponse {
    pub fn from_str(s: &str) -> Self {
        Self {
            error: ErrorResponseItem {
                code: "TODO".to_string(),
                message: s.to_string(),
            },
        }
    }
}

impl<T> From<T> for ErrorResponse
where
    T: std::error::Error + Send + Sync + 'static,
{
    fn from(e: T) -> Self {
        Self {
            error: ErrorResponseItem {
                code: "TODO".to_string(),
                message: e.to_string(),
            },
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponseItem {
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
pub struct OkResponse<T>
where
    T: Debug + Serialize,
{
    value: T,
}

impl<T> OkResponse<T>
where
    T: Debug + Serialize,
{
    pub fn new(value: T) -> Self {
        Self { value }
    }
}
