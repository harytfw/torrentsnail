use crate::{
    bencode::Value,
    session::{Peer, TorrentSession, TorrentSessionStatus},
    torrent::HashId,
    Error, Result,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::{
    ops::Add,
    str::FromStr,
    time::{Duration, SystemTime},
};

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
pub struct TorrentSessionInfo {
    name: String,
    info_hash: HexHashId,
    torrent: (),
    peers: Vec<PeerInfo>,
    status: TorrentSessionStatus,
}

impl TorrentSessionInfo {
    pub fn from_session(session: &TorrentSession) -> Self {
        Self {
            name: session.name.clone(),
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
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateTorrentSessionRequest {
    pub uri: String,
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
                port.parse::<u16>()
                    .map_err(|e| Error::Generic(e.to_string()))?,
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

impl From<Error> for ErrorResponse {
    fn from(e: Error) -> Self {
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
