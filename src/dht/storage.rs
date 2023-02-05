use crate::torrent::HashId;
use serde::Serialize;
use std::net::SocketAddr;
use tracing::debug;

const MAX_PEERS: usize = 100;

#[derive(Debug, Clone)]
pub struct Storage {
    pub hash: HashId,
    pub peers: Vec<SocketAddr>,
    stats: StorageStats,
}

impl Storage {
    pub fn new(hash: &HashId) -> Self {
        Self {
            hash: *hash,
            peers: vec![],
            stats: Default::default(),
        }
    }

    pub fn add_peer(&mut self, peer: &SocketAddr) {
        if self.find_by_addr(peer).is_some() {
            return;
        }

        if self.peers.len() > MAX_PEERS {
            debug!(peer = ?peer, hash=?self.hash, "ignore peer");
            return;
        }

        self.peers.push(*peer);
    }

    fn find_by_addr(&self, addr: &SocketAddr) -> Option<usize> {
        for (i, peer) in self.peers.iter().enumerate() {
            if peer == addr {
                return Some(i);
            }
        }
        None
    }

    pub fn refresh_stats(&mut self) -> &StorageStats {
        self.stats.peers = self.peers.clone();
        self.stats.peers = self.peers.clone();
        self.stats.hash = hex::encode(self.hash);
        &self.stats
    }
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct StorageStats {
    peers_num: usize,
    hash: String,
    peers: Vec<SocketAddr>,
}
