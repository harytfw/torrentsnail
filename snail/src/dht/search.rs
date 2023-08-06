use std::{
    borrow::Borrow,
    net::SocketAddr,
    time::{Duration, Instant, SystemTime},
};

use crate::torrent::HashId;
use serde::Serialize;

use super::Node;

pub struct SearchNode {
    id: HashId,
    addr: SocketAddr,
    send_query_at: SystemTime,
    reply_at: Instant,
    has_replied: bool,
    ack_announce_peer: bool,
    query_acc: usize,
    token: Vec<u8>,
    stats: SearchNodeStats,
}

impl<T: Borrow<Node>> From<T> for SearchNode {
    fn from(n: T) -> Self {
        let n: &Node = n.borrow();
        Self {
            id: *n.id(),
            addr: *n.addr(),
            has_replied: false,
            ack_announce_peer: false,
            query_acc: 0,
            send_query_at: SystemTime::UNIX_EPOCH,
            token: vec![],
            reply_at: Instant::now(),
            stats: SearchNodeStats {
                id: hex::encode(*n.id()),
                addr: *n.addr(),
                replied: false,
                ack_announce_peer: false,
            },
        }
    }
}

impl SearchNode {
    pub fn refresh_stats(&mut self) -> &SearchNodeStats {
        self.stats.replied = self.has_replied;
        self.stats.ack_announce_peer = self.ack_announce_peer;
        &self.stats
    }

    pub fn has_replied(&self) -> bool {
        self.has_replied
    }

    pub fn on_reply(&mut self, _token: &[u8]) {
        self.has_replied = true;
        self.reply_at = Instant::now();
    }

    pub fn on_send_query(&mut self) {
        self.query_acc += 1;
        self.send_query_at = SystemTime::now();
    }

    pub fn on_announce_peer_response(&mut self) {
        self.query_acc = 0;
        self.ack_announce_peer = true;
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn id(&self) -> &HashId {
        &self.id
    }

    pub fn token(&self) -> &[u8] {
        &self.token
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchNodeStats {
    id: String,
    addr: SocketAddr,
    replied: bool,
    ack_announce_peer: bool,
}

pub struct SearchSession {
    id: u8,
    info_hash: HashId,
    nodes: Vec<SearchNode>,
    start_at: SystemTime,
    started: bool,
}

impl SearchSession {
    pub fn new(id: u8, info_hash: HashId) -> Self {
        Self {
            id,
            info_hash,
            nodes: vec![],
            start_at: SystemTime::UNIX_EPOCH,
            started: false,
        }
    }

    pub fn all_replied(&self) -> bool {
        self.nodes().iter().all(|n| n.has_replied)
    }

    pub fn all_ack_announce_peer(&self) -> bool {
        self.nodes().iter().all(|n| n.ack_announce_peer)
    }

    pub fn ack_announce_peer_num(&self) -> usize {
        self.nodes().iter().filter(|n| n.ack_announce_peer).count()
    }

    pub fn continue_search(&self) -> bool {
        let elapsed_30_minute = match self.start_at.elapsed() {
            Ok(dur) => dur > Duration::from_secs(30 * 60),
            _ => true,
        };
        self.started && self.ack_announce_peer_num() <= 3 && elapsed_30_minute
    }

    pub fn start(&mut self) {
        self.started = true;
        self.start_at = SystemTime::now();
    }

    pub fn stop(&mut self) {
        self.started = false;
    }

    pub fn reset(&mut self, info_hash: HashId) {
        self.stop();
        self.nodes.clear();
        self.info_hash = info_hash;
    }

    pub fn add_node(&mut self, node: SearchNode) {
        if self.find_node(node.id()).is_some() {
            return;
        }
        self.nodes.push(node)
    }

    pub fn add_many_nodes(&mut self, nodes: impl Iterator<Item = SearchNode>) {
        for node in nodes {
            self.add_node(node)
        }
    }

    pub fn nodes(&self) -> &[SearchNode] {
        &self.nodes
    }

    pub fn mut_nodes(&mut self) -> &mut [SearchNode] {
        self.nodes.as_mut_slice()
    }

    pub fn find_node(&self, id: &HashId) -> Option<&SearchNode> {
        self.nodes().iter().find(|&node| &node.id == id)
    }

    pub fn find_mut_node(&mut self, id: &HashId) -> Option<&mut SearchNode> {
        self.mut_nodes().iter_mut().find(|node| &node.id == id)
    }

    pub fn get_peers_tid(&self) -> [u8; 4] {
        let id_hex = self.id_hex();
        [b'g', b'p', id_hex[0], id_hex[1]]
    }

    pub fn get_announce_peers_tid(&self) -> [u8; 4] {
        let id_hex = self.id_hex();
        [b'a', b'p', id_hex[0], id_hex[1]]
    }

    fn id_hex(&self) -> [u8; 2] {
        let id_hex = hex::encode(self.id.to_be_bytes());
        return [id_hex.as_bytes()[0], id_hex.as_bytes()[1]];
    }

    pub fn refresh_stats(&mut self) -> SearchSessionStats {
        let nodes = self
            .mut_nodes()
            .iter_mut()
            .map(|n| n.refresh_stats().clone())
            .collect();

        SearchSessionStats {
            id: self.id,
            hash: hex::encode(self.info_hash),
            get_peers_tid: String::from_utf8_lossy(&self.get_peers_tid()).to_string(),
            announce_peer_tid: String::from_utf8_lossy(&self.get_announce_peers_tid()).to_string(),
            all_ack_announce_peer: self.all_ack_announce_peer(),
            all_replied: self.all_replied(),
            nodes,
            start_at: self.start_at,
            continue_search: self.continue_search(),
        }
    }

    pub fn info_hash(&self) -> &HashId {
        &self.info_hash
    }

    pub fn id(&self) -> u8 {
        self.id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchSessionStats {
    pub id: u8,
    pub hash: String,
    pub get_peers_tid: String,
    pub announce_peer_tid: String,
    pub all_ack_announce_peer: bool,
    pub all_replied: bool,
    pub start_at: SystemTime,
    pub continue_search: bool,
    pub nodes: Vec<SearchNodeStats>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_session() {
        let mut ss = SearchSession::new(0, HashId::ZERO_V1);
        ss.start();
        ss.stop();
        ss.reset(HashId::ZERO_V1);
    }
}
