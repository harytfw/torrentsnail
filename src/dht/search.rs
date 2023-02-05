use std::{net::{SocketAddr}, time::Instant};

use crate::torrent::HashId;
use chrono::{prelude::*, Duration};
use serde::Serialize;

use super::Node;

pub struct SearchNode {
    pub id: HashId,
    pub addr: SocketAddr,
    pub send_query_at: DateTime<Utc>,
    pub reply_at: Instant,
    pub replied: bool,
    pub ack_announce_peer: bool,
    pub query_acc: usize,
    pub token: Vec<u8>,
    stats: SearchNodeStats,
}

impl From<&Node> for SearchNode {
    fn from(n: &Node) -> Self {
        Self {
            id: n.id,
            addr: n.addr,
            replied: false,
            ack_announce_peer: false,
            query_acc: 0,
            send_query_at: Utc.timestamp_millis_opt(0).unwrap(),
            token: vec![],
            reply_at: Instant::now(),
            stats: SearchNodeStats {
                id: hex::encode(n.id),
                addr: n.addr,
                replied: false,
                ack_announce_peer: false,
            },
        }
    }
}

impl SearchNode {
    pub fn refresh_stats(&mut self) -> &SearchNodeStats {
        self.stats.replied = self.replied;
        self.stats.ack_announce_peer = self.ack_announce_peer;
        &self.stats
    }

    pub fn on_reply(&mut self, token: &[u8]) {
        self.replied = true;
        self.reply_at = Instant::now();
        self.token = token.to_vec();
    }

    pub fn on_send_query(&mut self) {
        self.query_acc += 1;
        self.send_query_at = Utc::now();
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
    pub id: u8,
    pub hash: HashId,
    nodes_vec: Vec<SearchNode>,
    stats: SearchSessionStats,
    start_at: DateTime<Utc>,
}

impl SearchSession {
    pub fn new(id: u8) -> Self {
        Self {
            id,
            hash: Default::default(),
            nodes_vec: vec![],
            stats: Default::default(),
            start_at: Utc::now(),
        }
    }

    pub fn all_replied(&self) -> bool {
        self.nodes().iter().all(|n| n.replied)
    }

    pub fn all_ack_announce_peer(&self) -> bool {
        self.nodes().iter().all(|n| n.ack_announce_peer)
    }

    pub fn ack_announce_peer_num(&self) -> usize {
        self.nodes().iter().filter(|n| n.ack_announce_peer).count()
    }

    pub fn continue_search(&self) -> bool {
        self.ack_announce_peer_num() <= 3 && Utc::now() - self.start_at < Duration::minutes(30)
    }

    pub fn search_hash(&mut self, hash: &HashId) {
        self.start_at = Utc::now();
        self.hash = *hash;
    }

    pub fn reset(&mut self) {
        self.nodes_vec.clear();
        self.hash = HashId::zero();
    }

    pub fn add_node(&mut self, node: &Node) {
        if self.get_node(&node.id).is_some() {
            return;
        }
        self.nodes_vec.push(SearchNode::from(node))
    }

    pub fn nodes(&self) -> &[SearchNode] {
        &self.nodes_vec
    }

    pub fn nodes_mut(&mut self) -> &mut [SearchNode] {
        self.nodes_vec.as_mut_slice()
    }

    pub fn get_node(&self, id: &HashId) -> Option<&SearchNode> {
        self.nodes().iter().find(|&node| &node.id == id)
    }

    pub fn get_node_mut(&mut self, id: &HashId) -> Option<&mut SearchNode> {
        self.nodes_mut().iter_mut().find(|node| &node.id == id)
    }

    pub fn get_peers_tid(&self) -> [u8; 4] {
        let id_hex = self.id_hex();
        [b'g', b'p', id_hex[0], id_hex[1]]
    }

    pub fn get_announce_peers_tid(&self) -> [u8; 4] {
        let id_hex = self.id_hex();
        [b'a', b'p', id_hex[0], id_hex[1]]
    }

    pub fn id_hex(&self) -> [u8; 2] {
        let id_hex = hex::encode(self.id.to_be_bytes());
        assert_eq!(id_hex.len(), 2);
        return [id_hex.as_bytes()[0], id_hex.as_bytes()[1]];
    }

    pub fn refresh_stats(&mut self) -> SearchSessionStats {
        let nodes = self
            .nodes_mut()
            .iter_mut()
            .map(|n| n.refresh_stats().clone())
            .collect();
        self.stats.id = self.id;
        self.stats.hash = hex::encode(self.hash);
        self.stats.get_peers_tid = String::from_utf8_lossy(&self.get_peers_tid()).to_string();
        self.stats.announce_peer_tid =
            String::from_utf8_lossy(&self.get_announce_peers_tid()).to_string();
        self.stats.all_ack_announce_peer = self.all_ack_announce_peer();
        self.stats.all_replied = self.all_replied();
        self.stats.nodes = nodes;
        self.stats.start_at = self.start_at;
        self.stats.continue_search = self.continue_search();
        self.stats.clone()
    }
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct SearchSessionStats {
    pub id: u8,
    pub hash: String,
    pub get_peers_tid: String,
    pub announce_peer_tid: String,
    pub all_ack_announce_peer: bool,
    pub all_replied: bool,
    pub start_at: DateTime<Utc>,
    pub continue_search: bool,
    pub nodes: Vec<SearchNodeStats>,
}
