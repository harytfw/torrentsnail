use chrono::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::net::SocketAddr;


use crate::torrent::HashId;

use super::Node;
#[derive(Clone, Deserialize, Serialize)]
pub struct Bucket {
    pub min_id: HashId,
    pub last_changed_at: DateTime<Utc>,
    pub nodes: Vec<Node>,
    #[serde(skip)]
    stats: BucketStats,
}

impl Bucket {
    pub fn new(min_id: HashId) -> Self {
        Self {
            min_id,
            nodes: Default::default(),
            last_changed_at: Utc::now(),
            stats: Default::default(),
        }
    }

    pub fn get_node_mut(&mut self, id: &HashId) -> Option<&mut Node> {
        self.nodes.iter_mut().find(|node| &node.id == id)
    }

    pub fn bad_node_index(&self, time: DateTime<Utc>) -> Option<usize> {
        self.nodes.iter().enumerate().find_map(
            |(i, node)| {
                if node.is_bad(time) {
                    Some(i)
                } else {
                    None
                }
            },
        )
    }

    pub fn questionable_node_index(&self, time: DateTime<Utc>) -> Option<usize> {
        for (i, node) in self.nodes.iter().enumerate() {
            if !node.is_questionable(time) {
                return Some(i);
            }
        }
        None
    }

    pub fn min_query_acc(&self) -> usize {
        self.nodes
            .iter()
            .fold(usize::MAX, |acc, node| acc.min(node.query_acc))
    }

    pub fn max_query_acc(&self) -> usize {
        self.nodes
            .iter()
            .fold(usize::MIN, |acc, node| acc.max(node.query_acc))
    }

    pub fn active_num(&self) -> usize {
        self.nodes.iter().filter(|node| node.is_active()).count()
    }

    pub fn node_num(&self) -> usize {
        self.nodes.len()
    }

    pub fn no_reply_num(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.reply_at.is_none())
            .count()
    }

    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.nodes.iter().map(|node| node.addr).collect()
    }

    pub fn ids(&self) -> Vec<&HashId> {
        self.nodes.iter().map(|n| &n.id).collect()
    }

    pub fn short_node_info(&self) -> Vec<(String, SocketAddr, usize)> {
        self.nodes
            .iter()
            .map(|n| (format!("{:?}", n.id), n.addr, n.query_acc))
            .collect()
    }

    pub fn refresh_stats(&mut self) -> &BucketStats {
        self.stats.min_id = format!("{:?}", self.min_id);
        self.stats.nodes_num = self.node_num();
        self.stats.query_acc_range = [self.min_query_acc(), self.max_query_acc()];
        self.stats.nodes = self.short_node_info();
        self.stats.active_num = self.active_num();
        self.stats.no_reply_num = self.no_reply_num();
        &self.stats
    }
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("min_id", &self.min_id)
            .field("nodes_num", &self.node_num())
            .field("nodes", &self.short_node_info())
            .field("min_query_acc", &self.min_query_acc())
            .field("max_query_acc", &self.max_query_acc())
            .field("active_num", &self.active_num())
            .field("no_reply_num", &self.no_reply_num())
            .finish()
    }
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct BucketStats {
    pub min_id: String,
    pub nodes_num: usize,
    pub nodes: Vec<(String, SocketAddr, usize)>,
    pub query_acc_range: [usize; 2],
    pub active_num: usize,
    pub no_reply_num: usize,
}
