use crate::dht::Node;
use crate::torrent::HashId;
use serde::Serialize;
use std::fmt;
use std::net::SocketAddr;
use std::time::SystemTime;
#[derive(Clone)]
pub struct Bucket {
    pub min_id: HashId,
    last_changed_at: SystemTime,
    nodes: Vec<Node>,
    stats: BucketStats,
}

impl Bucket {
    pub fn new(min_id: HashId) -> Self {
        Self {
            min_id,
            nodes: Default::default(),
            last_changed_at: SystemTime::UNIX_EPOCH,
            stats: Default::default(),
        }
    }

    pub fn add_node(&mut self, node: Node) -> bool {
        let mut target = 0;
        for curr in self.nodes.iter() {
            if &node < curr {
                break;
            }
            target += 1;
        }
        self.nodes.insert(target, node);
        true
    }

    pub fn get_by_id_mut(&mut self, id: &HashId) -> Option<&mut Node> {
        self.nodes.iter_mut().find(|node| node.id() == id)
    }

    pub fn get(&self, index: usize) -> Option<&Node> {
        self.nodes.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut Node> {
        self.nodes.get_mut(index)
    }

    pub fn bad_node(&self) -> Option<usize> {
        for (i, node) in self.nodes.iter().enumerate() {
            if node.is_bad() {
                return Some(i);
            }
        }
        None
    }

    pub fn problem_node(&self) -> Option<usize> {
        for (i, node) in self.nodes.iter().enumerate() {
            if node.is_problem() {
                return Some(i);
            }
        }
        None
    }

    pub fn min_query_acc(&self) -> usize {
        self.nodes
            .iter()
            .fold(usize::MAX, |acc, node| acc.min(node.query_acc()))
    }

    pub fn max_query_acc(&self) -> usize {
        self.nodes
            .iter()
            .fold(usize::MIN, |acc, node| acc.max(node.query_acc()))
    }

    pub fn active_num(&self) -> usize {
        self.nodes.iter().filter(|node| node.is_active()).count()
    }

    pub fn nodes_num(&self) -> usize {
        self.nodes.len()
    }

    pub fn no_reply_num(&self) -> usize {
        self.nodes
            .iter()
            .filter(|node| node.reply_at().is_none())
            .count()
    }

    fn short_node_info(&self) -> Vec<(String, SocketAddr, usize)> {
        self.nodes
            .iter()
            .map(|n| (format!("{:?}", n.id()), *n.addr(), n.query_acc()))
            .collect()
    }

    pub fn refresh_stats(&mut self) -> &BucketStats {
        self.stats.min_id = format!("{:?}", self.min_id);
        self.stats.nodes_num = self.nodes_num();
        self.stats.query_acc_range = [self.min_query_acc(), self.max_query_acc()];
        self.stats.nodes = self.short_node_info();
        self.stats.active_num = self.active_num();
        self.stats.no_reply_num = self.no_reply_num();
        &self.stats
    }

    pub fn split_half(&mut self) -> Bucket {
        let mid = self.nodes.len() / 2;
        let mid_id = *self.nodes[mid].id();
        let mut new_bucket = Bucket::new(mid_id);

        new_bucket.nodes.extend(self.nodes.splice(mid.., []));

        assert!(self.min_id < new_bucket.min_id);
        assert_eq!(self.nodes.len(), mid);

        new_bucket
    }

    pub fn nodes(&self) -> &[Node] {
        self.nodes.as_slice()
    }

    pub fn mut_nodes(&mut self) -> &mut [Node] {
        self.nodes.as_mut_slice()
    }

    pub fn on_change(&mut self) {
        self.last_changed_at = SystemTime::now();
    }
}

impl fmt::Debug for Bucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bucket")
            .field("min_id", &self.min_id)
            .field("nodes_num", &self.nodes_num())
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

#[cfg(test)]
mod tests {
    use super::*;

    use rand::Rng;
    use std::net::SocketAddrV4;

    fn random_node() -> Node {
        let mut rng = rand::thread_rng();

        let mut id = HashId::ZERO_V1;
        rng.fill(id.as_mut());

        let ip: [u8; 4] = rand::random();
        let port: [u8; 2] = rand::random();
        Node::new(
            id,
            SocketAddr::V4(SocketAddrV4::new(
                u32::from_be_bytes(ip).into(),
                u16::from_be_bytes(port),
            )),
        )
    }

    #[test]
    fn test_add_node() {
        let mut bucket = Bucket::new(HashId::ZERO_V1);
        for _ in 0..100 {
            bucket.add_node(random_node());
        }
        // node should keep ascend order
        for i in 1..100 {
            assert!(bucket.get(i - 1) <= bucket.get(i))
        }
    }

    #[test]
    fn test_split() {
        let mut bucket = Bucket::new(HashId::ZERO_V1);
        let num = 100;
        for _ in 0..num {
            bucket.add_node(random_node());
        }
        let new = bucket.split_half();

        assert!(bucket.get(bucket.nodes_num() - 1) < new.get(0));

        for b in [bucket, new] {
            assert_eq!(b.nodes_num(), num / 2);
            for i in 1..b.nodes_num() {
                assert!(b.get(i - 1) < b.get(i))
            }
        }
    }
}
