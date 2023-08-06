use crate::addr::SocketAddrWithId;
use crate::torrent::HashId;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::SystemTime;

#[derive(Clone)]
pub struct Node {
    id: HashId,
    addr: SocketAddr,
    active_at: SystemTime,
    reply_at: Option<SystemTime>,
    query_acc: usize,
    last_ping_at: SystemTime,
}

impl From<&Node> for SocketAddrWithId {
    fn from(node: &Node) -> Self {
        Self::new(&node.id, &node.addr)
    }
}

impl From<SocketAddrWithId> for Node {
    fn from(node: SocketAddrWithId) -> Self {
        Self::new(*node.get_id(), *node.get_addr())
    }
}

impl Node {
    pub fn new(id: HashId, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            reply_at: None,
            query_acc: 0,
            active_at: SystemTime::UNIX_EPOCH,
            last_ping_at: SystemTime::UNIX_EPOCH,
        }
    }

    pub fn on_ping(&mut self) {
        self.query_acc += 1;
        self.last_ping_at = SystemTime::now();
    }

    pub fn on_query(&mut self) {
        self.query_acc += 1;
        self.active_at = SystemTime::now();
    }

    pub fn on_response(&mut self) {
        self.query_acc = 0;
        self.active_at = SystemTime::now();
        self.reply_at = Some(self.active_at);
    }

    pub fn should_ping(&self) -> bool {
        let min_interval = Duration::from_secs(15);
        let max_interval = Duration::from_secs(30 * 60);

        let interval = Duration::from_secs(2u64.pow(self.query_acc.min(15) as u32));
        let interval = interval.clamp(min_interval, max_interval);

        if let Ok(dur) = self.last_ping_at.elapsed() {
            dur < interval
        } else if let Ok(dur) = self.active_at.elapsed() {
            dur > min_interval
        } else {
            true
        }
    }

    pub fn is_problem(&self) -> bool {
        if self.reply_at.is_none() {
            return true;
        }
        if let Ok(dur) = self.active_at.elapsed() {
            dur > Duration::from_secs(15 * 60)
        } else {
            true
        }
    }

    pub fn is_bad(&self) -> bool {
        self.is_problem() && self.query_acc > 3
    }

    pub fn is_active(&self) -> bool {
        !self.is_bad()
    }

    pub fn query_acc(&self) -> usize {
        self.query_acc
    }

    pub fn active_at(&self) -> SystemTime {
        self.active_at
    }

    pub fn reply_at(&self) -> Option<SystemTime> {
        self.reply_at
    }

    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }

    pub fn id(&self) -> &HashId {
        &self.id
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("id", &self.id)
            .field("active_at", &self.active_at)
            .field("addr", &self.addr)
            .field("query_acc", &self.query_acc)
            .field("reply_at", &self.reply_at)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use std::net::SocketAddrV4;

    use super::*;
    #[test]
    fn test_node() {
        let node = Node::new(
            HashId::ZERO_V1,
            "127.0.0.1:1800".parse::<SocketAddrV4>().unwrap().into(),
        );

        node.is_active();
        node.is_bad();
        node.is_problem();
    }
}
