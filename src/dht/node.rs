use crate::addr::SocketAddrWithId;
use crate::torrent::HashId;
use chrono::prelude::*;
use chrono::Duration;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::net::SocketAddr;

#[derive(Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: HashId,
    pub addr: SocketAddr,
    pub active_at: DateTime<Utc>,
    pub reply_at: Option<DateTime<Utc>>,
    pub query_acc: usize,
    pub last_ping_at: DateTime<Utc>,
}

impl From<&Node> for SocketAddrWithId {
    fn from(node: &Node) -> Self {
        Self::new(&node.id, &node.addr.into())
    }
}

impl Node {
    pub fn new(addr: &SocketAddr, id: &HashId) -> Self {
        assert_eq!(id.len(), 20);

        Self {
            id: *id,
            addr: *addr,
            active_at: Utc::now(),
            reply_at: None,
            query_acc: 0,
            last_ping_at: Utc::now(),
        }
    }

    pub fn from_compact(node: SocketAddrWithId) -> Self {
        Self {
            id: *node.get_id(),
            addr: *node.get_addr(),
            active_at: Utc::now(),
            reply_at: None,
            query_acc: 0,
            last_ping_at: Utc::now(),
        }
    }

    pub fn should_ping(&self, time: DateTime<Utc>) -> bool {
        let min_interval = Duration::seconds(15);
        let max_interval = Duration::minutes(30);

        let interval = Duration::seconds(2i64.pow(self.query_acc.min(15) as u32));
        let interval = interval.clamp(min_interval, max_interval);

        if time - self.last_ping_at < interval {
            return false;
        }

        time - self.active_at > min_interval
    }

    pub fn is_questionable(&self, time: DateTime<Utc>) -> bool {
        if self.reply_at.is_none() {
            return true;
        }
        time - self.active_at > Duration::minutes(15)
    }

    pub fn is_bad(&self, _time: DateTime<Utc>) -> bool {
        self.is_questionable(_time) && self.query_acc > 3
    }

    pub fn is_active(&self) -> bool {
        !self.is_bad(Utc::now())
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
        self.partial_cmp(other).unwrap()
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
