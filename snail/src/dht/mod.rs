use crate::addr::CompactNodesV4;
use crate::addr::SocketAddrWithId;
use crate::torrent::HashId;
use crate::{Error, Result};
use bencode::to_bytes;
use bucket::BucketStats;
use rand::random;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing::{debug, error};

mod bucket;
mod node;
mod search;
mod token;
mod types;
pub use bucket::Bucket;
pub use node::Node;
pub use search::{SearchNode, SearchSession, SearchSessionStats};
pub use token::TokenManager;
pub use types::{
    AnnouncePeerQuery, AnnouncePeerResponse, DHTError, FindNodeQuery, FindNodeResponse,
    GetPeersQuery, GetPeersResponse, PingQuery, PingResponse, Query, QueryResponse, Response,
};

#[derive(Clone, Default, Debug, Serialize)]
pub struct DHTStats {
    node_num: usize,
    active_num: usize,
    no_reply_num: usize,
    query_acc_range: (usize, usize),
    search_session_replied_num: usize,
    ping_query: usize,
    ping_response: usize,
    find_node_query: usize,
    find_node_response: usize,
    get_peers_query: usize,
    get_peers_response: usize,
    announce_peer_query: usize,
    announce_peer_response: usize,
    error: usize,
    buckets_stats: Vec<BucketStats>,
    search_session_stats: Vec<SearchSessionStats>,
}

#[derive(Clone)]
pub struct DHTSender {
    sock: Arc<UdpSocket>,
    my_id: Arc<HashId>,
}

impl DHTSender {
    fn new(sock: Arc<UdpSocket>, my_id: Arc<HashId>) -> Self {
        Self { sock, my_id }
    }

    async fn reply_error(&self, dest: &SocketAddr, code: i64, message: &str) -> Result<()> {
        let err_rsp = DHTError::new(code, message);
        let buf = to_bytes(&err_rsp)?;
        self.sock.send_to(&buf, &dest).await?;
        Ok(())
    }

    async fn send_ping(&self, dest: &SocketAddr) -> Result<()> {
        debug!(addr = ?dest, "send ping");

        let mut q = PingQuery::new(&self.my_id);
        q.set_t(b"pn");

        let buf = to_bytes(&q)?;
        self.sock.send_to(&buf, &dest).await?;
        Ok(())
    }

    async fn reply_ping(&self, t: &[u8], dest: &SocketAddr) -> Result<()> {
        debug!(dest = ?dest, "reply ping query");

        let mut rsp = PingResponse::new(&self.my_id);
        rsp.set_t(t);

        let buf = to_bytes(&rsp)?;
        self.sock.send_to(&buf, dest).await?;
        Ok(())
    }

    async fn send_find_node(&self, dest: &SocketAddr, target: &HashId) -> Result<()> {
        debug!(dest=?dest, "send find node");

        let mut rsp = FindNodeQuery::new(&self.my_id);
        rsp.set_t(b"fn").set_target(*target);

        let buf = to_bytes(&rsp)?;
        self.sock.send_to(&buf, dest).await?;
        Ok(())
    }

    async fn reply_find_node(
        &self,
        t: &[u8],
        dest: &SocketAddr,
        compact_nodes: CompactNodesV4,
    ) -> Result<()> {
        debug!(dest = ?dest, "reply find node");

        let mut rsp = FindNodeResponse::new(&self.my_id);
        rsp.set_nodes(compact_nodes).set_t(t);

        let buf = to_bytes(&rsp)?;
        self.sock.send_to(&buf, dest).await?;
        Ok(())
    }

    async fn reply_get_peers(
        &self,
        dest: &SocketAddr,
        t: &[u8],
        token: &[u8],
        nodes: Option<CompactNodesV4>,
        peers: Option<Vec<SocketAddr>>,
    ) -> Result<()> {
        let mut rsp = GetPeersResponse::new(&self.my_id);
        rsp.set_t(t).set_token(token);

        if let Some(nodes) = nodes {
            rsp.set_nodes(nodes);
        }
        if let Some(peers) = peers {
            rsp.set_values(peers.into_iter().map(Into::into).collect());
        }

        let buf = to_bytes(&rsp)?;
        self.sock.send_to(&buf, dest).await?;
        Ok(())
    }

    async fn send_get_peers(
        &self,
        dest: &SocketAddr,
        tid: &[u8],
        info_hash: &HashId,
    ) -> Result<()> {
        debug!(dest = ?dest, "send get peer");

        let mut query = GetPeersQuery::new(&self.my_id);
        query.set_t(tid).set_info_hash(info_hash);

        let buf = to_bytes(&query)?;
        self.sock.send_to(&buf, dest).await?;
        Ok(())
    }

    async fn send_announce_peer(
        &self,
        dest: SocketAddr,
        t: &[u8],
        info_hash: &HashId,
        port: u16,
    ) -> Result<()> {
        debug!(dest = ?dest, "reply announce peer");

        let mut rsp = AnnouncePeerQuery::new(&self.my_id);
        rsp.set_t(t).set_info_hash(info_hash).set_port(port);

        let buf = to_bytes(&rsp)?;

        self.sock.send_to(&buf, dest).await?;

        Ok(())
    }

    async fn reply_announce_peer(&self, dest: &SocketAddr, t: &[u8]) -> Result<()> {
        debug!(dest = ?dest, "reply announce peer");

        let mut rsp = AnnouncePeerResponse::new(&self.my_id);
        rsp.set_t(t);

        let buf = to_bytes(&rsp)?;

        self.sock.send_to(&buf, dest).await?;

        Ok(())
    }
}

pub struct DHTInner {
    sender: DHTSender,
    my_id: Arc<HashId>,
    buckets: Vec<Bucket>,
    peer_storage: BTreeMap<HashId, BTreeSet<SocketAddr>>,
    token_manager: TokenManager,
    last_new_node_at: SystemTime,
    search: BTreeMap<HashId, SearchSession>,
    stats: DHTStats,
    routing_table_path: PathBuf,
}

impl DHTInner {
    fn new(my_id: Arc<HashId>, sock: Arc<UdpSocket>, routing_table_path: &Path) -> Self {
        Self {
            my_id: Arc::clone(&my_id),
            buckets: vec![Bucket::new(HashId::ZERO_V1)],
            peer_storage: Default::default(),
            last_new_node_at: SystemTime::UNIX_EPOCH,
            token_manager: TokenManager::new(),
            search: Default::default(),
            sender: DHTSender::new(sock, Arc::clone(&my_id)),
            stats: Default::default(),
            routing_table_path: routing_table_path.to_path_buf(),
        }
    }

    #[instrument(skip(self))]
    async fn new_node(&mut self, addr: &SocketAddr, id: &HashId) -> Result<()> {
        if id == self.my_id.as_ref() || addr.ip().is_loopback() {
            return Ok(());
        }

        let node = Node::new(*id, *addr);

        for ss in self.search.values_mut() {
            if !ss.continue_search() {
                continue;
            }
            ss.add_node(SearchNode::from(&node));
        }

        self.last_new_node_at = SystemTime::now();

        let bucket_index = self.find_bucket_index(id);
        let bucket = &mut self.buckets[bucket_index];
        bucket.on_change();

        if let Some(_exists_node) = bucket.get_by_id_mut(id) {
            return Ok(());
        }

        if let Some(bad_node) = bucket.bad_node().and_then(|index| bucket.get_mut(index)) {
            *bad_node = node;
            return Ok(());
        }

        if bucket.nodes_num() < 8 {
            debug!("bucket has space, insert new node");
            bucket.add_node(node);
            return Ok(());
        }

        debug!(?bucket.min_id, "bucket is full, split bucket");

        let mut ping_addr = None;
        if let Some(q_node) = bucket
            .problem_node()
            .and_then(|index| bucket.get_mut(index))
        {
            debug!(node = ?q_node, "send ping to questionable node");
            if q_node.should_ping() {
                q_node.on_ping();
                ping_addr = Some(*q_node.addr());
            }
        }

        let new_bucket = bucket.split_half();
        bucket.add_node(node);
        self.buckets.insert(bucket_index + 1, new_bucket);

        if let Some(addr) = ping_addr {
            self.sender.send_ping(&addr).await?;
        }

        Ok(())
    }

    fn find_bucket_index(&self, id: &HashId) -> usize {
        //  6
        // [0, 5, 10]
        let mut target = 0;
        for (i, b) in self.buckets.iter().enumerate() {
            if id < &b.min_id {
                break;
            }
            target = i;
        }
        target
    }

    #[instrument(level="Debug", skip_all, fields(addr=?packet.1))]
    pub async fn on_packet(&mut self, packet: (Vec<u8>, SocketAddr)) -> Result<()> {
        let (buf, addr) = packet;

        match QueryResponse::from_bytes(&buf) {
            Ok(qor) => {
                self.on_recv(&addr, qor).await?;
            }
            Err(err) => {
                error!(error = ?err, addr = ?addr , "bad message")
            }
        }
        Ok(())
    }

    async fn on_recv(&mut self, addr: &SocketAddr, qor: QueryResponse) -> Result<()> {
        match qor {
            QueryResponse::Query(q) => match q {
                Query::Ping(q) => {
                    self.stats.ping_query += 1;
                    self.update_node(q.get_id(), Node::on_query);
                    self.on_ping_query(addr, &q).await?;
                }
                Query::FindNode(q) => {
                    self.stats.find_node_query += 1;
                    self.update_node(q.get_id(), Node::on_query);
                    self.on_find_node_query(addr, &q).await?;
                }
                Query::GetPeers(q) => {
                    self.stats.get_peers_query += 1;
                    self.update_node(q.get_id(), Node::on_query);
                    self.on_get_peers_query(addr, &q).await?;
                }
                Query::AnnouncePeer(q) => {
                    self.stats.announce_peer_query += 1;
                    self.update_node(q.get_id(), Node::on_query);
                    self.on_announce_peer_query(addr, &q).await?;
                }
            },
            QueryResponse::Response(r) => match r {
                Response::Ping(rsp) => {
                    self.stats.ping_response += 1;
                    self.update_node(rsp.get_id(), Node::on_response);
                    self.on_ping_response(addr, &rsp).await?;
                }
                Response::FindNode(rsp) => {
                    self.stats.find_node_response += 1;
                    self.update_node(rsp.get_id(), Node::on_response);
                    self.on_find_node_response(addr, &rsp).await?;
                }
                Response::AnnouncePeer(rsp) => {
                    self.stats.announce_peer_response += 1;
                    self.update_node(rsp.get_id(), Node::on_response);
                    self.on_announce_peer_response(addr, &rsp).await?;
                }
                Response::GetPeers(rsp) => {
                    self.stats.get_peers_response += 1;
                    self.update_node(rsp.get_id(), Node::on_response);
                    self.on_get_peers_response(addr, &rsp).await?;
                }
            },
            QueryResponse::Err(e) => {
                self.stats.error += 1;
                error!(error = ?e, "dht error");
            }
        }
        Ok(())
    }

    async fn search_info_hash(&mut self, hash: &HashId) -> Result<()> {
        let nodes: Vec<SearchNode> = {
            let mut res = vec![];
            for bucket in self.buckets.iter() {
                for node in bucket.nodes().iter() {
                    if node.is_active() {
                        res.push(SearchNode::from(node));
                    }
                }
            }
            res
        };

        // let start_search = |session: &mut SearchSession| {
        //     debug!(session_id = ?session.id(), hash = ?hex::encode(hash), "try search hash");
        //     for node in nodes {
        //         session.add_node(node.clone());
        //     }
        //     session.start();
        // };

        let search = &mut self.search;

        if let Some(session) = search.get_mut(hash) {
            debug!(hash = ?hash, "use exists search session");
            session.add_many_nodes(nodes.into_iter());
            session.start();
            return Ok(());
        }

        if let Some(session) =
            search
                .iter_mut()
                .find_map(|(_, s)| if !s.continue_search() { Some(s) } else { None })
        {
            debug!(hash = ?hash, "use empty hash search session");
            session.reset(*hash);
            session.add_many_nodes(nodes.into_iter());
            session.start();
            return Ok(());
        }

        if search.len() < 100 {
            let len = search.len() as u8;
            let session = search
                .entry(*hash)
                .or_insert_with(|| SearchSession::new(len, *hash));
            session.add_many_nodes(nodes.into_iter());
            session.start();
            return Ok(());
        }

        Err(Error::Generic("too many concurrent search session".into()))
    }

    async fn on_ping_query(&mut self, addr: &SocketAddr, query: &PingQuery) -> Result<()> {
        self.new_node(addr, query.get_id()).await?;
        self.sender.reply_ping(query.get_t(), addr).await?;
        Ok(())
    }

    async fn on_ping_response(&mut self, addr: &SocketAddr, rsp: &PingResponse) -> Result<()> {
        debug!(addr = ?addr, "pong");
        self.new_node(addr, rsp.get_id()).await?;
        Ok(())
    }

    async fn on_find_node_query(&mut self, addr: &SocketAddr, query: &FindNodeQuery) -> Result<()> {
        self.new_node(addr, query.get_id()).await?;

        let index = self.find_bucket_index(query.get_id());

        let compact_nodes: CompactNodesV4 = CompactNodesV4::from_v4(
            self.buckets[index]
                .nodes()
                .iter()
                .map(SocketAddrWithId::from)
                .collect(),
        )?;

        self.sender
            .reply_find_node(query.get_t(), addr, compact_nodes)
            .await?;

        Ok(())
    }

    async fn on_find_node_response(
        &mut self,
        addr: &SocketAddr,
        rsp: &FindNodeResponse,
    ) -> Result<()> {
        self.new_node(addr, rsp.get_id()).await?;

        for info in rsp.nodes() {
            self.new_node(info.get_addr(), info.get_id()).await?
        }

        Ok(())
    }

    async fn on_get_peers_query(&mut self, addr: &SocketAddr, q: &GetPeersQuery) -> Result<()> {
        self.new_node(addr, q.get_id()).await?;
        let storage = &mut self.peer_storage;
        let storage = storage
            .entry(*q.get_info_hash())
            .or_insert_with(BTreeSet::new);

        if !storage.is_empty() {
            // send peer info that we know
            let peers: Vec<SocketAddr> = storage.iter().take(100).cloned().collect();
            self.sender
                .reply_get_peers(
                    addr,
                    q.get_t(),
                    &self.token_manager.make_token(addr),
                    None,
                    Some(peers),
                )
                .await?;
            return Ok(());
        }

        // we don't have peer info about queried info_hash, send node info instead

        let bucket = &self.buckets[self.find_bucket_index(q.get_id())];
        let compact_nodes =
            CompactNodesV4::from_v4(bucket.nodes().iter().map(SocketAddrWithId::from).collect())?;

        self.sender
            .reply_get_peers(
                addr,
                q.get_t(),
                &self.token_manager.make_token(addr),
                Some(compact_nodes),
                None,
            )
            .await?;
        Ok(())
    }
    async fn on_get_peers_response(
        &mut self,
        addr: &SocketAddr,
        rsp: &GetPeersResponse,
    ) -> Result<()> {
        self.new_node(addr, rsp.get_id()).await?;

        let mut searched_hash = None;
        for ss in self.search.values() {
            if ss.get_peers_tid() == rsp.get_t() {
                searched_hash = Some(*ss.info_hash());
                break;
            }
        }

        if let Some(hash) = searched_hash {
            let ss = self.search.get_mut(&hash).unwrap();

            let mut new_peers = vec![];
            let mut new_nodes = vec![];

            if let Some(node) = ss.find_mut_node(rsp.get_id()) {
                node.on_reply(rsp.get_token());

                debug!(peer = ?addr, ?hash, ?rsp, "on get peers response");

                // values contains peer info
                for peer in rsp.get_values() {
                    new_peers.push(peer);
                }

                // closest node info
                for node in rsp.get_nodes().deref() {
                    new_nodes.push(node);
                }
            }

            for node in new_nodes {
                self.new_node(node.get_addr(), node.get_id()).await?;
            }

            if let Some(storage) = self.peer_storage.get_mut(&hash) {
                const MAX_PEERS: usize = 100;
                let permit = MAX_PEERS - storage.len();
                storage.extend(new_peers.into_iter().map(SocketAddr::from).take(permit));
            }
        }
        Ok(())
    }

    async fn on_announce_peer_query(
        &mut self,
        addr: &SocketAddr,
        q: &AnnouncePeerQuery,
    ) -> Result<()> {
        self.new_node(addr, q.get_id()).await?;

        let token_match = self.token_manager.verify(addr, q.get_token());

        debug!(addr = ?addr, token_match = ? token_match, "on announce peer query");

        if !token_match {
            self.sender.reply_error(addr, 201, "token mismatch").await?;
            return Ok(());
        }

        let entry = self
            .peer_storage
            .entry(*q.get_info_hash())
            .or_insert_with(BTreeSet::new);

        entry.insert(*addr);

        self.sender.reply_announce_peer(addr, q.get_t()).await?;
        Ok(())
    }

    async fn on_announce_peer_response(
        &mut self,
        addr: &SocketAddr,
        rsp: &AnnouncePeerResponse,
    ) -> Result<()> {
        self.new_node(addr, rsp.get_id()).await?;

        for ss in self.search.values_mut() {
            if ss.get_announce_peers_tid() != rsp.get_t() {
                continue;
            }
            if let Some(node) = ss.find_mut_node(rsp.get_id()) {
                node.on_announce_peer_response()
            }
        }

        Ok(())
    }

    async fn on_tick(&mut self) -> Result<()> {
        self.do_ping_tick().await?;
        self.do_find_node_tick().await?;
        self.do_search_tick().await?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn do_ping_tick(&mut self) -> Result<()> {
        let mut ping_addr = vec![];
        for bucket in self.buckets.iter_mut() {
            for node in bucket.mut_nodes() {
                if node.should_ping() {
                    node.on_ping();
                    ping_addr.push(*node.addr());
                }
            }
        }

        let mut ping_handles = vec![];
        for addr in ping_addr.into_iter() {
            let sender = self.sender.clone();
            let handle = tokio::spawn(async move {
                let addr = addr;
                sender.send_ping(&addr).await;
            });
            ping_handles.push(handle);
        }

        for handle in ping_handles {
            handle.await.map_err(|e| Error::Generic(e.to_string()))?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn do_find_node_tick(&mut self) -> Result<()> {
        let find_node = match self.last_new_node_at.elapsed() {
            Ok(dur) => dur > Duration::from_secs(15),
            _ => true,
        };

        if !find_node {
            return Ok(());
        }

        if let Some((i, j)) = self.random_node() {
            debug!("try to find new nodes");
            let random_id = self.random_closest_id();

            let node = &mut self.buckets[i].get_mut(j).unwrap();
            node.on_query();
            self.sender.send_find_node(node.addr(), &random_id).await?;
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn do_search_tick(&mut self) -> Result<()> {
        let mut pending_search: Vec<(SocketAddr, [u8; 4], HashId)> = vec![];
        let mut max_concurrent_search_cnt = 10;
        {
            let search = &mut self.search;
            for session in search.values_mut() {
                if !session.continue_search() {
                    continue;
                }
                let hash = *session.info_hash();
                let tid = session.get_peers_tid();
                for node in session.mut_nodes() {
                    if max_concurrent_search_cnt < 0 {
                        continue;
                    }
                    if !node.has_replied() {
                        node.on_send_query();
                        max_concurrent_search_cnt -= 1;
                        pending_search.push((*node.addr(), tid, hash));
                    }
                }
            }
        }

        if !pending_search.is_empty() {
            debug!(pending = ?pending_search, "pending search peer");
        }

        let storage = &mut self.peer_storage;
        for pending in pending_search {
            let (addr, tid, hash) = pending;

            storage.entry(hash).or_insert_with(BTreeSet::new);

            self.sender.send_get_peers(&addr, &tid, &hash).await?;
        }

        Ok(())
    }

    async fn load_routing_table(&mut self) -> Result<()> {
        use tokio::io::AsyncReadExt;

        debug!(?self.routing_table_path, "loading routing table");

        if !self.routing_table_path.exists() {
            debug!(?self.routing_table_path, "routing table not exists, skip");
            return Ok(());
        }

        let mut nodes: Vec<Node> = vec![];

        let file = fs::File::options()
            .read(true)
            .open(&self.routing_table_path)?;
        let file = tokio::fs::File::from(file);
        let mut buf = tokio::io::BufReader::new(file);

        loop {
            let ver = buf.read_u8().await;
            match ver {
                Ok(4) => {
                    let mut node_buf = [0u8; 6];
                    buf.read_exact(&mut node_buf).await?;
                    let node = SocketAddrWithId::from_bytes(&node_buf)?;
                    nodes.push(node.into());
                }
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => {
                    break;
                }
                other => todo!("{:?}", other),
            }
        }

        debug!("loaded {} nodes", nodes.len());

        for node in nodes {
            self.new_node(node.addr(), node.id()).await?;
        }

        Ok(())
    }

    async fn save_routing_table(&self) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let t0 = Instant::now();

        let file = fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.routing_table_path)?;

        debug!(?self.routing_table_path, "start save routing table");

        let file = tokio::fs::File::from(file);
        let mut buf = tokio::io::BufWriter::new(file);
        for bucket in self.buckets.iter() {
            for node in bucket.nodes().iter() {
                let addr_with_id = SocketAddrWithId::new(node.id(), node.addr());
                buf.write_all_buf(&mut addr_with_id.to_bytes().as_slice())
                    .await?;
            }
        }
        buf.flush().await?;

        let cost = Instant::now() - t0;
        debug!(?cost, "save routing table done");
        Ok(())
    }

    fn random_closest_id(&self) -> HashId {
        let mut id = *self.my_id.clone();
        id[19] = random::<u8>();
        id
    }

    fn closest_good_node(&self) -> Option<&Node> {
        let mut good_nodes = vec![];
        for bucket in self.buckets.iter() {
            for node in bucket.nodes().iter() {
                if node.is_active() {
                    good_nodes.push((self.my_id.distance(node.id()), node));
                }
            }
        }
        good_nodes.sort_by(|a, b| a.0.cmp(&b.0));
        good_nodes.first().map(|(_, node)| *node)
    }

    fn update_node<F, T>(&mut self, id: &HashId, f: F) -> Option<T>
    where
        F: Fn(&mut Node) -> T,
    {
        for bucket in self.buckets.iter_mut() {
            if id < &bucket.min_id {
                continue;
            }
            if let Some(node) = bucket.get_by_id_mut(id) {
                return Some(f(node));
            }
        }
        None
    }

    async fn refresh_stats(&mut self) -> DHTStats {
        let bucket_stats: Vec<BucketStats> = {
            self.buckets
                .iter_mut()
                .map(|b| b.refresh_stats().clone())
                .collect()
        };

        let search_stats: Vec<SearchSessionStats> = {
            self.search
                .values_mut()
                .map(|ss| ss.refresh_stats())
                .collect()
        };

        let mut stats = &mut self.stats;
        stats.node_num = bucket_stats.iter().map(|b| b.nodes_num).sum();
        stats.active_num = bucket_stats.iter().map(|b| b.active_num).sum();
        stats.no_reply_num = bucket_stats.iter().map(|b| b.no_reply_num).sum();
        stats.query_acc_range = (
            bucket_stats
                .iter()
                .map(|b| b.query_acc_range[0])
                .min()
                .unwrap_or(0),
            bucket_stats
                .iter()
                .map(|b| b.query_acc_range[1])
                .max()
                .unwrap_or(0),
        );
        stats.search_session_replied_num = search_stats.iter().filter(|s| s.all_replied).count();
        stats.buckets_stats = bucket_stats;
        stats.search_session_stats = search_stats;

        stats.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn random_node(&self) -> Option<(usize, usize)> {
        use rand::Rng;

        assert_ne!(self.buckets.len(), 0);

        let mut rng = rand::thread_rng();

        let b_index = rng.gen_range(0..self.buckets.len());

        if self.buckets[b_index].nodes_num() == 0 {
            return None;
        }

        let n_index = rng.gen_range(0..self.buckets[b_index].nodes_num());

        Some((b_index, n_index))
    }
}

#[derive(Clone)]
pub struct DHT {
    inner: Arc<RwLock<DHTInner>>,
    cancel: CancellationToken,
}

impl DHT {
    pub fn new(sock: Arc<UdpSocket>, my_id: &HashId, routing_table_path: &Path) -> Self {
        let dht = Self {
            inner: Arc::new(RwLock::new(DHTInner::new(
                Arc::new(*my_id),
                sock,
                routing_table_path,
            ))),
            cancel: CancellationToken::new(),
        };
        {
            let dht_clone = dht.clone();
            tokio::spawn(dht_clone.tick());
        }
        dht
    }

    async fn tick(self) {
        let t = async {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                {
                    let mut inner = self.inner.write().await;
                    inner.on_tick().await?;
                }
            }
        };
        tokio::select! {
            r = t => {
                let r : Result<()> = r;
                match r {
                    Ok(()) => {}
                    Err(err) => {
                        error!(?err, "dht tick")
                    }
                }
            },
            _ = self.cancel.cancelled() => {}
        }
    }

    pub async fn on_packet(&self, packet: (Vec<u8>, SocketAddr)) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.on_packet(packet).await?;
        Ok(())
    }

    pub async fn ping(&self, addr: &SocketAddr) -> Result<()> {
        let inner = self.inner.read().await;
        inner.sender.send_ping(addr).await?;
        Ok(())
    }

    pub async fn send_get_peers(&self, addr: &SocketAddr, info_hash: &HashId) -> Result<()> {
        let inner = self.inner.read().await;

        if let Some(tid) = inner.search.get(info_hash).map(|ss| ss.get_peers_tid()) {
            inner.sender.send_get_peers(addr, &tid, info_hash).await?;
        }

        Ok(())
    }

    pub async fn search_info_hash(&self, info_hash: &HashId) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.search_info_hash(info_hash).await?;
        Ok(())
    }

    pub async fn get_peers(&self, info_hash: &HashId) -> Vec<SocketAddr> {
        let inner = self.inner.read().await;
        inner
            .peer_storage
            .get(info_hash)
            .map(|storage| storage.iter().cloned().collect())
            .unwrap_or_else(Vec::new)
    }

    pub async fn announce(&self, info_hash: &HashId, public_addr: SocketAddr) -> Result<()> {
        let inner = self.inner.read().await;
        let sender = inner.sender.clone();
        let info_hash_arc = Arc::new(*info_hash);
        for bucket in inner.buckets.iter() {
            for node in bucket.nodes().iter() {
                if node.is_active() {
                    let dest_addr = *node.addr();
                    let sender_clone = sender.clone();
                    let info_hash_arc = Arc::clone(&info_hash_arc);
                    tokio::spawn(async move {
                        let info_hash_arc = info_hash_arc;
                        match sender_clone
                            .send_announce_peer(dest_addr, &[], &info_hash_arc, public_addr.port())
                            .await
                        {
                            Ok(()) => {}
                            Err(err) => {
                                error!(?err, "dht announce")
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        let inner = self.inner.read().await;
        inner.shutdown().await.unwrap();
        Ok(())
    }

    pub async fn load_routing_table(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.load_routing_table().await?;
        Ok(())
    }

    pub async fn save_routing_table(&self) -> Result<()> {
        let inner = self.inner.read().await;
        inner.save_routing_table().await?;
        Ok(())
    }
}
