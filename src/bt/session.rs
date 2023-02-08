use crate::bt::file::{build_file_piece_map, FilePieceMap};
use crate::bt::peer::Peer;
use crate::bt::piece::{Piece, PieceFragment};
use crate::bt::types::{
    BTExtMessage, BTHandshake, BTMessage, PieceData, PieceInfo, UTMetadataMessage,
    UTMetadataPieceData,
};
use crate::bt::BT;
use crate::torrent::TorrentFile;
use crate::{bencode, torrent, tracker, Error, Result, SNAIL_VERSION};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use torrent::{HashId, TorrentInfo};
use tracing::{debug, error, warn};

const MSG_UT_METADATA: &str = "ut_metadata";
const METADATA_PIECE_SIZE: usize = 16384;

#[derive(Debug, Clone)]
pub struct SessionState {}

impl Default for SessionState {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Clone)]
pub struct TorrentSession {
    pub info_hash: Arc<HashId>,
    pub torrent: Arc<RwLock<Option<TorrentFile>>>,
    bt_weak: Weak<BT>,
    peers: Arc<RwLock<Vec<Peer>>>,
    pieces: Arc<RwLock<Vec<Piece>>>,
    torrent_metadata_pieces: Arc<RwLock<Vec<Piece>>>,
    state: Arc<RwLock<SessionState>>,
    handshake_template: Arc<BTHandshake>,
    cancel: CancellationToken,
    file_maps: Arc<RwLock<Vec<FilePieceMap>>>,
    peer_conn_req_tx: mpsc::Sender<SocketAddr>,
    peer_piece_req_tx: mpsc::Sender<(Peer, PieceInfo)>,
    long_term_tasks: Arc<RwLock<JoinSet<()>>>,
    short_term_tasks: Arc<RwLock<JoinSet<()>>>,
}

impl TorrentSession {
    pub async fn from_torrent(bt: Weak<BT>, torrent: TorrentFile) -> Result<Self> {
        Self::build(bt, torrent.info_hash().unwrap(), Some(torrent)).await
    }

    pub async fn from_info_hash(bt: Weak<BT>, info_hash: HashId) -> Result<Self> {
        Self::build(bt, info_hash, None).await
    }

    async fn build(bt: Weak<BT>, info_hash: HashId, torrent: Option<TorrentFile>) -> Result<Self> {
        let bt_arc = bt.upgrade().unwrap();

        let info_hash = Arc::new(info_hash);
        let handshake_template = Self::build_handshake(&bt_arc.my_id, &info_hash);

        let pieces: Vec<Piece>;
        let files: Vec<FilePieceMap>;
        if let Some(torrent) = torrent.as_ref() {
            pieces = Piece::from_length(torrent.info.total_length(), torrent.info.piece_length);
            files = build_file_piece_map(&torrent.info);
        } else {
            pieces = vec![];
            files = vec![];
        }

        debug!(?info_hash, "new torrent session from info hash");

        let (peer_conn_req_tx, peer_addr_rx) = mpsc::channel(10);
        let (peer_piece_req_tx, peer_piece_req_rx) = mpsc::channel(50);

        let s = Self {
            bt_weak: bt,
            peers: Arc::new(RwLock::new(vec![])),
            pieces: Arc::new(RwLock::new(pieces)),
            info_hash: Arc::clone(&info_hash),
            cancel: CancellationToken::new(),
            state: Default::default(),
            torrent_metadata_pieces: Arc::new(RwLock::new(vec![])),
            handshake_template: Arc::new(handshake_template),
            file_maps: Arc::new(RwLock::new(files)),
            torrent: Arc::new(RwLock::new(torrent)),
            peer_conn_req_tx,
            peer_piece_req_tx,
            long_term_tasks: Arc::new(RwLock::new(JoinSet::new())),
            short_term_tasks: Default::default(),
        };
        s.start_tick(peer_addr_rx, peer_piece_req_rx).await;
        Ok(s)
    }

    pub async fn passive_handshake(
        &self,
        peer_handshake: BTHandshake,
        mut tcp: TcpStream,
    ) -> Result<Peer> {
        tcp.write_all(&self.handshake_template.to_bytes()).await?;

        if peer_handshake.extension.get_ext_handshake() {
            return self.ext_handshake(peer_handshake, tcp).await;
        }

        let peer = self.on_handshake_done(peer_handshake, tcp).await?;
        Ok(peer)
    }

    async fn active_handshake(&self, mut tcp: TcpStream) -> Result<Peer> {
        tcp.write_all(&self.handshake_template.to_bytes()).await?;

        let peer_handshake = BTHandshake::from_reader_async(&mut tcp).await?;

        if !self.info_hash.is_same(&peer_handshake.info_hash) {
            return Err(Error::Generic(
                "info hash not match during handshake".into(),
            ));
        }

        if self.handshake_template.extension.get_ext_handshake()
            && peer_handshake.extension.get_ext_handshake()
        {
            return self.ext_handshake(peer_handshake, tcp).await;
        }

        let peer = self.on_handshake_done(peer_handshake, tcp).await?;
        Ok(peer)
    }

    async fn start_tick(
        &self,
        peer_addr_rx: mpsc::Receiver<SocketAddr>,
        peer_req_rx: mpsc::Receiver<(Peer, PieceInfo)>,
    ) {
        let mut long_term = self.long_term_tasks.write().await;
        long_term.spawn(self.clone().tick_announce());
        long_term.spawn(self.clone().tick_check_peer());
        long_term.spawn(self.clone().tick_peer_req(peer_req_rx));
        long_term.spawn(self.clone().tick_connect_peer(peer_addr_rx));
        long_term.spawn(self.clone().tick_consume_short_term());
    }

    fn bt(&self) -> Arc<BT> {
        self.bt_weak.upgrade().expect("bt is destroyed")
    }

    fn build_handshake(my_id: &HashId, info_hash: &HashId) -> BTHandshake {
        let mut handshake = BTHandshake::new(my_id, info_hash);

        handshake.set_ext_handshake(true);
        {
            let ext = handshake.ext_handshake.as_mut().unwrap();
            ext.set_msg(2, MSG_UT_METADATA).set_version(SNAIL_VERSION);
        }
        handshake
    }

    async fn ext_handshake(
        &self,
        mut peer_handshake: BTHandshake,
        mut tcp: TcpStream,
    ) -> Result<Peer> {
        let mut ext_handshake = self.handshake_template.ext_handshake.clone().unwrap();

        if let Ok(addr) = tcp.peer_addr() {
            ext_handshake.set_youip(addr.ip().into());
        }

        tcp.write_all(&BTMessage::from(ext_handshake).to_bytes())
            .await?;

        let msg = BTMessage::from_reader_async(&mut tcp).await?;

        match msg {
            BTMessage::Ext(ext) => {
                let peer_ext_handshake = ext.into_handshake()?;
                debug!(?peer_ext_handshake, peer_id=?peer_handshake.peer_id);
                peer_handshake.ext_handshake = Some(peer_ext_handshake);
            }
            _ => return Err(Error::Handshake("not extended handshake message".into())),
        }

        self.on_handshake_done(peer_handshake, tcp).await
    }

    async fn on_handshake_done(&self, peer_handshake: BTHandshake, tcp: TcpStream) -> Result<Peer> {
        let (peer, msg_rx) = Peer::attach(peer_handshake.clone(), tcp).await?;
        debug!(peer = ?peer, "attach new peer");
        let mut peers = self.peers.write().await;

        let exists = peers
            .iter()
            .any(|peer| peer.peer_id.is_same(&peer_handshake.peer_id));
        if !exists {
            peers.push(peer.clone());
        } else {
            peer.shutdown().await?;
            return Err(Error::Generic("duplicate peer".into()));
        }
        peers.push(peer.clone());
        let mut short_term = self.short_term_tasks.write().await;
        short_term.spawn(self.clone().tick_peer_message(peer.clone(), msg_rx));
        Ok(peer)
    }

    async fn announce_tracker_event(&self, event: tracker::Event) -> Result<()> {
        let mut short_term = self.short_term_tasks.write().await;
        short_term.spawn(self.clone().announce_tracker_event_inner(event));
        Ok(())
    }

    // bep007 -  `The key should remain the same for a particular infohash during a torrent session. `
    pub fn make_announce_key(&self) -> u32 {
        let mut hasher = DefaultHasher::new();
        self.bt().my_id.hash(&mut hasher);
        self.info_hash.hash(&mut hasher);
        let hash = hasher.finish();
        (hash >> 32) as u32
    }

    async fn announce_tracker_event_inner(self, event: tracker::Event) {
        let t = async {
            let mut req = tracker::AnnounceRequest::new();
            req.set_key(self.make_announce_key())
                .set_ip_address(&self.bt().listen_addr)
                .set_info_hash(&self.info_hash)
                .set_peer_id(&self.bt().my_id)
                .set_num_want(10)
                .set_event(event);
            debug!(req = ?req, "announce tracker");
            let mut rx = self.bt().tracker.send_announce(&req).await;
            loop {
                match rx.try_recv() {
                    Ok(result) => match result {
                        Ok((url, rsp)) => {
                            for peer in rsp.peers() {
                                debug!(peer = ?peer, url = ?url, "new peer from tracker");
                                self.bt().dht.send_get_peers(&peer, &self.info_hash).await?;

                                self.peer_conn_req_tx.send(peer).await?;
                            }
                            // TODO:
                        }
                        Err(err) => {
                            // TODO: ignore skip announce
                            error!(?err, "tracker send announce");
                        }
                    },
                    Err(mpsc::error::TryRecvError::Empty) => tokio::task::yield_now().await,

                    Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
                }
            }
        };
        tokio::select! {
            r = t => {
                let r : Result<(),Error> = r;
                if let Err(err) = r {
                    error!(?err, "announce tracker event")
                }
            },
            _ = self.cancel.cancelled() => {}
        }
    }

    pub async fn get_state(&self) -> SessionState {
        todo!()
    }

    async fn pending_fragments(&self, peer: &Peer) -> VecDeque<PieceFragment> {
        const MAX_FRAGMENTS: usize = 100;

        let pieces = self.pieces.read().await;
        let mut fragments = vec![];
        for piece in pieces.iter() {
            if piece.should_send_req_to_peer(peer) {
                fragments.append(&mut piece.pending_fragments());
            }
            if fragments.len() > MAX_FRAGMENTS {
                break;
            }
        }
        fragments.sort_by_key(|frag| frag.get_weight());
        fragments.into_iter().collect()
    }

    async fn tick_announce(self) {
        let t = async {
            loop {
                self.bt().lsd.announce(&self.info_hash).await?;
                let exists_peers = {
                    let peers = self.peers.read().await;
                    let m: HashSet<SocketAddr> =
                        peers.iter().map(|peer| *peer.addr.as_ref()).collect();
                    m
                };
                let peers = self.bt().dht.get_peers(&self.info_hash).await;

                debug!(info_hash = ?self.info_hash, peers = ?peers," get peers");

                'inner: for addr in peers {
                    if exists_peers.contains(&addr) {
                        continue 'inner;
                    }
                    let tcp = TcpStream::connect(addr).await?;
                    debug!(local_addr =?tcp.local_addr(), peer_addr = ?tcp.peer_addr(), "try active handshake");
                    match self.active_handshake(tcp).await {
                        Ok(_peer) => {}
                        Err(e) => {
                            error!(err = ?e, "connect peer");
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            }
        };
        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                match r {
                    Ok(()) => {}
                    Err(e) => error!(err = ?e, "tick announce")
                }
            },
            _ = self.cancel.cancelled() => {}
        }
    }

    async fn handle_message(&self, peer: &Peer, msg: &BTMessage) -> Result<()> {
        match msg {
            BTMessage::Choke => {
                let mut state = peer.state.write().await;
                state.choke = true;
            }
            BTMessage::Unchoke => {
                let mut state = peer.state.write().await;
                state.choke = false;
            }
            BTMessage::Interested => {
                let mut state = peer.state.write().await;
                state.interested = true
            }
            BTMessage::NotInterested => {
                let mut state = peer.state.write().await;
                state.interested = false
            }
            BTMessage::Request(info) => {
                self.peer_piece_req_tx
                    .send((peer.clone(), info.clone()))
                    .await?;
            }
            BTMessage::Piece(data) => {
                self.on_piece_arrive(peer, data).await?;
            }
            BTMessage::Have(index) => {
                let index = *index as usize;
                let mut state = peer.state.write().await;
                if state.owned_pieces.is_empty() {
                    let pieces = self.pieces.read().await;
                    state.owned_pieces = bit_vec::BitVec::from_elem(pieces.len(), false)
                }
                if index < state.owned_pieces.len() {
                    state.owned_pieces.set(index, true);
                }
            }
            BTMessage::Cancel(_info) => {}
            BTMessage::BitField(fields) => {
                let mut state = peer.state.write().await;
                state.owned_pieces = bit_vec::BitVec::from_bytes(fields);
            }
            BTMessage::Ping => {
                peer.send_message_now(BTMessage::Ping).await?;
            }
            BTMessage::Ext(ext_msg) => {
                self.handle_ext_msg(peer, ext_msg).await?;
            }
            BTMessage::Unknown(id) => {
                warn!(?id, "unknown msg id")
            }
        }
        Ok(())
    }

    async fn on_piece_arrive(&self, peer: &Peer, data: &PieceData) -> Result<(), Error> {
        let mut done = false;
        let mut verified = false;
        let mut all_verified = false;
        let index = data.index as usize;
        {
            let mut pieces = self.pieces.write().await;
            if let Some(piece) = pieces.get_mut(index) {
                if piece.is_downloading() {
                    piece.on_data(data.begin as usize, &data.fragment)?;
                }
                done = piece.is_all_received();
                if done {
                    let torrent = self.torrent.read().await;

                    let expected_sha1 = torrent.as_ref().unwrap().info.get_pieces_sha1(index);

                    verified = piece.on_verify_sha1(expected_sha1);
                }
            }
        }
        {
            let mut state = peer.state.write().await;
            state.available_fragment_req += 1;
        }
        if done && verified {
            debug!(index = ?index, "sha1 verified");

            let pieces = self.pieces.read().await;

            let piece = pieces.get(index).unwrap();

            let file_maps = self.file_maps.read().await;
            for map in file_maps.iter() {
                if map.support_piece(piece) {
                    debug!(?map, "flush to file");
                    map.accept(&[piece])?;
                }
            }

            let pending_cnt = pieces.iter().filter(|p| p.is_pending()).count();
            let downloading_cnt = pieces.iter().filter(|p| p.is_downloading()).count();
            let verified_cnt = pieces.iter().filter(|p| p.is_verified()).count();
            all_verified = pieces.iter().all(Piece::is_verified);
            debug!(?pending_cnt, ?downloading_cnt, ?verified_cnt);
        }
        if done && !verified {
            warn!(index = ?index, "sha1 not match, this piece will re-download soon");
        }
        if all_verified {
            debug!("all verified");
            self.stop().await?;
        }
        Ok(())
    }

    async fn handle_ext_msg(&self, peer: &Peer, ext_msg: &BTExtMessage) -> Result<()> {
        let msg_name = peer
            .handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_name(ext_msg.id));
        if let Some(msg_name) = msg_name {
            match msg_name {
                MSG_UT_METADATA => {
                    let metadata_msg = UTMetadataMessage::from_bytes(&ext_msg.payload)?;
                    self.handle_ut_metadata_msg(peer, &metadata_msg).await?;
                }
                msg_name => {
                    debug!(?msg_name, "ignore unsupported ext msg");
                }
            }
        }
        Ok(())
    }

    async fn handle_ut_metadata_msg(&self, peer: &Peer, msg: &UTMetadataMessage) -> Result<()> {
        debug!(?msg, MSG_UT_METADATA);

        let mut torrent_metadata: Option<TorrentInfo> = None;
        match msg {
            UTMetadataMessage::Request(index) => {
                let msg: UTMetadataMessage = {
                    let index = *index;
                    let metadata_pieces = self.torrent_metadata_pieces.read().await;

                    if let Some(piece) = metadata_pieces.get(index) {
                        let total_size = metadata_pieces.iter().map(Piece::size).sum();
                        UTMetadataMessage::Data(UTMetadataPieceData {
                            piece: index,
                            total_size,
                            payload: piece.get_buf().to_vec(),
                        })
                    } else {
                        UTMetadataMessage::Reject(index)
                    }
                };

                let msg_id = self
                    .handshake_template
                    .ext_handshake
                    .as_ref()
                    .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
                    .unwrap();

                peer.send_message_now((msg_id, msg)).await?;
            }

            UTMetadataMessage::Reject(index) => {
                let mut pieces = self.torrent_metadata_pieces.write().await;
                if let Some(piece) = pieces.get_mut(*index) {
                    piece.add_band_peer_id(Arc::clone(&peer.peer_id));
                }
            }

            UTMetadataMessage::Data(piece_data) => {
                let mut metadata_pieces = self.torrent_metadata_pieces.write().await;
                if let Some(piece) = metadata_pieces.get_mut(piece_data.piece) {
                    piece.on_data(0, &piece_data.payload)?;
                }
                if metadata_pieces.iter().all(Piece::is_all_received) {
                    let total_size = metadata_pieces.iter().map(Piece::size).sum();
                    let mut total_buf: Vec<u8> = Vec::with_capacity(total_size);
                    for piece in metadata_pieces.iter() {
                        total_buf.extend_from_slice(piece.get_buf())
                    }
                    torrent_metadata = bencode::from_bytes(&total_buf)?;
                }
            }
        }

        if let Some(metadata) = torrent_metadata {
            let computed_info_hash = metadata.info_hash()?;
            debug!(?metadata, "full metadata");
            if !self.info_hash.is_same(&computed_info_hash) {
                debug!("info hash not match");
                let mut metadata_pieces = self.torrent_metadata_pieces.write().await;
                metadata_pieces.clear();
                return Ok(());
            }
            let total_len = metadata.total_length();
            let piece_len = metadata.piece_length;
            {
                debug!("save torrent info");
                let mut torrent = self.torrent.write().await;
                *torrent = Some(TorrentFile::from_info(metadata.clone()))
            }
            {
                debug!(?total_len, ?piece_len, "construct pieces");
                let mut pieces = self.pieces.write().await;
                *pieces = Piece::from_length(total_len, piece_len);
            }
            {
                let file_maps = build_file_piece_map(&metadata);
                let mut files = self.file_maps.write().await;
                *files = file_maps
            }
        }
        Ok(())
    }

    async fn handle_req_metadata(&self, peer: &Peer) -> Result<bool> {
        // bep-009
        {
            let torrent = self.torrent.read().await;
            if torrent.is_some() {
                return Ok(false);
            }
        }
        {
            let mut pieces = self.torrent_metadata_pieces.write().await;

            if pieces.is_empty() {
                if let Some(total_len) = peer
                    .handshake
                    .ext_handshake
                    .as_ref()
                    .and_then(|ext| ext.get_metadata_size())
                {
                    *pieces = Piece::from_length(total_len, METADATA_PIECE_SIZE);
                }
            }

            if pieces.iter().all(Piece::is_verified) {
                return Ok(false);
            }

            let msg_id = peer
                .handshake
                .ext_handshake
                .as_ref()
                .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
                .unwrap();
            debug!(?peer, "send metadata req");
            let max_req_cnt = 3;
            for piece in pieces
                .iter_mut()
                .filter(|p| p.should_send_req_to_peer(peer))
                .take(max_req_cnt)
            {
                piece.on_send_req();
                let req = UTMetadataMessage::Request(piece.get_index());
                peer.send_message((msg_id, req)).await?;
            }
            peer.flush().await?;
        }
        Ok(false)
    }

    async fn handle_peer(
        &self,
        peer: &Peer,
        pending: &mut VecDeque<PieceFragment>,
    ) -> Result<Option<String>> {
        let state = { peer.state.read().await.clone() };

        if self.support_ut_metadata(peer) {
            let handled = self.handle_req_metadata(peer).await?;
            if handled {
                return Ok(state.broken);
            }
        }

        if state.choke {
            if !pending.is_empty() {
                peer.send_message_now(BTMessage::Interested).await?;
                return Ok(state.broken);
            }
            return Ok(state.broken);
        }

        if state.available_fragment_req == 0 || pending.is_empty() {
            return Ok(state.broken);
        }

        let mut req_piece_info = vec![];

        while !pending.is_empty() {
            {
                let mut state = peer.state.write().await;
                if state.available_fragment_req == 0 {
                    break;
                }
                state.available_fragment_req -= 1;
            }
            let frag = pending.pop_front().unwrap();
            let mut pieces = self.pieces.write().await;
            let piece = pieces.get_mut(frag.get_index()).unwrap();
            piece.on_send_req();
            req_piece_info.push(frag.into());
        }

        if !req_piece_info.is_empty() {
            peer.send_message(BTMessage::Interested).await?;
            for piece_info in req_piece_info {
                peer.send_message(BTMessage::Request(piece_info)).await?;
            }
            peer.flush().await?;
        }

        Ok(state.broken)
    }

    async fn tick_check_peer(self) {
        let t = async {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                let mut broken_peers = vec![];
                let peers = { self.peers.read().await.clone() };
                for peer in peers {
                    let mut pending = self.pending_fragments(&peer).await;
                    if let Some(reason) = self.handle_peer(&peer, &mut pending).await? {
                        broken_peers.push((peer, reason));
                    }
                }

                if broken_peers.is_empty() {
                    continue;
                }
                {
                    let broken_addr: HashSet<SocketAddr> =
                        broken_peers.iter().map(|(p, _)| *p.addr).collect();

                    let mut peers = self.peers.write().await;
                    peers.retain(|p| !broken_addr.contains(&p.addr))
                }
                let mut short_term = self.short_term_tasks.write().await;
                for (peer, err) in broken_peers {
                    debug!(peer = ?peer.addr, err= ?err, "remove broken peer");
                    short_term.spawn(async move {
                        match peer.shutdown().await {
                            Ok(()) => {}
                            Err(err) => {
                                error!(?err, "shutdown broken peer");
                            }
                        }
                    });
                }
            }
        };

        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                if let Err(err) = r {
                    error!(?err, "tick check peer")
                }
            },
            _ =  self.cancel.cancelled() => {
                debug!("tick_check_peer stop")
            }
        }
    }

    async fn tick_connect_peer(self, mut rx: mpsc::Receiver<SocketAddr>) {
        let t = async {
            loop {
                while let Some(addr) = rx.recv().await {
                    match self.add_peer_with_addr(addr).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!(?addr, ?err, "connect peer tick");
                        }
                    }
                }
            }
        };

        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                if let Err(err) = r {
                    error!(?err, "tick connect peer")
                }
            },
            _ =  self.cancel.cancelled() => {
                debug!("tick connect peer stop")
            }
        }
    }
    async fn tick_peer_message(self, peer: Peer, mut bt_msg_rx: mpsc::Receiver<BTMessage>) {
        let t = async {
            loop {
                while let Some(msg) = bt_msg_rx.recv().await {
                    self.handle_message(&peer, &msg).await?;
                }
            }
        };

        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                if let Err(err) = r {
                    error!(?err, "tick check peer message")
                }
            },
            _ = self.cancel.cancelled() => {
                debug!("tick_peer_message stop")
            }
        }
    }

    async fn tick_peer_req(self, mut peer_req_rx: mpsc::Receiver<(Peer, PieceInfo)>) {
        let t = async {
            loop {
                while let Some((peer, info)) = peer_req_rx.recv().await {
                    let pieces = self.pieces.read().await;
                    if let Some(p) = pieces.iter().find(|p| p.get_index() == info.index as usize) {
                        let frags = p.finished_fragments(info.begin as usize, info.length as usize);
                        let msgs = frags.into_iter().map(|f| {
                            BTMessage::Piece(PieceData {
                                index: f.get_index() as u32,
                                begin: f.get_begin() as u32,
                                fragment: p.get_buf()[f.get_begin()..f.get_begin() + f.get_len()]
                                    .to_vec(),
                            })
                        });
                        for msg in msgs {
                            peer.send_message(msg).await?;
                        }
                        peer.flush().await?;
                    }
                }
            }
            Ok(())
        };

        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                if let Err(err) = r {
                    error!(?err)
                }
            },
            _ = self.cancel.cancelled() => { }
        }
    }

    async fn tick_consume_short_term(self) {
        use tokio::time::sleep;
        use tokio::time::Duration;
        'next_round: loop {
            // wait 5s or it was cancelled
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    return
                }
                _ = sleep(Duration::from_secs(5)) => {}
            }
            let mut short_term = self.short_term_tasks.write().await;
            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        return
                    }
                    r = short_term.join_next() => {
                        if r.is_none() {
                            // no tasks
                            continue 'next_round;
                        }
                        // one task is complete
                    },
                    else => {
                        // task is incomplete, wait next round
                        continue 'next_round;
                    }
                }
            }
        }
    }

    fn support_ut_metadata(&self, peer: &Peer) -> bool {
        match (
            &self.handshake_template.ext_handshake,
            &peer.handshake.ext_handshake,
        ) {
            (Some(self_ext), Some(peer_ext)) => {
                self_ext.get_m().contains_key(MSG_UT_METADATA)
                    && peer_ext.get_msg_id(MSG_UT_METADATA).is_some()
            }
            (_, _) => false,
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.bt().dht.search_info_hash(&self.info_hash).await?;
        self.announce_tracker_event(tracker::Event::Started).await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        self.announce_tracker_event(tracker::Event::Stopped).await?;
        {
            for peer in self.peers.write().await.drain(..) {
                peer.shutdown().await?;
            }
        }
        Ok(())
    }

    pub async fn delete(&self) -> Result<()> {
        self.stop().await?;
        self.cancel.cancel();
        Ok(())
    }

    pub async fn add_peer_with_addr(&self, addr: impl ToSocketAddrs) -> Result<Peer> {
        let tcp = TcpStream::connect(addr).await?;
        debug!(local_addr =?tcp.local_addr(), peer_addr = ?tcp.peer_addr(), "manually add peer");
        self.active_handshake(tcp).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.stop().await?;
        self.cancel.cancel();
        {
            let mut short_term = self.short_term_tasks.write().await;
            while short_term.join_next().await.is_some() {}
        }
        {
            let mut long_term = self.long_term_tasks.write().await;
            while long_term.join_next().await.is_some() {}
        }
        Ok(())
    }
}
