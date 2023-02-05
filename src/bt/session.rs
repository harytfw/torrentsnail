use crate::bt::file::{build_file_map, FilePieceMap};
use crate::bt::peer::Peer;
use crate::bt::piece::{Piece, PieceFragment};
use crate::bt::types::{
    BTExtMessage, BTHandshake, BTMessage, UTMetadataMessage, UTMetadataPieceData,
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
use tokio_util::sync::CancellationToken;
use torrent::{HashId, TorrentInfo};
use tracing::{debug, error};

const MSG_UT_METADATA: &str = "ut_metadata";
const METADATA_PIECE_SIZE: usize = 16384;

#[derive(Debug, Clone, Default)]
pub enum DownloadState {
    #[default]
    Pause,
    Downloading,
}

#[derive(Clone)]
pub struct SessionState {
    download_state: DownloadState,
    max_metadata_req: usize,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            download_state: DownloadState::Pause,
            max_metadata_req: 4,
        }
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
}

impl TorrentSession {
    pub fn from_torrent(bt_weak: Weak<BT>, torrent: TorrentFile) -> Result<Self> {
        let bt_arc = bt_weak.upgrade().unwrap();
        let info_hash = Arc::new(torrent.info_hash().unwrap());
        let pieces = Piece::from_length(torrent.info.total_length(), torrent.info.piece_length);
        let handshake_template = Self::build_handshake(&bt_arc.my_id, &info_hash);
        let files = build_file_map(&torrent.info);
        debug!(?info_hash, "new torrent session from info");

        let s = Self {
            bt_weak,
            peers: Arc::new(RwLock::new(vec![])),
            pieces: Arc::new(RwLock::new(pieces)),
            info_hash: Arc::clone(&info_hash),
            cancel: CancellationToken::new(),
            state: Default::default(),
            torrent_metadata_pieces: Arc::new(RwLock::new(vec![])),
            handshake_template: Arc::new(handshake_template),
            file_maps: Arc::new(RwLock::new(files)),
            torrent: Arc::new(RwLock::new(Some(torrent))),
        };
        s.start_tick();
        Ok(s)
    }

    pub fn from_info_hash(bt: Weak<BT>, info_hash: HashId) -> Result<Self> {
        let bt_arc = bt.upgrade().unwrap();

        let info_hash = Arc::new(info_hash);

        let handshake_template = Self::build_handshake(&bt_arc.my_id, &info_hash);

        debug!(?info_hash, "new torrent session from info hash");

        let state = SessionState {
            ..Default::default()
        };

        let s = Self {
            bt_weak: bt,
            peers: Arc::new(RwLock::new(vec![])),
            pieces: Arc::new(RwLock::new(vec![])),
            info_hash: Arc::clone(&info_hash),
            cancel: CancellationToken::new(),
            state: Arc::new(RwLock::new(state)),
            torrent_metadata_pieces: Arc::new(RwLock::new(vec![])),
            handshake_template: Arc::new(handshake_template),
            file_maps: Default::default(),
            torrent: Arc::new(RwLock::new(None)),
        };
        s.start_tick();
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

    pub async fn active_handshake(&self, mut tcp: TcpStream) -> Result<Peer> {
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

    fn start_tick(&self) {
        tokio::spawn(self.clone().tick_announce());
        tokio::spawn(self.clone().tick_check_peer());
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
        tokio::spawn(self.clone().tick_peer_message(peer.clone(), msg_rx));
        Ok(peer)
    }

    async fn announce_tracker_event(&self, event: tracker::Event) -> Result<()> {
        tokio::spawn(self.clone().announce_tracker_event_inner(event));
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

    pub async fn get_state(&self) -> DownloadState {
        todo!()
    }

    async fn pending_fragments(&self, peer: &Peer) -> VecDeque<PieceFragment> {
        const MAX_FRAGMENTS: usize = 100;

        let pieces = self.pieces.read().await;
        let mut fragments = vec![];
        for piece in pieces.iter() {
            if piece.should_send_req_to_peer(peer) {
                fragments.append(&mut piece.fragments());
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
            BTMessage::Request(_info) => {
                peer.send_choke().await?;
            }
            BTMessage::Piece(data) => {
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
                        done = piece.is_all_done();
                        if done {
                            let torrent = self.torrent.read().await;

                            let expected_sha1 =
                                torrent.as_ref().unwrap().info.get_pieces_sha1(index);

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
                } else {
                    debug!(index = ?index, "sha1 not match, re-download");
                }

                if all_verified {
                    debug!("all verified");
                    self.stop().await?;
                }
            }
            BTMessage::Have(index) => {
                let mut state = peer.state.write().await;
                state.owned_piece_indices.insert(*index as usize);
            }
            BTMessage::Cancel(_info) => {}
            BTMessage::BitField(_fields) => {}
            BTMessage::Ping => {
                peer.send_ping().await?;
            }
            BTMessage::Ext(ext_msg) => {
                self.handle_ext_msg(peer, ext_msg).await?;
            }
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
                _ => {
                    todo!()
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

                peer.send_message((msg_id, msg)).await?;
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
                if metadata_pieces.iter().all(Piece::is_all_done) {
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
                *torrent = Some(TorrentFile::new(metadata.clone()))
            }
            let file_maps: Vec<FilePieceMap>;
            {
                debug!(?total_len, ?piece_len, "construct pieces");
                let mut pieces = self.pieces.write().await;
                *pieces = Piece::from_length(total_len, piece_len);
                file_maps = build_file_map(&metadata);
            }
            {
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
                peer.send_interested().await?;
                return Ok(state.broken);
            }
            return Ok(state.broken);
        }

        if state.available_fragment_req == 0 || pending.is_empty() {
            return Ok(state.broken);
        }

        let mut req_msgs = vec![];

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
            req_msgs.push(frag.into());
        }

        if !req_msgs.is_empty() {
            peer.send_interested().await?;
            for msg in req_msgs {
                peer.send_request(msg).await?;
            }
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
                    debug!(len = ?pending.len(), "pending");
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
                for (peer, err) in broken_peers {
                    debug!(peer = ?peer.addr, err= ?err, "remove broken peer");
                    tokio::spawn(async move {
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
        Ok(())
    }
}
