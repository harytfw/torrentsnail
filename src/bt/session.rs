use crate::bt::peer::Peer;
use crate::bt::piece::{PieceLogManager, PieceManager};
use crate::bt::types::{
    BTExtMessage, BTHandshake, BTMessage, PieceData, PieceInfo, UTMetadataMessage,
    UTMetadataPieceData,
};
use crate::bt::BT;
use crate::torrent::TorrentFile;
use crate::tracker::TrackerClient;
use crate::{bencode, torrent, tracker, Error, Result, SNAIL_VERSION};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use torrent::{HashId, TorrentInfo};
use tracing::{debug, error, instrument, warn};

const MSG_UT_METADATA: &str = "ut_metadata";
const METADATA_PIECE_SIZE: usize = 16384;

#[derive(Default)]
pub struct TorrentSessionBuilder {
    info_hash: Option<HashId>,
    torrent_path: Option<PathBuf>,
    torrent: Option<TorrentFile>,
    storage_dir: Option<PathBuf>,
    bt: Option<Weak<BT>>,
    check_files: bool,
}

impl TorrentSessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_bt(self, bt: Weak<BT>) -> Self {
        Self {
            bt: Some(bt),
            ..self
        }
    }

    pub fn with_info_hash(self, info_hash: HashId) -> Self {
        Self {
            info_hash: Some(info_hash),
            ..self
        }
    }

    pub fn with_torrent_path(self, path: impl AsRef<Path>) -> Self {
        Self {
            torrent_path: Some(path.as_ref().to_path_buf()),
            ..self
        }
    }

    pub fn with_torrent(self, torrent: TorrentFile) -> Self {
        Self {
            torrent: Some(torrent),
            ..self
        }
    }

    pub fn with_storage_dir(self, path: impl AsRef<Path>) -> Self {
        Self {
            storage_dir: Some(path.as_ref().to_path_buf()),
            ..self
        }
    }

    pub fn with_check_files(self, check: bool) -> Self {
        Self {
            check_files: check,
            ..self
        }
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

    pub fn build(mut self) -> Result<TorrentSession> {
        let bt_weak = self.bt.unwrap();
        let bt = bt_weak.upgrade().unwrap();

        let mut pm: PieceManager;
        let mut metadata_cache: PieceManager;
        let piece_log_man = PieceLogManager::new();
        let metadata_log_man = PieceLogManager::new();

        let mut torrent = self.torrent.take();
        let mut info_hash = HashId::ZERO_V1;

        if let Some(hash) = self.info_hash {
            info_hash = hash;
        }

        let temp_dir = std::env::temp_dir();
        let storage_dir: PathBuf = self.storage_dir.unwrap_or_else(|| temp_dir.join("snail"));
        let torrent_path = self
            .torrent_path
            .unwrap_or_else(|| temp_dir.join("snail/tmp.torrent"));

        if torrent_path.try_exists().unwrap() {
            let torrent_file = TorrentFile::from_path(&torrent_path)?;
            torrent = Some(torrent_file);
        }

        if let Some(torrent) = torrent.as_ref() {
            info_hash = torrent.info_hash().unwrap();
            pm = PieceManager::from_torrent_info(&storage_dir, &torrent.info)?;
            let metadata_buf = bencode::to_bytes(torrent.get_origin_info().unwrap())?;
            metadata_cache = PieceManager::from_single_file(
                &torrent_path,
                metadata_buf.len(),
                METADATA_PIECE_SIZE,
            )?;
            metadata_cache.assume_checked();

            if self.check_files {
                let paths: Vec<PathBuf> = pm.paths().iter().map(|p| p.to_path_buf()).collect();
                for path in paths {
                    let pass = pm.check_file(&path, |i| torrent.info.get_piece_sha1(i))?;
                    debug!(?path, ?pass, "check file");
                }
            }
        } else {
            pm = PieceManager::empty();
            metadata_cache = PieceManager::empty();
        }

        let handshake_template = Self::build_handshake(&bt.my_id, &info_hash);

        let (peer_conn_req_tx, peer_addr_rx) = mpsc::channel(10);
        let (peer_piece_req_tx, peer_piece_req_rx) = mpsc::channel(50);

        if info_hash.is_zero() {
            return Err(Error::Generic("no info hash".into()));
        }

        let tracker = TrackerClient::new();

        let ts = TorrentSession {
            bt_weak,
            tracker: Arc::new(tracker),
            peers: Arc::new(RwLock::new(vec![])),
            info_hash: Arc::new(info_hash),
            metadata_pm: Arc::new(Mutex::new(metadata_cache)),
            handshake_template: Arc::new(handshake_template),
            torrent: Arc::new(RwLock::new(torrent)),
            peer_conn_req_tx,
            peer_piece_req_tx,
            piece_manager: Arc::new(Mutex::new(pm)),
            piece_log_man: Arc::new(RwLock::new(piece_log_man)),
            metadata_log_man: Arc::new(RwLock::new(metadata_log_man)),
            long_term_tasks: Default::default(),
            short_term_tasks: Default::default(),
            cancel: CancellationToken::new(),
            status: Arc::new(AtomicU32::new(Status::Started as u32)),
            storage_dir: Arc::new(storage_dir),
        };
        {
            let ts_clone = ts.clone();
            tokio::spawn(async move {
                ts_clone.start_tick(peer_addr_rx, peer_piece_req_rx).await;
            });
        }
        Ok(ts)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Status {
    Stopped,
    Started,
    Seeding,
}

impl Status {
    pub fn to_u32(self) -> u32 {
        match self {
            Self::Started => 0,
            Self::Stopped => 1,
            Self::Seeding => 2,
        }
    }

    pub fn from_u32(num: u32) -> Option<Self> {
        let s = match num {
            0 => Self::Started,
            1 => Self::Stopped,
            2 => Self::Seeding,
            _ => return None,
        };
        Some(s)
    }
}

#[derive(Clone)]
pub struct TorrentSession {
    pub info_hash: Arc<HashId>,
    pub torrent: Arc<RwLock<Option<TorrentFile>>>,
    storage_dir: Arc<PathBuf>,

    bt_weak: Weak<BT>,
    tracker: Arc<TrackerClient>,
    peers: Arc<RwLock<Vec<Peer>>>,
    handshake_template: Arc<BTHandshake>,
    long_term_tasks: Arc<RwLock<JoinSet<()>>>,
    short_term_tasks: Arc<RwLock<JoinSet<()>>>,
    piece_manager: Arc<Mutex<PieceManager>>,
    metadata_pm: Arc<Mutex<PieceManager>>,
    piece_log_man: Arc<RwLock<PieceLogManager>>,
    metadata_log_man: Arc<RwLock<PieceLogManager>>,

    peer_conn_req_tx: mpsc::Sender<SocketAddr>,
    peer_piece_req_tx: mpsc::Sender<(Peer, PieceInfo)>,
    status: Arc<AtomicU32>,
    cancel: CancellationToken,
}

impl TorrentSession {
    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
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

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
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
        {
            let pm = self.piece_manager.lock().await;
            if !pm.checked_bits().is_empty() {
                peer.send_message_now(BTMessage::BitField(pm.checked_bits().to_bytes()))
                    .await?;
            }
        }
        peers.push(peer.clone());
        let mut short_term = self.short_term_tasks.write().await;
        short_term.spawn(self.clone().tick_peer_message(peer.clone(), msg_rx));
        Ok(peer)
    }

    #[instrument(skip_all)]
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
            let mut rx = self.tracker.send_announce(&req).await;
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

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
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

                'inner: for addr in peers {
                    if exists_peers.contains(&addr) {
                        continue 'inner;
                    }
                    // TODO: handle io error
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

    #[instrument(skip_all, fields(peer_id=?peer.peer_id))]
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
                self.on_piece_arrived(peer, data).await?;
            }
            BTMessage::Have(index) => {
                let index = *index as usize;
                let mut state = peer.state.write().await;
                if state.owned_pieces.is_empty() {
                    let num = self.piece_manager.lock().await.piece_num();
                    state.owned_pieces = bit_vec::BitVec::from_elem(num, false)
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

    async fn on_piece_arrived(&self, peer: &Peer, data: &PieceData) -> Result<(), Error> {
        let index = data.index;
        let _begin = data.begin;

        let sha1 = {
            let torrent = self.torrent.read().await;
            let info = torrent.as_ref().map(|t| &t.info).unwrap();
            info.get_piece_sha1(index).to_vec()
        };

        let mut all_checked = false;

        let complete_piece = {
            let mut log_man = self.piece_log_man.write().await;
            log_man.on_piece_data(
                data.index,
                data.begin,
                &data.fragment,
                Arc::clone(&peer.peer_id),
            )
        };

        if let Some(piece) = complete_piece {
            let mut pm = self.piece_manager.lock().await;
            pm.write(piece)?;
            let checked = pm.check(index, &sha1)?;
            all_checked = pm.all_checked();
            if checked {
                peer.send_message_now(BTMessage::Have(index as u32)).await?;
            }
            if all_checked {
                pm.flush()?;
            }
        }
        if all_checked {
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

        let mut torrent_metadata_buf: Option<Vec<u8>> = None;
        match msg {
            UTMetadataMessage::Request(index) => {
                let msg_id = self
                    .handshake_template
                    .ext_handshake
                    .as_ref()
                    .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
                    .unwrap();

                let msg: UTMetadataMessage = {
                    let index = *index;
                    let mut metadata_pm = self.metadata_pm.lock().await;
                    let total_size = metadata_pm.total_size();

                    if metadata_pm.is_checked(index) {
                        let piece = metadata_pm.read(index)?;
                        let piece_data = UTMetadataPieceData {
                            piece: index,
                            total_size,
                            payload: piece.into_buf(),
                        };
                        UTMetadataMessage::Data(piece_data)
                    } else {
                        UTMetadataMessage::Reject(index)
                    }
                };

                peer.send_message_now((msg_id, msg)).await?;
            }

            UTMetadataMessage::Reject(index) => {
                let mut plm = self.metadata_log_man.write().await;
                plm.on_reject(*index, Arc::clone(&peer.peer_id))
            }

            UTMetadataMessage::Data(piece_data) => {
                let piece = {
                    let mut log_man = self.metadata_log_man.write().await;
                    log_man
                        .on_piece_data(
                            piece_data.piece,
                            0,
                            &piece_data.payload,
                            Arc::clone(&peer.peer_id),
                        )
                        .unwrap()
                };
                let index = piece.index();
                let sha1 = piece.sha1();
                let mut metadata_pm = self.metadata_pm.lock().await;
                // TODO: handle error
                metadata_pm.write(piece)?;
                let checked = metadata_pm.check(index, &sha1)?;
                debug!(index, checked);
                if metadata_pm.all_checked() {
                    {
                        let mut log_man = self.metadata_log_man.write().await;
                        log_man.sync(&mut metadata_pm)?;
                    }
                    let mut buf = Vec::with_capacity(metadata_pm.total_size());
                    for i in 0..metadata_pm.piece_num() {
                        let piece = metadata_pm.fetch(i)?;
                        buf.extend(piece.buf())
                    }
                    torrent_metadata_buf = Some(buf);
                }
            }
        }

        if let Some(metadata_buf) = torrent_metadata_buf {
            // FIXME: support sha256
            let computed_info_hash = {
                HashId::try_from(
                    ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &metadata_buf)
                        .as_ref(),
                )
                .unwrap()
            };

            if !self.info_hash.is_same(&computed_info_hash) {
                debug!("info hash not match");
                let mut metadata_pm = self.metadata_pm.lock().await;
                metadata_pm.clear_all_checked_bits()?;
                return Ok(());
            }

            let val: bencode::Value = bencode::from_bytes(&metadata_buf)?;
            let info: TorrentInfo = bencode::from_bytes(&metadata_buf)?;

            // TODO: add atomic bool to prevent session from using partial state

            let total_len = info.total_length();
            let piece_len = info.piece_length;
            {
                debug!(?total_len, ?piece_len, "construct pieces");
                let mut cache = self.piece_manager.lock().await;
                *cache = PieceManager::from_torrent_info(self.storage_dir.as_ref(), &info)?;

                let mut log_man = self.piece_log_man.write().await;
                log_man.sync(&mut cache)?;
            }
            {
                debug!("save torrent info");
                let mut torrent = TorrentFile::from_info(info.clone());
                torrent.set_origin_info(val);
                let mut torrent_lock = self.torrent.write().await;
                *torrent_lock = Some(torrent)
            }
        }
        Ok(())
    }

    async fn handle_req_metadata(&self, peer: &Peer) -> Result<()> {
        // bep-009

        let mut metadata_pm = self.metadata_pm.lock().await;

        if metadata_pm.total_size() == 0 {
            if let Some(total_len) = peer
                .handshake
                .ext_handshake
                .as_ref()
                .and_then(|ext| ext.get_metadata_size())
            {
                debug!(?total_len, "create torrent metadata piece manager");
                *metadata_pm = PieceManager::from_single_file(
                    "/tmp/snail_tmp.torrent",
                    total_len,
                    METADATA_PIECE_SIZE,
                )?;
            }
        }

        if metadata_pm.all_checked() {
            return Ok(());
        }

        let piece_info = {
            let mut metadata_log_man = self.metadata_log_man.write().await;
            metadata_log_man.sync(&mut metadata_pm)?;
            metadata_log_man.pull(Arc::clone(&peer.peer_id))
        };

        let msg_id = peer
            .handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
            .unwrap();

        for info in piece_info {
            debug!(?info);
            let req = UTMetadataMessage::Request(info.index);
            peer.send_message((msg_id, req)).await?;
        }
        peer.flush().await?;

        Ok(())
    }

    #[instrument(skip_all, fields(peer_id=?peer.peer_id))]
    async fn handle_peer(&self, peer: &Peer) -> Result<()> {
        let torrent_exists = { self.torrent.read().await.is_some() };

        if !torrent_exists && self.support_ut_metadata(peer) {
            self.handle_req_metadata(peer).await?;
            return Ok(());
        }

        let state = { peer.state.read().await.clone() };

        let pending_piece_info = {
            let mut log_man = self.piece_log_man.write().await;
            log_man.pull(Arc::clone(&peer.peer_id))
        };

        if state.choke {
            if !pending_piece_info.is_empty() {
                peer.send_message_now(BTMessage::Interested).await?;
            }
            return Ok(());
        }

        if pending_piece_info.is_empty() {
            return Ok(());
        }

        peer.send_message(BTMessage::Interested).await?;
        for piece_info in pending_piece_info {
            peer.send_message(BTMessage::Request(piece_info)).await?;
        }
        peer.flush().await?;

        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn tick_check_peer(self) {
        let t = async {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                {
                    let mut log_man = self.piece_log_man.write().await;
                    let mut pm = self.piece_manager.lock().await;
                    log_man.sync(&mut pm)?;
                }
                let mut broken_peers = vec![];
                let peers = { self.peers.read().await.clone() };

                for peer in peers {
                    self.handle_peer(&peer).await?;
                    let broken = { peer.state.read().await.broken.clone() };
                    if let Some(reason) = broken {
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

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
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

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn tick_peer_message(self, peer: Peer, mut bt_msg_rx: mpsc::Receiver<BTMessage>) {
        let t = async {
            loop {
                match bt_msg_rx.try_recv() {
                    Ok(msg) => {
                        self.handle_message(&peer, &msg).await?;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        tokio::task::yield_now().await;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()),
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

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    async fn tick_peer_req(self, mut peer_req_rx: mpsc::Receiver<(Peer, PieceInfo)>) {
        let t = async {
            loop {
                while let Some((peer, info)) = peer_req_rx.recv().await {
                    {
                        let mut pm = self.piece_manager.lock().await;
                        let index = info.index;
                        if pm.is_checked(index) {
                            let piece = pm.fetch(index)?;
                            let piece_data = PieceData::new(
                                index,
                                info.begin,
                                &piece.buf()[info.begin..info.begin + info.length],
                            );

                            peer.send_message_now(BTMessage::from(piece_data)).await?;
                        }
                    }
                }
            }
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

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn tick_consume_short_term(self) {
        use tokio::time::sleep;
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
                        // all task is incomplete, wait next round
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

    #[instrument(skip_all)]
    pub async fn add_peer_with_addr(&self, addr: impl ToSocketAddrs) -> Result<Peer> {
        let tcp = TcpStream::connect(addr).await?;
        debug!(local_addr =?tcp.local_addr(), peer_addr = ?tcp.peer_addr(), "manually add peer");
        self.active_handshake(tcp).await
    }

    pub fn status(&self) -> Status {
        Status::from_u32(self.status.load(Ordering::Acquire)).unwrap()
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    pub async fn start(&self) -> Result<()> {
        {
            let pm = self.piece_manager.lock().await;
            let checked_num = (0..pm.piece_num()).filter(|&i| pm.is_checked(i)).count();
            debug!(piece_num=?pm.piece_num(), ?checked_num)
        }
        self.bt().dht.search_info_hash(&self.info_hash).await?;
        self.announce_tracker_event(tracker::Event::Started).await?;
        self.status.store(Status::Started as u32, Ordering::Release);
        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    pub async fn stop(&self) -> Result<()> {
        self.announce_tracker_event(tracker::Event::Stopped).await?;
        {
            for peer in self.peers.write().await.drain(..) {
                peer.shutdown().await?;
            }
        }
        {
            let mut pm = self.piece_manager.lock().await;
            pm.flush()?;
        }
        self.status.store(Status::Stopped as u32, Ordering::Release);

        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    pub async fn delete(&self) -> Result<()> {
        self.stop().await?;
        self.cancel.cancel();
        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
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
        {
            let mut cache = self.piece_manager.lock().await;
            if let Err(err) = cache.flush() {
                error!(?err)
            }
        }
        Ok(())
    }

    pub fn piece_manager(&self) -> Arc<Mutex<PieceManager>> {
        Arc::clone(&self.piece_manager)
    }

    pub fn tracker(&self) -> Arc<TrackerClient> {
        Arc::clone(&self.tracker)
    }
}
