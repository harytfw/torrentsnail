use crate::config::Config;
use crate::dht::DHT;
use crate::lsd::LSD;
use crate::magnet::MagnetURI;
use crate::message::{
    BTHandshake, BTMessage, LTDontHaveMessage, PieceData, PieceInfo, UTMetadataMessage,
    MSG_LT_DONTHAVE, MSG_UT_METADATA,
};
use crate::proxy::socks5::Socks5Client;
use crate::session::manager::PieceActivityManager;
use crate::session::peer::Peer;
use crate::session::storage::StorageManager;
use crate::session::utils::make_announce_key;
use crate::torrent::TorrentFile;
use crate::tracker::TrackerClient;
use crate::{torrent, tracker, Error, Result, SNAIL_VERSION};
use core::fmt;
use num::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use torrent::HashId;
use tracing::{debug, error, info, instrument};

const METADATA_PIECE_SIZE: usize = 16384;

#[derive(Default)]
pub struct TorrentSessionBuilder {
    info_hash: Option<HashId>,
    torrent_path: Option<PathBuf>,
    torrent: Option<TorrentFile>,
    storage_dir: Option<PathBuf>,
    check_files: bool,
    dht: Option<DHT>,
    lsd: Option<LSD>,
    my_id: Option<HashId>,
    listen_addr: Option<SocketAddr>,
    config: Option<Arc<Config>>,
}

impl TorrentSessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_uri(self, uri: &str) -> Result<Self> {
        if uri.starts_with("magnet:") {
            let magnet = MagnetURI::from_uri(uri)?;
            let xt_hash = magnet
                .xt_hash()
                .ok_or_else(|| Error::Generic("no info hash".to_string()))?;
            let info_hash = HashId::from_hex(xt_hash)?;
            let mut b = self.with_info_hash(info_hash);
            if let Some(dn) = magnet.dn.as_ref() {
                b = b.with_display_name(dn);
            }
            Ok(b)
        } else if uri.starts_with("file:") {
            Err(Error::Generic("not support file protocol".to_string()))
        } else {
            Err(Error::Generic(format!("not support uri: {uri}")))
        }
    }

    pub fn with_display_name(self, _name: &str) -> Self {
        // TODO:
        self
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

    pub fn with_dht(self, dht: DHT) -> Self {
        Self {
            dht: Some(dht),
            ..self
        }
    }

    pub fn with_lsd(self, lsd: LSD) -> Self {
        Self {
            lsd: Some(lsd),
            ..self
        }
    }

    pub fn with_my_id(self, my_id: HashId) -> Self {
        Self {
            my_id: Some(my_id),
            ..self
        }
    }

    pub fn with_listen_addr(self, listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr: Some(listen_addr),
            ..self
        }
    }

    pub fn with_config(self, config: Arc<Config>) -> Self {
        Self {
            config: Some(config),
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

    pub async fn build(mut self) -> Result<TorrentSession> {
        let my_id = self
            .my_id
            .ok_or_else(|| Error::Generic("no my_id".to_string()))?;

        let mut pm: StorageManager;
        let mut metadata_cache: StorageManager;
        let piece_log_man = PieceActivityManager::new();
        let metadata_log_man = PieceActivityManager::new();

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
            pm = StorageManager::from_torrent_info(&storage_dir, &torrent.info)?;
            let metadata_buf = bencode::to_bytes(torrent.get_origin_info().unwrap())?;
            metadata_cache = StorageManager::from_single_file(
                &torrent_path,
                metadata_buf.len(),
                METADATA_PIECE_SIZE,
            )?;
            metadata_cache.assume_checked();

            if self.check_files {
                let paths: Vec<PathBuf> = pm.paths().iter().map(|p| p.to_path_buf()).collect();
                for path in paths {
                    let pass = pm
                        .check_file(&path, |i| torrent.info.get_piece_sha1(i))
                        .await?;
                    debug!(?path, ?pass, "check file");
                }
            }
        } else {
            pm = StorageManager::empty();
            metadata_cache = StorageManager::empty();
        }

        let handshake_template = Self::build_handshake(&my_id, &info_hash);

        let (peer_conn_req_tx, peer_addr_rx) = mpsc::channel(10);
        let (peer_piece_req_tx, peer_piece_req_rx) = mpsc::channel(50);
        let (peer_to_poll_tx, peer_to_poll_rx) = mpsc::channel(100);

        if info_hash.is_zero() {
            return Err(Error::Generic("no info hash".into()));
        }

        let tracker = TrackerClient::new();

        let ts = TorrentSession {
            name: torrent
                .as_ref()
                .map(|t| t.info.name.to_string())
                .unwrap_or_else(|| info_hash.hex()),
            tracker,
            peers: Arc::new(dashmap::DashMap::new()),
            info_hash: Arc::new(info_hash),
            metadata_sm: Arc::new(RwLock::new(metadata_cache)),
            handshake_template: Arc::new(handshake_template),
            torrent: Arc::new(RwLock::new(torrent)),
            peer_conn_req_tx,
            peer_piece_req_tx,
            sm: Arc::new(RwLock::new(pm)),
            piece_activity_man: Arc::new(RwLock::new(piece_log_man)),
            metadata_piece_activity_man: Arc::new(RwLock::new(metadata_log_man)),
            long_term_tasks: Default::default(),
            short_term_tasks: Default::default(),
            cancel: CancellationToken::new(),
            status: Arc::new(AtomicU32::new(TorrentSessionStatus::Started as u32)),
            storage_dir: Arc::new(storage_dir),
            lsd: self.lsd.unwrap(),
            dht: self.dht.unwrap(),
            my_id: self.my_id.unwrap(),
            listen_addr: self.listen_addr.unwrap(),
            cfg: self.config.unwrap(),
            peer_to_poll_tx,
        };
        {
            let ts_clone = ts.clone();
            tokio::spawn(async move {
                ts_clone
                    .start_tick(peer_addr_rx, peer_piece_req_rx, peer_to_poll_rx)
                    .await;
            });
        }
        info!(?info_hash, "new torrent session");
        Ok(ts)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TorrentSessionStatus {
    Stopped = 0,
    Started,
    Seeding,
}

impl FromPrimitive for TorrentSessionStatus {
    fn from_i64(num: i64) -> Option<Self> {
        Self::from_u64(num as u64)
    }

    fn from_u64(num: u64) -> Option<Self> {
        let s = match num {
            0 => Self::Started,
            1 => Self::Stopped,
            2 => Self::Seeding,
            _ => return None,
        };
        Some(s)
    }
}

impl ToPrimitive for TorrentSessionStatus {
    fn to_i64(&self) -> Option<i64> {
        Some(*self as i64)
    }

    fn to_u64(&self) -> Option<u64> {
        Some(*self as u64)
    }
}

impl fmt::Display for TorrentSessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Seeding => "seeding",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for TorrentSessionStatus {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "started" => Ok(Self::Started),
            "stopped" => Ok(Self::Stopped),
            "seeding" => Ok(Self::Seeding),
            _ => Err(Error::Generic(format!("invalid status: {}", s))),
        }
    }
}

impl Serialize for TorrentSessionStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TorrentSessionStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// TODO: reduce struct size
#[derive(Clone)]
pub struct TorrentSession {
    pub info_hash: Arc<HashId>,
    pub torrent: Arc<RwLock<Option<TorrentFile>>>,
    pub(crate) storage_dir: Arc<PathBuf>,

    peers: Arc<dashmap::DashMap<HashId, Peer>>,
    pub(crate) handshake_template: Arc<BTHandshake>,
    long_term_tasks: Arc<RwLock<JoinSet<()>>>,
    short_term_tasks: Arc<RwLock<JoinSet<()>>>,
    pub(crate) sm: Arc<RwLock<StorageManager>>,
    pub(crate) metadata_sm: Arc<RwLock<StorageManager>>,
    pub(crate) piece_activity_man: Arc<RwLock<PieceActivityManager>>,
    pub(crate) metadata_piece_activity_man: Arc<RwLock<PieceActivityManager>>,

    peer_conn_req_tx: mpsc::Sender<SocketAddr>,
    pub(crate) peer_piece_req_tx: mpsc::Sender<(Peer, PieceInfo)>,
    status: Arc<AtomicU32>,
    cancel: CancellationToken,
    my_id: HashId,
    listen_addr: SocketAddr,
    tracker: TrackerClient,
    dht: DHT,
    lsd: LSD,
    cfg: Arc<Config>,
    peer_to_poll_tx: mpsc::Sender<HashId>,
    pub name: String,
}

impl Debug for TorrentSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentSession")
            .field("info_hash", &self.info_hash)
            .finish()
    }
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
    pub async fn active_handshake_with_addr(&self, addr: impl ToSocketAddrs) -> Result<Peer> {
        let tcp = TcpStream::connect(addr).await?;
        self.active_handshake_with_tcp(tcp).await
    }

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    pub async fn active_handshake_with_tcp(&self, mut tcp: TcpStream) -> Result<Peer> {
        tcp.write_all(&self.handshake_template.to_bytes()).await?;

        let peer_handshake = BTHandshake::from_reader_async(&mut tcp).await?;

        if self.info_hash != peer_handshake.info_hash {
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
        peer_to_poll_rx: mpsc::Receiver<HashId>,
    ) {
        let mut long_term = self.long_term_tasks.write().await;
        long_term.spawn(self.clone().tick_announce());
        long_term.spawn(self.clone().tick_check_peer());
        long_term.spawn(self.clone().tick_peer_req(peer_req_rx));
        long_term.spawn(self.clone().tick_connect_peer(peer_addr_rx));
        long_term.spawn(self.clone().tick_consume_short_term_task());
        long_term.spawn(self.clone().poll_peer(peer_to_poll_rx));
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
        let (peer, msg_rx) = Peer::attach(peer_handshake.clone(), tcp, Default::default()).await?;
        debug!(peer = ?peer, "attach new peer");
        if !self.peers.contains_key(&peer.peer_id) {
            self.peers.insert(peer.peer_id, peer.clone());
        } else {
            peer.shutdown().await?;
            return Err(Error::Generic("duplicate peer".into()));
        }
        {
            let pm = self.sm.read().await;
            if !pm.checked_bits().is_empty() {
                peer.send_message_now(BTMessage::BitField(pm.checked_bits().to_bytes()))
                    .await?;
            }
        }
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

    async fn announce_tracker_event_inner(self, event: tracker::Event) {
        let t = async {
            let mut req = tracker::AnnounceRequest::new();
            req.set_key(make_announce_key(&self.my_id, &self.info_hash))
                .set_ip_address(&self.listen_addr)
                .set_info_hash(&self.info_hash)
                .set_peer_id(&self.my_id)
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
                                self.dht.send_get_peers(&peer, &self.info_hash).await?;

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

    async fn connect_peer_addr(&mut self, addr: SocketAddr) -> Result<()> {
        let proxy_config = self.cfg.network.proxy.clone();
        let tcp: TcpStream;
        debug!("try active handshake");
        if let Some(proxy_config) = proxy_config.as_ref() {
            match proxy_config.r#type.as_str() {
                "socks5" => {
                    let proxy_client = Socks5Client::new(proxy_config.to_socks5_addr().unwrap());
                    tcp = proxy_client.connect(addr).await?;
                }
                _ => {
                    return Err(Error::Generic("not support proxy type".into()));
                }
            }
        } else {
            // TODO: handle io error
            tcp = TcpStream::connect(addr).await?;
        }
        match self.active_handshake_with_tcp(tcp).await {
            Ok(_peer) => {
                debug!("active handshake success");
            }
            Err(e) => {
                error!(err = ?e, "connect peer");
            }
        };
        Ok(())
    }

    async fn search_peer_and_connect(&mut self) -> Result<()> {
        let exists_peers = {
            let m: HashSet<SocketAddr> = self.peers.iter().map(|peer| peer.addr).collect();
            m
        };
        let peers = self.dht.get_peers(&self.info_hash).await;

        'inner: for addr in peers {
            if exists_peers.contains(&addr) {
                continue 'inner;
            }
            self.connect_peer_addr(addr).await?;
        }
        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn do_announce(&mut self) -> Result<()> {
        debug!("announce lsd");
        self.lsd.announce(&self.info_hash).await?;
        self.dht.announce(&self.info_hash, self.listen_addr).await?;
        self.search_peer_and_connect().await?;
        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn tick_announce(mut self) {
        let cancel = self.cancel.clone();

        let t = async {
            loop {
                self.do_announce().await?;
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
            _ = cancel.cancelled() => {}
        }
    }

    pub(crate) async fn on_piece_arrived(
        &self,
        peer: &Peer,
        data: &PieceData,
    ) -> Result<(), Error> {
        let index = data.index;
        let _begin = data.begin;

        let sha1 = {
            let torrent = self.torrent.read().await;
            let info = torrent.as_ref().map(|t| &t.info).unwrap();
            info.get_piece_sha1(index).to_vec()
        };

        let mut all_checked = false;

        let complete_piece = {
            let mut log_man = self.piece_activity_man.write().await;
            log_man.on_piece_data(data.index, data.begin, &data.fragment, &peer.peer_id)
        };

        if let Some(piece) = complete_piece {
            let mut sm = self.sm.write().await;
            sm.write(piece).await?;
            let checked = sm.check(index, &sha1).await?;
            all_checked = sm.all_checked();
            if checked {
                peer.send_message_now(BTMessage::Have(index as u32)).await?;
            }
            if all_checked {
                sm.flush().await?;
            }
        }
        if all_checked {
            self.stop().await?;
        }
        Ok(())
    }

    async fn handle_req_metadata(&self, peer_id: HashId) -> Result<()> {
        // bep-009
        let peer = self
            .peers
            .get(&peer_id)
            .ok_or(Error::Generic("peer not found".to_owned()))?;

        let mut metadata_pm = self.metadata_sm.write().await;

        if metadata_pm.total_size() == 0 {
            if let Some(total_len) = peer
                .handshake
                .ext_handshake
                .as_ref()
                .and_then(|ext| ext.get_metadata_size())
            {
                debug!(?total_len, "create torrent metadata piece manager");
                *metadata_pm = StorageManager::from_single_file(
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
            let mut metadata_log_man = self.metadata_piece_activity_man.write().await;
            metadata_log_man.sync(&mut metadata_pm)?;
            metadata_log_man.pull_req(&peer.peer_id)
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

    #[instrument(skip_all, fields(peer_id=?peer_id))]
    async fn handle_peer(&self, peer_id: HashId) -> Result<()> {
        let torrent_exists = { self.torrent.read().await.is_some() };

        if !torrent_exists && self.support_ut_metadata(peer_id) {
            self.handle_req_metadata(peer_id).await?;
            return Ok(());
        }

        let peer = self.peers.iter().find(|p| p.peer_id == peer_id).unwrap();

        let state = { peer.state.read().await.clone() };

        let pending_piece_info = {
            let mut man = self.piece_activity_man.write().await;
            man.pull_req(&peer.peer_id)
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
        debug!("tick check peer");
        let t = async {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                {
                    let mut log_man = self.piece_activity_man.write().await;
                    let mut pm = self.sm.write().await;
                    log_man.sync(&mut pm)?;
                }
                // let mut broken_peers = vec![];
                let candidate_peer_id =
                    { self.peers.iter().map(|p| p.peer_id).collect::<Vec<_>>() };

                for peer_id in candidate_peer_id {
                    debug!(?peer_id, "check peer");
                    self.peer_to_poll_tx.send(peer_id).await.unwrap();
                }
            }
        };

        let cancel = self.cancel.clone();
        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                if let Err(err) = r {
                    error!(?err, "tick check peer")
                }
            },
            _ =  cancel.cancelled() => {
                debug!("tick_check_peer stop")
            }
        }
    }

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    async fn poll_peer(self, mut rx: mpsc::Receiver<HashId>) {
        let t = async {
            loop {
                match rx.try_recv() {
                    Ok(peer_id) => {
                        debug!(?peer_id, "poll peer");
                        if let Some(peer) = self.peers.get(&peer_id) {
                            let state = peer.state.read().await;
                            let mut log_man = self.piece_activity_man.write().await;
                            log_man.sync_peer_pieces(&peer.peer_id, &state.owned_pieces)?;
                        }
                        self.handle_peer(peer_id).await?;

                        let mut is_broken = false;

                        if let Some(peer) = self.peers.get(&peer_id) {
                            if let Some(broken_reason) = peer.state.read().await.broken.as_ref() {
                                info!(?broken_reason, ?peer_id, "peer broken");
                                peer.shutdown().await?;
                                is_broken = true;
                            }
                        }
                        if is_broken {
                            self.peers.remove(&peer_id);
                        }
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
                        let mut pm = self.sm.write().await;
                        let index = info.index;
                        if pm.is_checked(index) {
                            let piece = pm.fetch(index).await?;
                            let piece_data = PieceData::new(
                                index,
                                info.begin,
                                &piece.buf()[info.begin..info.begin + info.length],
                            );

                            peer.send_message_now(BTMessage::from(piece_data)).await?;
                        } else if let Some(id) = peer.get_msg_id(MSG_LT_DONTHAVE) {
                            let donthave = LTDontHaveMessage::new(info.index);
                            peer.send_message_now(BTMessage::from((id, donthave)))
                                .await?;
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
    async fn tick_consume_short_term_task(self) {
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

    fn support_ut_metadata(&self, peer_id: HashId) -> bool {
        let peer = match self.peers.get(&peer_id) {
            Some(peer) => peer,
            None => return false,
        };
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
        self.active_handshake_with_addr(addr).await
    }

    pub fn status(&self) -> TorrentSessionStatus {
        TorrentSessionStatus::from_u32(self.status.load(Ordering::Acquire)).unwrap()
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    pub async fn start(&self) -> Result<()> {
        {
            let pm = self.sm.read().await;
            let checked_num = (0..pm.piece_num()).filter(|&i| pm.is_checked(i)).count();
            debug!(piece_num=?pm.piece_num(), ?checked_num)
        }
        self.dht.search_info_hash(&self.info_hash).await?;
        self.announce_tracker_event(tracker::Event::Started).await?;
        self.status
            .store(TorrentSessionStatus::Started as u32, Ordering::Release);
        Ok(())
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    pub async fn stop(&self) -> Result<()> {
        self.announce_tracker_event(tracker::Event::Stopped).await?;
        {
            for peer in self.peers.iter() {
                peer.shutdown().await?;
            }
            self.peers.clear();
        }
        {
            let mut pm = self.sm.write().await;
            pm.flush().await?;
        }
        self.status
            .store(TorrentSessionStatus::Stopped as u32, Ordering::Release);

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
            let mut cache = self.sm.write().await;
            if let Err(err) = cache.flush().await {
                error!(?err)
            }
        }
        Ok(())
    }

    pub fn piece_manager(&self) -> Arc<RwLock<StorageManager>> {
        Arc::clone(&self.sm)
    }

    pub fn tracker(&self) -> TrackerClient {
        self.tracker.clone()
    }

    pub fn peers(&self) -> Arc<dashmap::DashMap<HashId, Peer>> {
        Arc::clone(&self.peers)
    }
}
