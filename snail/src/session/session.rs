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
use crate::session::TorrentSessionStatus;
use crate::tracker::TrackerClient;
use crate::{torrent, tracker, Error, Result};
use num::FromPrimitive;
use std::collections::HashSet;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

fn compute_torrent_path(data_dir: &Path, info_hash: &HashId) -> PathBuf {
    data_dir.join(format!("{}.torrent", info_hash.hex()))
}

// TODO: reduce struct size
#[derive(Clone)]
pub struct TorrentSession {
    pub info_hash: Arc<HashId>,
    pub name: Arc<String>,
    pub my_id: Arc<HashId>,

    pub(crate) data_dir: Arc<PathBuf>,

    pub(crate) peers: Arc<dashmap::DashMap<HashId, Peer>>,
    pub(crate) handshake_template: Arc<BTHandshake>,

    pub(crate) long_term_tasks: Arc<RwLock<JoinSet<()>>>,
    pub(crate) short_term_tasks: Arc<RwLock<JoinSet<()>>>,

    pub(crate) peer_conn_req_tx: mpsc::Sender<SocketAddr>,
    pub(crate) peer_piece_req_tx: mpsc::Sender<(Peer, PieceInfo)>,
    pub(crate) status: Arc<AtomicU32>,
    pub(crate) cancel: CancellationToken,
    pub(crate) listen_addr: Arc<SocketAddr>,
    pub(crate) tracker: TrackerClient,
    pub(crate) dht: DHT,
    pub(crate) lsd: LSD,
    pub(crate) cfg: Arc<Config>,
    pub(crate) peer_to_poll_tx: mpsc::Sender<HashId>,

    pub(crate) main_sm: StorageManager,
    pub(crate) main_am: PieceActivityManager,
    pub(crate) aux_sm: StorageManager,
    pub(crate) aux_am: PieceActivityManager,
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

    pub(crate) async fn start_tick(
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
            if !self.main_sm.checked_bits().await.is_empty() {
                peer.send_message_now(BTMessage::BitField(
                    self.main_sm.checked_bits().await.to_bytes(),
                ))
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
        self.dht.announce(&self.info_hash, *self.listen_addr).await?;
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

        let mut all_checked = false;

        let complete_piece = {
            self.main_am
                .on_piece_data(data.index, data.begin, &data.fragment, &peer.peer_id)
                .await
        };

        if let Some(piece) = complete_piece {
            self.main_sm.write(piece).await?;
            let checked = self.main_sm.verify_checksum(index).await?;
            all_checked = self.main_sm.all_checked().await;
            if checked {
                peer.send_message_now(BTMessage::Have(index as u32)).await?;
            }
            if all_checked {
                self.main_sm.flush().await?;
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

        if self.aux_sm.total_size().await == 0 {
            if let Some(total_len) = peer
                .handshake
                .ext_handshake
                .as_ref()
                .and_then(|ext| ext.get_metadata_size())
            {
                debug!(?total_len, "create torrent metadata piece manager");
                self.aux_sm
                    .reinit_from_file(
                        compute_torrent_path(&self.data_dir, &self.info_hash),
                        total_len,
                        METADATA_PIECE_SIZE,
                    )
                    .await?;
            }
        }

        let snapshot = self.aux_sm.snapshot().await;

        if snapshot.all_checked() {
            return Ok(());
        }

        let piece_reqs = {
            self.aux_am.sync(&snapshot).await?;
            self.aux_am.make_piece_request(&peer.peer_id).await
        };

        let msg_id = peer
            .handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
            .unwrap();

        for req in piece_reqs {
            debug!(?req);
            let req = UTMetadataMessage::Request(req.index);
            peer.send_message((msg_id, req)).await?;
        }
        peer.flush().await?;

        Ok(())
    }

    #[instrument(skip_all, fields(peer_id=?peer_id))]
    async fn handle_peer(&self, peer_id: HashId) -> Result<()> {
        if self.support_ut_metadata(peer_id) && self.is_missing_torrent_file().await {
            self.handle_req_metadata(peer_id).await?;
            return Ok(());
        }

        let peer = self.peers.iter().find(|p| p.peer_id == peer_id).unwrap();

        let state = { peer.state.read().await.clone() };

        let pending_piece_info = { self.main_am.make_piece_request(&peer.peer_id).await };

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

    async fn is_missing_torrent_file(&self) -> bool {
        let size = self.aux_sm.total_size().await;
        size == 0
    }

    #[instrument(skip_all,fields(info_hash=?self.info_hash))]
    async fn tick_check_peer(self) {
        debug!("tick check peer");
        let t = async {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                {
                    let snapshot = self.main_sm.snapshot().await;
                    self.main_am.sync(&snapshot).await?;
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
                            self.main_am
                                .sync_peer_pieces(&peer.peer_id, &state.owned_pieces)
                                .await?;
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
                        let index = info.index;
                        if self.main_sm.is_checked(index).await {
                            let piece = self.main_sm.fetch(index).await?;
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
            let checked_num = (0..self.main_sm.piece_num().await)
                .filter(|&i| self.main_sm.blocking_is_checked(i))
                .count();
            debug!(piece_num=?self.main_sm.piece_num().await, ?checked_num)
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
            self.main_sm.flush().await?;
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
        self.persist().await?;
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
            if let Err(err) = self.main_sm.flush().await {
                error!(?err)
            }
        }
        Ok(())
    }

    pub fn piece_manager(&self) -> StorageManager {
        self.main_sm.clone()
    }

    pub fn tracker(&self) -> TrackerClient {
        self.tracker.clone()
    }

    pub fn peers(&self) -> Arc<dashmap::DashMap<HashId, Peer>> {
        Arc::clone(&self.peers)
    }

    pub async fn check_files(&self) -> Result<bool> {
        Ok(true)
    }
}
