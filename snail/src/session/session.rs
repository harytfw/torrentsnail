use crate::config::Config;
use crate::dht::DHT;
use crate::lsd::LSD;
use crate::message::BTHandshake;
use crate::session::manager::PieceActivityManager;
use crate::session::peer::Peer;
use crate::session::storage::StorageManager;
use crate::session::TorrentSessionStatus;
use crate::tracker::types::AnnounceResponse;
use crate::tracker::TrackerClient;
use crate::{torrent, tracker, Result};
use num::FromPrimitive;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::ToSocketAddrs;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use torrent::HashId;
use tracing::{debug, error, instrument};

use super::event::SessionEvent;

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

    pub(crate) status: Arc<AtomicU32>,
    pub(crate) cancel: CancellationToken,
    pub(crate) listen_addr: Arc<SocketAddr>,
    pub(crate) tracker: TrackerClient,
    pub(crate) dht: DHT,
    pub(crate) lsd: LSD,
    pub(crate) cfg: Arc<Config>,

    pub(crate) main_sm: StorageManager,
    pub(crate) main_am: PieceActivityManager,
    pub(crate) aux_sm: StorageManager,
    pub(crate) aux_am: PieceActivityManager,
    pub(crate) event_tx: mpsc::Sender<SessionEvent>,
}

impl Debug for TorrentSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentSession")
            .field("info_hash", &self.info_hash)
            .finish()
    }
}

impl TorrentSession {
    pub(crate) async fn start_tick(&self, action_rx: mpsc::Receiver<SessionEvent>) {
        let mut long_term = self.long_term_tasks.write().await;
        long_term.spawn(self.clone().tick_announce());
    }

    #[instrument(skip_all)]
    async fn announce_tracker_event(&self, event: tracker::Event) -> Result<()> {
        self.event_tx
            .send(SessionEvent::AnnounceTrackerRequest(event))
            .await?;
        return Ok(());
    }

    async fn do_announce(&mut self) -> Result<()> {
        debug!("announce lsd");
        self.lsd.announce(&self.info_hash).await?;
        self.dht
            .announce(&self.info_hash, *self.listen_addr)
            .await?;
        self.search_peer_and_connect().await?;
        Ok(())
    }

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

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    async fn chain_announce_response_to_action(
        self,
        mut announce_response_rx: mpsc::Receiver<AnnounceResponse>,
    ) {
        let cancel = self.cancel.clone();

        let t = async {
            loop {
                match announce_response_rx.recv().await {
                    Some(rsp) => {
                        self.event_tx.send(SessionEvent::from(rsp)).await.unwrap();
                    }
                    None => {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        };

        tokio::select! {
            r = t => {
                let r: Result<()> = Ok(r);
                match r {
                    Ok(()) => {}
                    Err(e) => error!(err = ?e, "tick announce")
                }
            },
            _ = cancel.cancelled() => {}
        }
    }

    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    async fn poll_peer(self, mut rx: mpsc::Receiver<HashId>) {
        let t = async {
            loop {
                match rx.try_recv() {
                    Ok(peer_id) => {}
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
