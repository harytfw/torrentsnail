use crate::{Error, Result};
use rand::random;
use std::{fs, net::SocketAddr, path, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpSocket, TcpStream, UdpSocket},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, error, span, Instrument, Level};
pub mod file;
pub mod peer;
pub mod session;
pub mod types;
use self::types::BTHandshake;
use crate::{
    dht::DHT,
    lsd::LSD,
    torrent::{HashId, TorrentFile},
    tracker::TrackerClient,
};
use session::TorrentSession;
pub mod piece;

#[derive(Clone)]
pub struct BT {
    pub my_id: Arc<HashId>,
    pub listen_addr: Arc<SocketAddr>,
    dht: DHT,
    lsd: LSD,
    tracker: TrackerClient,
    sessions: Arc<RwLock<Vec<TorrentSession>>>,
    cancel: CancellationToken,
}

impl BT {
    pub async fn start() -> Result<Arc<Self>> {
        let listen_addr: Arc<SocketAddr> = Arc::new("0.0.0.0:8081".parse()?);

        let my_id = Arc::new(Self::load_id()?);
        let udp = Arc::new(UdpSocket::bind(listen_addr.as_ref()).await?);

        let dht = { DHT::new(udp.clone(), &my_id) };
        dht.load_routing_table().await?;

        let tracker = { TrackerClient::new(udp.clone()) };

        let lsd = { LSD::new(Arc::clone(&udp), "192.168.2.56", 8081) };

        let bt = Arc::new(Self {
            my_id,
            dht,
            tracker,
            lsd,
            sessions: Arc::new(RwLock::new(vec![])),
            cancel: CancellationToken::new(),
            listen_addr: Arc::clone(&listen_addr),
        });
        {
            let tcp = TcpSocket::new_v4()?;
            tcp.bind(*listen_addr)?;
            let listener = tcp.listen(20)?;
            tokio::spawn(Arc::clone(&bt).accept_tcp(listener));
        }
        {
            tokio::spawn(Arc::clone(&bt).recv_udp(Arc::clone(&udp)));
        }

        Ok(bt)
    }

    pub async fn download_with_torrent(
        self: &Arc<Self>,
        torrent: TorrentFile,
    ) -> Result<TorrentSession> {
        let info_hash = torrent.info_hash().unwrap();
        let mut sessions = self.sessions.write().await;
        for s in sessions.iter() {
            if s.info_hash.is_same(&info_hash) {
                return Ok(s.clone());
            }
        }
        let s = TorrentSession::from_torrent(Arc::downgrade(self), torrent)?;
        sessions.push(s.clone());
        Ok(s)
    }

    pub async fn download_with_info_hash(
        self: &Arc<Self>,
        info_hash: &HashId,
    ) -> Result<TorrentSession> {
        let mut sessions = self.sessions.write().await;
        for s in sessions.iter() {
            if s.info_hash.is_same(info_hash) {
                return Ok(s.clone());
            }
        }
        let s = TorrentSession::from_info_hash(Arc::downgrade(self), *info_hash)?;
        sessions.push(s.clone());
        Ok(s)
    }

    async fn accept_tcp(self: Arc<Self>, listener: TcpListener) {
        let t = async {
            loop {
                let (tcp, _) = listener.accept().await?;
                let c = self.clone();
                tokio::spawn(async move {
                    let tcp = tcp;
                    c.perform_handshake(tcp).await;
                });
            }
        };

        tokio::select! {
            result = t => {
                let result: Result<()> = result;
                match result {
                    Ok(()) => {},
                    Err(e) => {
                        error!(err = ?e, "accept tcp")
                    }
                }
            },
            _ = self.cancel.cancelled() => { }
        }
    }

    async fn recv_udp(self: Arc<Self>, udp: Arc<UdpSocket>) {
        let t = async {
            let mut buf = vec![0; 1024];
            loop {
                let (len, addr) = udp.recv_from(&mut buf).await?;
                let recv_buf = buf[..len].to_vec();
                if recv_buf.len() > 2 && &recv_buf[..1] == b"d" {
                    debug!(src = ?addr, "dispatch packet to dht");
                    self.dht.on_packet((recv_buf, addr)).await?;
                } else {
                    debug!(src = ?addr, "dispatch packet to tracker");
                    self.tracker.on_udp_packet((recv_buf, addr)).await?;
                }
            }
        };
        tokio::select! {
            result = t => {
                let result: Result<(),Error> = result;
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        error!(err = ?e, "recv udp");
                    }
                }
            },
            _ = self.cancel.cancelled() => { }
        }
    }

    async fn perform_handshake(self: Arc<Self>, mut tcp: TcpStream) {
        let addr = match tcp.peer_addr() {
            Ok(addr) => addr,
            Err(err) => {
                error!(?err, "no peer addr");
                return;
            }
        };

        let handshake = match BTHandshake::from_reader_async(&mut tcp).await {
            Ok(s) => s,
            Err(err) => {
                error!(?err, "parse handshake");
                return;
            }
        };

        let span =
            span!(Level::DEBUG, "check handshake", info_hash =?handshake.info_hash, addr = ?addr);

        async {
            let torrent = self
                .sessions
                .read()
                .await
                .iter()
                .find(|t| t.info_hash.as_ref() == &handshake.info_hash)
                .cloned();

            if let Some(torrent) = torrent {
                debug!("passive handshake");
                if let Err(e) = torrent.passive_handshake(handshake, tcp).await {
                    error!(err = ?e);
                };
                return;
            }

            debug!("torrent session not found");
            if let Err(e) = tcp.shutdown().await {
                error!(err = ?e);
            }
        }
        .instrument(span)
        .await;
    }

    fn load_id() -> Result<HashId> {
        let id_path = path::Path::new("/tmp/dht.id");
        if id_path.exists() {
            let id = fs::read(id_path)?;
            assert_eq!(id.len(), 20);
            return HashId::from_slice(&id);
        }
        let id: [u8; 20] = random();
        debug!(id = ?id, "generate new id");
        fs::write(id_path, id)?;
        Ok(id.into())
    }

    pub async fn shutdown(self: Arc<Self>) -> Result<()> {
        self.cancel.cancel();
        async {
            self.dht.shutdown().await?;
            self.tracker.shutdown().await?;
            for session in self.sessions.write().await.drain(..) {
                session.shutdown().await?;
            }
            Ok(())
        }
        .instrument(debug_span!("shutdown"))
        .await
    }

    pub async fn add_tracker(self: Arc<Self>, url: &str) -> Result<()> {
        self.tracker.add_tracker(url).await?;
        Ok(())
    }
}