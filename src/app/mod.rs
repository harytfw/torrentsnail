use crate::message::BTHandshake;
use crate::session::{TorrentSession, TorrentSessionBuilder};
use crate::{dht::DHT, lsd::LSD, torrent::HashId};
use crate::{Error, Result};
use rand::random;
use std::{fs, net::SocketAddr, path, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpSocket, TcpStream, UdpSocket},
    sync::RwLock,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, error, instrument, Instrument};
pub const MAX_FRAGMENT_LENGTH: usize = 16 << 10;

#[derive(Clone)]
pub struct Application {
    pub my_id: Arc<HashId>,
    pub listen_addr: Arc<SocketAddr>,
    dht: DHT,
    lsd: LSD,
    sessions: Arc<RwLock<Vec<TorrentSession>>>,
    cancel: CancellationToken,
}

impl Application {
    pub async fn start() -> Result<Arc<Self>> {
        let listen_addr: Arc<SocketAddr> = Arc::new("0.0.0.0:8081".parse()?);

        let my_id = Arc::new(Self::load_id()?);
        let udp = Arc::new(UdpSocket::bind(listen_addr.as_ref()).await?);

        let dht = { DHT::new(udp.clone(), &my_id) };
        dht.load_routing_table().await?;

        let lsd = { LSD::new(Arc::clone(&udp), "192.168.2.56", 8081) };

        let sup = Arc::new(Self {
            my_id,
            dht,
            lsd,
            sessions: Arc::new(RwLock::new(vec![])),
            cancel: CancellationToken::new(),
            listen_addr: Arc::clone(&listen_addr),
        });
        {
            let tcp = TcpSocket::new_v4()?;
            tcp.bind(*listen_addr)?;
            let listener = tcp.listen(20)?;
            tokio::spawn(Arc::clone(&sup).accept_tcp(listener));
        }
        {
            tokio::spawn(Arc::clone(&sup).recv_udp(Arc::clone(&udp)));
        }

        Ok(sup)
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
                self.dht.on_packet((recv_buf, addr)).await?;
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

    #[instrument(skip_all, fields(local=?tcp.local_addr(),remote=?tcp.peer_addr()))]
    async fn perform_handshake(self: Arc<Self>, mut tcp: TcpStream) {
        let handshake = match BTHandshake::from_reader_async(&mut tcp).await {
            Ok(s) => s,
            Err(err) => {
                error!(?err, "parse handshake");
                return;
            }
        };

        let session = self
            .sessions
            .read()
            .await
            .iter()
            .find(|t| t.info_hash.as_ref() == &handshake.info_hash)
            .cloned();

        if let Some(session) = session {
            if let Err(e) = session.passive_handshake(handshake, tcp).await {
                error!(err = ?e);
            };
            return;
        }

        debug!("torrent session not found");
        if let Err(e) = tcp.shutdown().await {
            error!(err = ?e);
        }
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

    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        self.cancel.cancel();
        async {
            self.dht.shutdown().await?;
            for session in self.sessions.write().await.drain(..) {
                session.shutdown().await?;
            }
            Ok(())
        }
        .instrument(debug_span!("shutdown"))
        .await
    }

    pub async fn sessions(self: &Arc<Self>) -> Vec<TorrentSession> {
        let sessions = self.sessions.read().await.clone();
        sessions
    }

    pub(crate) async fn attach_session(self: &Arc<Self>, session: TorrentSession) {
        let mut s = self.sessions.write().await;
        s.push(session);
    }

    pub fn builder(self: &Arc<Self>) -> TorrentSessionBuilder {
        TorrentSessionBuilder::new().with_supervisor(Arc::downgrade(self))
    }
}
