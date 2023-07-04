use crate::config::Config;
use crate::message::BTHandshake;
use crate::session::{TorrentSession, TorrentSessionBuilder};
use crate::{dht::DHT, lsd::LSD, torrent::HashId};
use crate::{Error, Result};

use crate::app::handler::start_server;
use dashmap::DashMap;
use rand::random;
use tracing_subscriber::field::debug;
use std::path::Path;
use std::{fs, net::SocketAddr, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpSocket, TcpStream, UdpSocket},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, error, instrument, Instrument};

#[derive(Clone)]
pub struct Application {
    pub my_id: Arc<HashId>,
    pub listen_addr: Arc<SocketAddr>,
    dht: DHT,
    lsd: LSD,
    sessions: Arc<DashMap<HashId, TorrentSession>>,
    pub(crate) cancel: CancellationToken,
    pub(crate) config: Arc<Config>,
}

fn load_id(id_path: &Path) -> Result<HashId> {
    if id_path.exists() {
        let id = fs::read(id_path)?;
        assert_eq!(id.len(), 20);
        return HashId::from_slice(&id);
    }
    let id: [u8; 20] = random();
    debug!(id = ?id, id_path = ?id_path, "generate new id");
    fs::write(id_path, id)?;
    Ok(id.into())
}

impl Application {
    pub async fn from_config(config: Config) -> Result<Arc<Self>> {
        debug!(?config, "dump config");

        let config = Arc::new(config);

        if !Path::new(&config.data_dir).exists() {
            fs::create_dir_all(&config.data_dir)?;
        }

        let listen_addr: Arc<SocketAddr> = Arc::new(
            format!(
                "{}:{}",
                config.network.bind_interface, config.network.torrent_port
            )
            .parse()?,
        );

        debug!("setup my_id");
        let my_id = Arc::new(load_id(&config.id_path())?);

        debug!(?listen_addr, "setup udp socket");
        let udp = Arc::new(UdpSocket::bind(listen_addr.as_ref()).await?);

        debug!("setup dht");
        let dht = DHT::new(udp.clone(), &my_id, &config.routing_table_path());

        dht.load_routing_table().await?;

        debug!("setup lsd");
        let lsd = {
            LSD::new(
                Arc::clone(&udp),
                &config.network.bind_interface,
                config.network.torrent_port,
            )
        };

        let app = Arc::new(Self {
            my_id,
            dht,
            lsd,
            sessions: Arc::new(DashMap::new()),
            cancel: CancellationToken::new(),
            listen_addr: Arc::clone(&listen_addr),
            config: Arc::clone(&config),
        });
        {
            debug!(?listen_addr, "setup tcp listener");
            let tcp = TcpSocket::new_v4()?;
            tcp.bind(*listen_addr)?;
            let listener = tcp.listen(config.network.tcp_backlog)?;
            tokio::spawn(Arc::clone(&app).accept_tcp(listener));
        }
        {
            debug!("setup udp receiver");
            tokio::spawn(Arc::clone(&app).recv_udp(Arc::clone(&udp)));
        }
        {
            debug!("start http server");
            tokio::spawn(start_server(Arc::clone(&app)));
        }
        Ok(app)
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

        let session: Option<TorrentSession> =
            self.sessions.get(&handshake.info_hash).map(|s| s.clone());

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

    pub async fn shutdown(self: &Arc<Self>) -> Result<()> {
        self.cancel.cancel();
        async {
            self.dht.shutdown().await?;
            for session in self.sessions.iter() {
                session.shutdown().await?;
            }
            self.sessions.clear();
            Ok(())
        }
        .instrument(debug_span!("shutdown"))
        .await
    }

    pub fn sessions(self: &Arc<Self>) -> Arc<DashMap<HashId, TorrentSession>> {
        self.sessions.clone()
    }

    pub fn get_session(self: &Arc<Self>, info_hash: &HashId) -> Option<TorrentSession> {
        self.sessions.get(info_hash).map(|s| s.clone())
    }

    pub(crate) async fn attach_session(self: &Arc<Self>, session: TorrentSession) {
        self.sessions.insert(*session.info_hash, session);
    }

    pub fn builder(self: &Arc<Self>) -> TorrentSessionBuilder {
        TorrentSessionBuilder::new()
            .with_app(Arc::downgrade(self))
            .with_lsd(self.lsd.clone())
            .with_dht(self.dht.clone())
            .with_my_id(*self.my_id)
            .with_listen_addr(*self.listen_addr)
            .with_config(self.config.clone())
    }

    pub async fn wait_until_shutdown(self: &Arc<Self>) {
        self.cancel.cancelled().await;
    }
}
