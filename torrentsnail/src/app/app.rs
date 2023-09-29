use anyhow::{Error, Result};
use snail::config::Config;
use snail::message::BTHandshake;
use snail::session::{TorrentSession, TorrentSessionBuilder};
use snail::{dht::DHT, lsd::LSD, torrent::HashId};

use crate::app::handler::start_server;
use dashmap::DashMap;

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
    pub ui_url_prefix: String,
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

        let my_id = Arc::new(snail::utils::load_id(&config.id_path())?);
        debug!(?my_id, "setup my_id");

        let udp = Arc::new(UdpSocket::bind(listen_addr.as_ref()).await?);
        debug!(?listen_addr, "listen udp socket");

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
            ui_url_prefix: "/ui".to_string(),
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

        debug!(info_hash=?handshake.info_hash, "torrent session not found, shutdown tcp connection");

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
        .instrument(debug_span!("shutdown application"))
        .await
    }

    pub fn sessions(self: &Arc<Self>) -> Arc<DashMap<HashId, TorrentSession>> {
        self.sessions.clone()
    }

    pub fn get_session_by_info_hash(
        self: &Arc<Self>,
        info_hash: &HashId,
    ) -> Option<TorrentSession> {
        debug!(?info_hash, len = ?self.sessions.len(), "get session by info hash");
        self.sessions.get(info_hash).map(|s| s.clone())
    }

    pub fn acquire_builder(self: &Arc<Self>) -> TorrentSessionBuilder {
        snail::session::TorrentSessionBuilder::new()
            .with_lsd(self.lsd.clone())
            .with_dht(self.dht.clone())
            .with_my_id(*self.my_id)
            .with_listen_addr(*self.listen_addr)
            .with_config(self.config.clone())
    }

    pub async fn consume_builder(self: &Arc<Self>, builder: TorrentSessionBuilder) -> Result<()> {
        let session = builder.build().await?;
        self.sessions.insert(*session.info_hash.clone(), session);
        Ok(())
    }

    pub async fn wait_until_shutdown(self: &Arc<Self>) {
        self.cancel.cancelled().await;
    }
}
