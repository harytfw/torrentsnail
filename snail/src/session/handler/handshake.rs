use crate::message::{BTHandshake, BTMessage};
use crate::session::peer::Peer;
use crate::session::TorrentSession;
use crate::{Error, Result};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tracing::debug;
use tracing::instrument;

impl TorrentSession {
    #[instrument(skip_all, fields(info_hash=?self.info_hash))]
    pub async fn active_handshake_with_addr(&self, addr: impl ToSocketAddrs) -> Result<Peer> {
        let tcp = TcpStream::connect(addr).await?;
        self.active_handshake_with_tcp(tcp).await
    }

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
        let peer = Peer::attach(peer_handshake.clone(), tcp, Default::default()).await?;
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
        Ok(peer)
    }
}
