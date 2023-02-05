use crate::bt::{
    types::{BTHandshake, BTMessage, PieceInfo},
    Error, Result,
};
use crate::torrent::HashId;
use std::{
    collections::BTreeSet,
    net::{SocketAddr},
    sync::Arc,
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio::{io::AsyncWriteExt, sync::mpsc, sync::RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error};

#[derive(Debug, Clone)]
pub struct PeerState {
    pub broken: Option<String>,
    pub choke: bool,
    pub interested: bool,
    pub owned_piece_indices: BTreeSet<usize>,
    pub available_fragment_req: usize,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            choke: true,
            broken: None,
            interested: false,
            owned_piece_indices: Default::default(),
            available_fragment_req: 30,
        }
    }
}

#[derive(Clone)]
pub struct Peer {
    msg_tcp_tx: mpsc::Sender<BTMessage>,
    msg_tx: mpsc::Sender<BTMessage>,
    cancel: CancellationToken,
    pub state: Arc<RwLock<PeerState>>,
    pub addr: Arc<SocketAddr>,
    pub peer_id: Arc<HashId>,
    pub handshake: Arc<BTHandshake>,
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer")
            .field("id", &self.peer_id)
            .field("addr", &self.addr)
            .finish()
    }
}

impl Peer {
    pub async fn attach(
        handshake: BTHandshake,
        tcp: TcpStream,
    ) -> Result<(Self, mpsc::Receiver<BTMessage>)> {
        let addr = tcp.peer_addr()?;
        let (tcp_rx, tcp_tx) = tcp.into_split();

        let (msg_tcp_tx, msg_tcp_rx) = mpsc::channel(10);
        let (msg_tx, msg_rx) = mpsc::channel(10);

        let peer = Self {
            state: Arc::new(RwLock::new(PeerState::default())),
            msg_tcp_tx,
            msg_tx,
            cancel: CancellationToken::new(),
            addr: Arc::new(addr),
            peer_id: handshake.peer_id.into(),
            handshake: Arc::new(handshake),
        };

        {
            let peer_clone = peer.clone();
            tokio::spawn(peer_clone.read_tcp(tcp_rx));
        }
        {
            let peer_clone = peer.clone();
            tokio::spawn(peer_clone.write_tcp(tcp_tx, msg_tcp_rx));
        }

        Ok((peer, msg_rx))
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }

    pub async fn fetch_torrent_file(&self, _info_hash: &HashId) {
        todo!()
    }

    pub async fn send_message(&self, msg: impl Into<BTMessage>) -> Result<()> {
        self.msg_tcp_tx.send(msg.into()).await?;
        Ok(())
    }

    pub async fn send_ping(&self) -> Result<()> {
        self.send_message(BTMessage::Ping).await
    }

    pub async fn send_have(&self, n: usize) -> Result<()> {
        self.send_message(BTMessage::Have(n as u32)).await
    }

    pub async fn send_choke(&self) -> Result<()> {
        self.send_message(BTMessage::Choke).await
    }

    pub async fn send_unchoke(&self) -> Result<()> {
        self.send_message(BTMessage::Unchoke).await
    }

    pub async fn send_interested(&self) -> Result<()> {
        self.send_message(BTMessage::Interested).await
    }

    pub async fn send_not_interested(&self) -> Result<()> {
        self.send_message(BTMessage::NotInterested).await
    }

    pub async fn send_request(&self, info: PieceInfo) -> Result<()> {
        self.send_message(BTMessage::Request(info)).await
    }

    pub async fn send_piece(&self) -> Result<()> {
        todo!()
    }

    async fn write_tcp(self, tx: OwnedWriteHalf, mut msg_tcp_rx: mpsc::Receiver<BTMessage>) {
        let mut buf_tx = tokio::io::BufWriter::new(tx);
        let t = async {
            loop {
                while let Some(msg) = msg_tcp_rx.recv().await {
                    buf_tx.write_all(&msg.to_bytes()).await?;
                    buf_tx.flush().await?;
                }
            }
        };
        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                match r {
                    Ok(())=>{},
                    Err(e) => {
                        error!(err=  ?e, "write tcp");
                    }
                }
            },
            _ = self.cancel.cancelled() => {}
        }
    }

    async fn set_broken(&self, e: Error) {
        let mut state = self.state.write().await;
        state.broken = Some(e.to_string());
    }

    async fn read_tcp(self, rx: OwnedReadHalf) {
        static MAX_BUFFER_SIZE: usize = 64 << 20;

        let mut buf_rx = tokio::io::BufReader::new(rx);
        let t = async {
            loop {
                let result = BTMessage::from_reader_async(&mut buf_rx).await;
                match result {
                    Ok(msg) => {
                        self.msg_tx
                            .send(msg)
                            .await
                            .map_err(|e| Error::SendError(e.to_string()))?;
                    }
                    Err(e) => {
                        error!(peer=?self, "read bt message: {}", e);
                        return Err(e);
                    }
                }
            }
        };
        tokio::select! {
            r = t => {
                let r: Result<()> = r;
                match r {
                    Ok(()) => {},
                    Err(err) => {
                        self.set_broken(err).await;
                    },
                }
            },
            _ = self.cancel.cancelled() => {}
        }
    }
}
