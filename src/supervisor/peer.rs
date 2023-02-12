use crate::supervisor::{
    types::{BTHandshake, BTMessage},
    Error, Result,
};
use crate::torrent::HashId;
use std::{net::SocketAddr, sync::Arc};
use tokio::{io::AsyncWriteExt, sync::mpsc, sync::RwLock};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

#[derive(Debug, Clone)]
pub struct PeerState {
    pub broken: Option<String>,
    pub choke: bool,
    pub interested: bool,
    pub owned_pieces: bit_vec::BitVec,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            choke: true,
            broken: None,
            interested: false,
            owned_pieces: Default::default(),
        }
    }
}

enum TcpMessage {
    Payload(BTMessage),
    Flush,
}

impl From<BTMessage> for TcpMessage {
    fn from(msg: BTMessage) -> Self {
        Self::Payload(msg)
    }
}

#[derive(Clone)]
pub struct Peer {
    msg_tcp_tx: mpsc::Sender<TcpMessage>,
    msg_tx: mpsc::Sender<BTMessage>,
    cancel: CancellationToken,
    pub state: Arc<RwLock<PeerState>>,
    pub addr: Arc<SocketAddr>,
    pub peer_id: Arc<HashId>,
    pub handshake: Arc<BTHandshake>,
    tasks: Arc<RwLock<JoinSet<()>>>,
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
            tasks: Default::default(),
        };

        {
            let mut task = peer.tasks.write().await;
            task.spawn(peer.clone().read_tcp(tcp_rx));
            task.spawn(peer.clone().write_tcp(tcp_tx, msg_tcp_rx));
        }

        Ok((peer, msg_rx))
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        let mut tasks = self.tasks.write().await;
        while tasks.join_next().await.is_some() {}
        debug!(peer_id=?self.peer_id, addr = ?self.addr, "shutdown peer");
        Ok(())
    }

    pub async fn send_message(&self, msg: impl Into<BTMessage>) -> Result<()> {
        self.msg_tcp_tx.send(TcpMessage::from(msg.into())).await?;
        Ok(())
    }

    pub async fn send_message_now(&self, msg: impl Into<BTMessage>) -> Result<()> {
        self.send_message(msg).await?;
        self.flush().await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.msg_tcp_tx.send(TcpMessage::Flush).await?;
        Ok(())
    }

    async fn write_tcp(self, tx: OwnedWriteHalf, mut msg_tcp_rx: mpsc::Receiver<TcpMessage>) {
        let mut buf_tx = tokio::io::BufWriter::new(tx);
        let t = async {
            loop {
                let time_to_flush = tokio::time::sleep(std::time::Duration::from_secs(15));
                tokio::select! {
                    tcp_msg_recv = msg_tcp_rx.recv() => {
                        if let Some(tcp_msg) = tcp_msg_recv {
                            match tcp_msg {
                                TcpMessage::Payload(msg) => {
                                    buf_tx.write_all(&msg.to_bytes()).await?;
                                }
                                TcpMessage::Flush => {
                                    buf_tx.flush().await?;
                                }
                            }
                        }
                    }
                    _ = time_to_flush => {
                        buf_tx.flush().await?;
                    }
                };
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
