use crate::message::{BTHandshake, BTMessage};
use crate::session::utils::{calc_speed, timestamp_sec};
use crate::torrent::HashId;
use crate::{Error, Result};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::SystemTime};
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
    // the pieces that current peer reported. if peer didn't reported yet, the length is 0,
    // otherwise the length is the number of pieces
    pub owned_pieces: bit_vec::BitVec,

    // store the number of bytes that we upload,
    // key: timestamp in second, value: the number of bytes per second
    pub upload_bytes: BTreeMap<u64, usize>,
    pub download_bytes: BTreeMap<u64, usize>,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            choke: true,
            broken: None,
            interested: false,
            owned_pieces: Default::default(),
            upload_bytes: BTreeMap::new(),
            download_bytes: BTreeMap::new(),
        }
    }
}

impl PeerState {
    fn update_download_bytes(&mut self, t: SystemTime, size: usize) {
        let entry = self.download_bytes.entry(timestamp_sec(t)).or_insert(0);
        *entry += size;
        Self::retain_record(&mut self.download_bytes, 60);
    }

    fn update_upload_bytes(&mut self, t: SystemTime, size: usize) {
        let entry = self.upload_bytes.entry(timestamp_sec(t)).or_insert(0);
        *entry += size;
        Self::retain_record(&mut self.upload_bytes, 60);
    }

    fn retain_record(m: &mut BTreeMap<u64, usize>, retain_size: usize) {
        while m.len() > retain_size {
            m.pop_first();
        }
    }

    pub fn upload_speed(&self) -> usize {
        calc_speed(&self.upload_bytes)
    }

    pub fn download_speed(&self) -> usize {
        calc_speed(&self.download_bytes)
    }
}

enum PeerMessage {
    BT(BTMessage),
    Flush,
}

impl From<BTMessage> for PeerMessage {
    fn from(msg: BTMessage) -> Self {
        Self::BT(msg)
    }
}

#[derive(Clone)]
pub struct Peer {
    msg_tcp_tx: mpsc::Sender<PeerMessage>,
    msg_tx: mpsc::Sender<BTMessage>,
    cancel: CancellationToken,
    pub state: Arc<RwLock<PeerState>>,
    pub addr: SocketAddr,
    pub peer_id: HashId,
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
            addr: addr,
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
        Result::Ok(())
    }

    pub async fn send_message(&self, msg: impl Into<BTMessage>) -> Result<()> {
        self.msg_tcp_tx.send(PeerMessage::from(msg.into())).await?;
        Ok(())
    }

    pub async fn send_message_now(&self, msg: impl Into<BTMessage>) -> Result<()> {
        self.send_message(msg).await?;
        self.flush().await?;
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.msg_tcp_tx.send(PeerMessage::Flush).await?;
        Result::Ok(())
    }

    fn on_send_piece(&self, size: usize) {
        let state = Arc::clone(&self.state);
        let t = SystemTime::now();
        tokio::spawn(async move {
            let mut state = state.write().await;
            state.update_upload_bytes(t, size);
        });
    }

    fn on_received_piece(&self, size: usize) {
        let state = Arc::clone(&self.state);
        let t = SystemTime::now();
        tokio::spawn(async move {
            let mut state = state.write().await;
            state.update_download_bytes(t, size);
        });
    }

    async fn write_tcp(self, tx: OwnedWriteHalf, mut msg_tcp_rx: mpsc::Receiver<PeerMessage>) {
        let mut buf_tx = tokio::io::BufWriter::new(tx);
        let t = async {
            loop {
                let time_to_flush = tokio::time::sleep(std::time::Duration::from_secs(15));
                tokio::select! {
                    tcp_msg_recv = msg_tcp_rx.recv() => {
                        if let Some(tcp_msg) = tcp_msg_recv {
                            match tcp_msg {
                                PeerMessage::BT(msg) => {
                                    // only record the size of piece message?
                                    if let BTMessage::Piece(data) = &msg {
                                        self.on_send_piece(data.fragment.len());
                                    }
                                    buf_tx.write_all(&msg.to_bytes()).await?;
                                }
                                PeerMessage::Flush => {
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
                        if let BTMessage::Piece(data) = &msg {
                            self.on_received_piece(data.fragment.len())
                        }
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

    pub fn get_msg_id(&self, msg_name: impl AsRef<str>) -> Option<u8> {
        self.handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_id(msg_name.as_ref()))
    }
}

#[cfg(test)]
mod tests {}
