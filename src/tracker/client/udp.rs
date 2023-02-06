use crate::tracker::types::*;
use crate::tracker::SessionState;
use crate::tracker::SessionStatus;
use crate::{Error, Result};
use std::collections::HashMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

#[derive(Debug, Clone)]
pub struct Session {
    sock: Arc<UdpSocket>,
    addr: Arc<SocketAddr>,
    pub url: Arc<String>,
    transactions: Arc<Mutex<HashMap<u32, oneshot::Sender<Response>>>>,
    state: Arc<RwLock<SessionState>>,
    packet_tx: mpsc::Sender<Vec<u8>>,
    cancel: CancellationToken,
}

impl Session {
    fn new(sock: Arc<UdpSocket>, addr: SocketAddr, url: &str) -> Self {
        let (tx, rx) = mpsc::channel(10);
        let state = SessionState::default();
        let session = Self {
            sock,
            addr: Arc::new(addr),
            url: Arc::new(url.to_string()),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            state: Arc::new(RwLock::new(state)),
            packet_tx: tx,
            cancel: CancellationToken::new(),
        };
        {
            let session_clone = session.clone();
            tokio::spawn(session_clone.notify_transaction(rx));
        }
        session
    }

    async fn dispatch_packet(&self, packet: Vec<u8>) -> Result<()> {
        self.packet_tx.send(packet).await?;
        Ok(())
    }

    async fn notify_transaction(self, mut chan: mpsc::Receiver<Vec<u8>>) {
        let t = async move {
            loop {
                while let Some(ref buf) = chan.recv().await {
                    let (transaction_id, rsp) = if let Ok(rsp) = ConnectResponse::from_bytes(buf) {
                        (rsp.transaction_id, Response::Connect(rsp))
                    } else if let Ok(rsp) = AnnounceResponseV4::from_bytes(buf) {
                        (rsp.transaction_id, Response::AnnounceV4(rsp))
                    } else if let Ok(rsp) = ScrapeResponse::from_bytes(buf) {
                        (rsp.transaction_id, Response::Scrape(rsp))
                    } else if let Ok(rsp) = TrackerError::from_bytes(buf) {
                        (rsp.transaction_id, Response::Error(rsp))
                    } else {
                        debug!("unknown packet");
                        continue;
                    };
                    {
                        let mut transaction = self.transactions.lock().await;
                        if let Some((_, cb)) = transaction.remove_entry(&transaction_id) {
                            cb.send(rsp)
                                .map_err(|_| Error::SendError("tracker transaction".into()))?;
                        }
                    }
                }
            }
        };
        tokio::select! {
            r = t => {
                let r : Result<()> = r;
                match r {
                    Ok(()) => {},
                    Err(err) => {
                        error!(?err, "notify transaction")
                    }
                }
            },
            _ = self.cancel.cancelled() => {

            },
        }
    }

    async fn wait_retransmit(
        &self,
        buf: &[u8],
        mut rx: oneshot::Receiver<Response>,
    ) -> Result<Response> {
        let timeout_sec = (0..=8).map(|n| 15 * 2u64.pow(n));
        for sec in timeout_sec {
            debug!(addr = ?self.addr, "send packet to tracker");
            self.sock.send_to(buf, self.addr.as_ref()).await?;
            let timeout = tokio::time::sleep(std::time::Duration::from_secs(sec));
            tokio::select! {
                _ = timeout => {},
                _ = self.cancel.cancelled() => {
                    return Err(Error::Cancel)
                },
                ret = &mut rx => {
                    return match ret {
                        Ok(rsp) => Ok(rsp),
                        Err(err) => Err(Error::RecvError(err.to_string()))
                    }
                }
            }
        }
        Err(Error::Timeout)
    }

    async fn new_transaction(&self) -> (u32, oneshot::Receiver<Response>) {
        let id: u32 = rand::random();
        let (tx, rx) = oneshot::channel();
        {
            let mut transactions = self.transactions.lock().await;
            transactions.insert(id, tx);
            debug!(id = ?id, "new transaction");
        }
        (id, rx)
    }

    async fn clean_transaction(&self, id: u32) {
        let mut transactions = self.transactions.lock().await;
        transactions.remove(&id);
        debug!(id = ?id, "clean transaction");
    }

    pub async fn send_connect(&self) -> Result<ConnectResponse> {
        let state = { self.state.read().await.clone() };

        if state.status.is_connected() {
            return Err(Error::Generic("already connected".into()));
        }

        let (transaction_id, rx) = self.new_transaction().await;

        let mut req = ConnectRequest::new();
        req.set_transaction_id(transaction_id);
        let buf = req.to_bytes()?;

        let result = self.wait_retransmit(&buf, rx).await;
        match result {
            Ok(rsp) => match rsp {
                Response::Connect(rsp) => {
                    {
                        let mut state = self.state.write().await;
                        if state.status.is_disconnected() {
                            state.conn_id = rsp.connection_id;
                            state.status = SessionStatus::Connected;
                            debug!("connected");
                        }
                    }
                    Ok(rsp)
                }
                Response::Error(err) => Err(Error::from(err)),
                _ => Err(Error::Generic("incorrect response type".into())),
            },
            Err(e) => {
                self.clean_transaction(transaction_id).await;
                Err(e)
            }
        }
    }

    pub async fn send_announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponseV4> {
        let state = { self.state.read().await.clone() };

        if state.status.is_disconnected() {
            return Err(Error::Generic("not connected".into()));
        }

        if let Some(announce_at) = state.last_announce_at {
            if announce_at.elapsed() < state.announce_interval {
                return Err(Error::SkipAnnounce);
            }
        }

        let (transaction_id, rx) = self.new_transaction().await;

        let buf = {
            let mut req = req.clone();
            req.connection_id = state.conn_id;
            req.transaction_id = transaction_id;
            req.to_bytes()?
        };

        let result = self.wait_retransmit(&buf, rx).await;
        match result {
            Ok(rsp) => match rsp {
                Response::AnnounceV4(rsp) => {
                    {
                        let mut state = self.state.write().await;
                        state.last_announce_at = Some(Instant::now());
                        state.announce_interval =
                            std::time::Duration::from_secs(rsp.interval as u64);
                    }
                    Ok(rsp)
                }
                Response::Error(err) => Err(Error::from(err)),
                _ => Err(Error::Generic("incorrect response type".into())),
            },
            Err(e) => {
                self.clean_transaction(transaction_id).await;
                Err(e)
            }
        }
    }

    pub fn send_scrape(&mut self) -> Result<()> {
        todo!()
    }

    pub fn get_tracker_url(&self) -> &str {
        &self.url
    }

    pub async fn get_state(&self) -> SessionState {
        self.state.read().await.clone()
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }
}

#[derive(Clone)]
pub struct TrackerUdpClient {
    sock: Arc<UdpSocket>,
    sessions: Arc<RwLock<Vec<Session>>>,
    cancel_token: tokio_util::sync::CancellationToken,
}

impl TrackerUdpClient {
    pub fn new(sock: Arc<UdpSocket>) -> Self {
        Self {
            sock,
            sessions: Arc::new(RwLock::new(vec![])),
            cancel_token: Default::default(),
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel_token.cancel();
        Ok(())
    }

    pub async fn on_packet(&self, packet: (Vec<u8>, SocketAddr)) -> Result<()> {
        let (buf, addr) = packet;
        let sessions = self.sessions.read().await;
        if let Some(session) = sessions.iter().find(|s| s.addr.as_ref() == &addr) {
            session.dispatch_packet(buf).await?;
        } else {
            debug!(?addr, "tracker session not found")
        }
        Ok(())
    }

    pub async fn add_tracker(&self, tracker_url: &str) -> Result<Session> {
        use url::Url;

        let url = Url::parse(tracker_url)?;
        assert_eq!(url.scheme(), "udp");
        let host = url
            .host_str()
            .ok_or_else(|| Error::Generic("no host".into()))?;

        let port = url
            .port_or_known_default()
            .ok_or_else(|| Error::Generic("no port".into()))?;

        let host_port = format!("{host}:{port}");

        let addr_vec: Vec<SocketAddr> = tokio::net::lookup_host(host_port).await?.collect();

        match addr_vec.first() {
            Some(addr) => {
                debug!(tracker = ?tracker_url, ?addr, "new tracker session");
                let session = Session::new(self.sock.clone(), *addr, tracker_url);
                {
                    let mut sessions = self.sessions.write().await;
                    sessions.push(session.clone());
                }
                debug!("create tracker session");
                Ok(session)
            }
            None => Err(Error::NoAddress),
        }
    }

    pub async fn all_sessions(&self) -> Vec<Session> {
        let sessions = self.sessions.read().await;
        sessions.iter().cloned().collect()
    }

    pub async fn get_session(&self, url: &str) -> Option<Session> {
        let sessions = self.sessions.read().await;
        sessions
            .iter()
            .find(|s| s.get_tracker_url() == url)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client() -> Result<()> {
        let sock = Arc::new(UdpSocket::bind("0.0.0.0:8081").await?);
        let client = TrackerUdpClient::new(sock);
        client.shutdown().await?;
        Ok(())
    }
}
