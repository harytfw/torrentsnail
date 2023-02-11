use crate::tracker::types::*;
use crate::tracker::SessionState;
use crate::{Error, Result};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
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

    async fn dispatch_packet(&self, buf: &[u8]) -> Result<()> {
        self.packet_tx.send(buf.to_vec()).await?;
        Ok(())
    }

    async fn notify_transaction(self, mut chan: mpsc::Receiver<Vec<u8>>) {
        let t = async move {
            loop {
                while let Some(ref buf) = chan.recv().await {
                    let (transaction_id, rsp) = match Response::from_bytes(buf) {
                        Ok(rsp) => (rsp.transaction_id(), rsp),
                        Err(err) => {
                            error!(?err, "bad packet");
                            continue;
                        }
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

        if state.connected {
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
                        if !state.connected {
                            state.conn_id = rsp.connection_id;
                            state.connected = true;
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

        if !state.can_announce() {
            return Err(Error::SkipAnnounce);
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
                        state.on_announce_response(std::time::Duration::from_secs(
                            rsp.interval as u64,
                        ));
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

    pub fn tracker_url(&self) -> &str {
        &self.url
    }

    pub async fn state(&self) -> SessionState {
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
    cancel: CancellationToken,
}

impl TrackerUdpClient {
    pub fn new() -> Self {
        let std_sock = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
        std_sock.set_nonblocking(true).unwrap();
        let sock = UdpSocket::from_std(std_sock).unwrap();
        let cli = Self {
            sock: Arc::new(sock),
            sessions: Arc::new(RwLock::new(vec![])),
            cancel: Default::default(),
        };
        tokio::spawn(cli.clone().recv_udp());
        cli
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.cancel.cancel();
        Ok(())
    }

    pub async fn on_packet(&self, packet: (&[u8], SocketAddr)) -> Result<()> {
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

        let session = match addr_vec.first() {
            Some(addr) => {
                debug!(tracker = ?tracker_url, ?addr, "new tracker session");
                let session = Session::new(self.sock.clone(), *addr, tracker_url);
                {
                    let mut sessions = self.sessions.write().await;
                    sessions.push(session.clone());
                }
                debug!("create tracker session");
                session
            }
            None => return Err(Error::NoAddress),
        };
        {
            let s = session.clone();
            tokio::spawn(async move {
                loop {
                    debug!(tracker = ?s.url, "try connect to tracker");
                    match s.send_connect().await {
                        Ok(_) => {
                            debug!(tracker = ?s.url, "success connect to tracker");
                            return;
                        }
                        Err(e) => {
                            error!(err = ?e, tracker = ?s.url, "connect tracker failed");
                        }
                    }
                }
            });
        }
        Ok(session)
    }

    pub async fn remove_tracker(&self, url: &str) {
        let mut ss = self.sessions.write().await;
        let index = ss
            .iter()
            .enumerate()
            .find(|(_, s)| s.tracker_url() == url)
            .map(|(i, _)| i);
        if let Some(index) = index {
            ss.remove(index);
        }
    }

    pub async fn sessions(&self) -> Vec<Session> {
        let sessions = self.sessions.read().await;
        sessions.iter().cloned().collect()
    }

    pub async fn get_session(&self, url: &str) -> Option<Session> {
        let sessions = self.sessions.read().await;
        sessions.iter().find(|s| s.tracker_url() == url).cloned()
    }

    async fn recv_udp(self) {
        let t = async {
            let mut buf = vec![0; 1024];
            loop {
                let (len, addr) = self.sock.recv_from(&mut buf).await?;
                self.on_packet((&buf[..len], addr)).await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
}
