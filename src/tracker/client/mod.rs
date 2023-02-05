mod http;
mod udp;
use self::types::AnnounceResponse;
pub use super::types;
use super::types::AnnounceRequest;
use crate::Result;
pub use http::TrackerHttpClient;
use std::{net::SocketAddr, time::Instant};
use std::{sync::Arc, time::Duration};
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{debug, error};
pub use udp::TrackerUdpClient;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum SessionStatus {
    #[default]
    Disconnect,
    Connected,
}

impl SessionStatus {
    pub fn is_disconnected(&self) -> bool {
        matches!(self, Self::Disconnect)
    }

    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SessionState {
    status: SessionStatus,
    conn_id: u64,
    announce_interval: Duration,
    last_announce_at: Option<Instant>,
}

pub enum Session {
    Http(http::Session),
    Udp(udp::Session),
}
impl Session {
    pub fn get_tracker_url(&self) -> &str {
        match self {
            Self::Http(s) => s.get_tracker_url(),
            Self::Udp(s) => s.get_tracker_url(),
        }
    }

    pub async fn get_state(&self) -> SessionState {
        match self {
            Self::Http(s) => s.get_state().await,
            Self::Udp(s) => s.get_state().await,
        }
    }

    pub async fn get_state_mut(&self) -> SessionState {
        match self {
            Self::Http(s) => s.get_state().await,
            Self::Udp(s) => s.get_state().await,
        }
    }

    pub async fn send_announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse> {
        let rsp = match self {
            Self::Http(s) => AnnounceResponse::Http(s.send_announce(req).await?),
            Self::Udp(s) => AnnounceResponse::V4(s.send_announce(req).await?),
        };
        Ok(rsp)
    }
}

impl From<http::Session> for Session {
    fn from(s: http::Session) -> Self {
        Self::Http(s)
    }
}

impl From<&http::Session> for Session {
    fn from(s: &http::Session) -> Self {
        Self::Http(s.clone())
    }
}

impl From<udp::Session> for Session {
    fn from(s: udp::Session) -> Self {
        Self::Udp(s)
    }
}

impl From<&udp::Session> for Session {
    fn from(s: &udp::Session) -> Self {
        Self::Udp(s.clone())
    }
}

#[derive(Clone)]
pub struct TrackerClient {
    http: Arc<TrackerHttpClient>,
    udp: Arc<TrackerUdpClient>,
}

impl TrackerClient {
    pub fn new(sock: Arc<UdpSocket>) -> Self {
        let http = Arc::new(TrackerHttpClient::new());
        let udp = Arc::new(TrackerUdpClient::new(sock));
        Self { http, udp }
    }

    pub async fn add_tracker(&self, tracker: &str) -> Result<()> {
        if tracker.starts_with("udp:") {
            let session = self.udp.add_tracker(tracker).await?;
            tokio::spawn(async move {
                loop {
                    debug!(tracker = ?session.url, "try connect to tracker");
                    match session.send_connect().await {
                        Ok(_) => {
                            debug!(tracker = ?session.url, "success connect to tracker");
                            return;
                        }
                        Err(e) => {
                            error!(err = ?e, tracker = ?session.url, "connect tracker failed");
                        }
                    }
                }
            });
        } else {
            self.http.add_tracker(tracker).await?;
        }
        Ok(())
    }

    pub async fn all_sessions(&self) -> Vec<Session> {
        let mut merge: Vec<Session> = vec![];
        merge.extend(self.udp.all_sessions().await.into_iter().map(Into::into));
        merge.extend(self.http.all_sessions().await.into_iter().map(Into::into));
        merge
    }

    pub async fn send_announce(
        &self,
        req: &AnnounceRequest,
    ) -> mpsc::Receiver<Result<(String, AnnounceResponse)>> {
        let (tx, rx) = mpsc::channel(20);
        let req = Arc::new(req.clone());
        for session in self.all_sessions().await {
            let req_arc = Arc::clone(&req);
            let tx_clone = tx.clone();
            let url = session.get_tracker_url().to_string();
            tokio::spawn(async move {
                let r = session.send_announce(&req_arc).await;
                let r = r.map(|rsp| (url, rsp));
                match tx_clone.send(r).await {
                    Ok(()) => {}
                    Err(e) => error!(err=?e),
                }
            });
        }
        rx
    }

    pub async fn get_session(&self, url: &str) -> Option<Session> {
        if url.starts_with("udp:") {
            self.udp.get_session(url).await.map(Into::into)
        } else {
            self.http.get_session(url).await.map(Into::into)
        }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.udp.shutdown().await?;
        self.http.shutdown().await?;
        Ok(())
    }

    pub async fn on_udp_packet(&self, packet: (Vec<u8>, SocketAddr)) -> Result<()> {
        self.udp.on_packet(packet).await?;
        Ok(())
    }
}
