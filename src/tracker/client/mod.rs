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

#[derive(Debug, Clone, Default)]
pub struct SessionState {
    pub connected: bool,
    pub conn_id: u64,
    pub announce_interval: Duration,
    pub last_announce_at: Option<Instant>,
}

impl SessionState {
    pub fn can_announce(&self) -> bool {
        if !self.connected {
            return false;
        }

        self.last_announce_at
            .map(|at| at.elapsed() < self.announce_interval)
            .unwrap_or(true)
    }

    pub fn on_announce_response(&mut self, interval: Duration) {
        self.last_announce_at = Some(Instant::now());
        self.announce_interval = interval
    }
}

pub enum Session {
    Http(http::Session),
    Udp(udp::Session),
}

impl Session {
    pub fn tracker_url(&self) -> &str {
        match self {
            Self::Http(s) => s.get_tracker_url(),
            Self::Udp(s) => s.tracker_url(),
        }
    }

    pub async fn state(&self) -> SessionState {
        match self {
            Self::Http(s) => s.get_state().await,
            Self::Udp(s) => s.state().await,
        }
    }

    pub async fn send_announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponse> {
        let rsp = match self {
            Self::Http(s) => AnnounceResponse::Http(s.send_announce(req).await?),
            Self::Udp(s) => AnnounceResponse::V4(s.send_announce(req).await?),
        };
        Ok(rsp)
    }

    pub async fn description(&self) -> Result<String> {
        todo!()
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
    pub fn new() -> Self {
        let http = Arc::new(TrackerHttpClient::new());
        let udp = Arc::new(TrackerUdpClient::new());
        Self { http, udp }
    }

    pub async fn add_tracker(&self, tracker: &str) -> Result<()> {
        if tracker.starts_with("udp:") {
            self.udp.add_tracker(tracker).await?;
        } else {
            self.http.add_tracker(tracker).await?;
        }
        Ok(())
    }

    pub async fn remove_tracker(&self, tracker: &str) -> Result<()> {
        todo!()
    }

    pub async fn sessions(&self) -> Vec<Session> {
        let mut merge: Vec<Session> = vec![];
        merge.extend(self.udp.sessions().await.into_iter().map(Into::into));
        merge.extend(self.http.all_sessions().await.into_iter().map(Into::into));
        merge
    }

    pub async fn send_announce(
        &self,
        req: &AnnounceRequest,
    ) -> mpsc::Receiver<Result<(String, AnnounceResponse)>> {
        let (tx, rx) = mpsc::channel(20);
        let req = Arc::new(req.clone());
        for session in self.sessions().await {
            let req_arc = Arc::clone(&req);
            let tx_clone = tx.clone();
            let url = session.tracker_url().to_string();
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
}
