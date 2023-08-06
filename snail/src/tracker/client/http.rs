use crate::tracker::client::SessionState;
use crate::tracker::types::*;
use crate::{Error, Result};
use reqwest::StatusCode;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

#[derive(Clone)]
pub struct Session {
    http_client: reqwest::Client,
    url: Arc<String>,
    state: Arc<RwLock<SessionState>>,
}

impl Session {
    pub fn new(http_client: reqwest::Client, url: &str) -> Self {
        Self {
            http_client,
            url: Arc::new(url.to_string()),
            state: Arc::new(RwLock::from(SessionState {
                connected: true,
                ..Default::default()
            })),
        }
    }

    pub async fn send_announce(&self, req: &AnnounceRequest) -> Result<AnnounceResponseHttp> {
        let state = { self.state.read().await.clone() };

        if !state.can_announce() {
            return Err(Error::SkipAnnounce);
        }

        let mut http_req = self.http_client.get(self.url.as_str()).build()?;
        http_req.url_mut().set_query(Some(&req.to_query_string()));
        let rsp = self.http_client.execute(http_req).await?;
        match rsp.status() {
            StatusCode::OK => {
                let bytes = rsp.bytes().await?;
                match bencode::from_bytes::<AnnounceResponseHttp, _>(&bytes) {
                    Ok(rsp) => {
                        let mut state = self.state.write().await;
                        state.on_announce_response(std::time::Duration::from_secs(rsp.interval));
                        return Ok(rsp);
                    }
                    Err(err) => {
                        error!(?err)
                    }
                }
                match bencode::from_bytes::<bencode::Value, _>(&bytes) {
                    Ok(val) => {
                        error!(?val);
                        Err(Error::Generic("decode response failed".into()))
                    }
                    Err(err) => Err(Error::Bencode(err)),
                }
            }
            code => Err(Error::Generic(format!("http code: {code}"))),
        }
    }

    pub fn get_tracker_url(&self) -> &str {
        &self.url
    }

    pub async fn get_state(&self) -> SessionState {
        self.state.read().await.clone()
    }
}

#[derive(Default)]
pub struct TrackerHttpClient {
    http_client: reqwest::Client,
    sessions: Arc<RwLock<BTreeMap<String, Session>>>,
}

impl TrackerHttpClient {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(Default::default())),
            http_client: reqwest::Client::new(),
        }
    }

    pub async fn add_tracker(&self, url: &str) -> Result<Session> {
        let mut map = self.sessions.write().await;
        let session = Session::new(self.http_client.clone(), url);
        map.insert(url.to_string(), session.clone());
        Ok(session)
    }

    pub async fn all_sessions(&self) -> Vec<Session> {
        self.sessions.read().await.values().cloned().collect()
    }

    pub async fn get_session(&self, url: &str) -> Option<Session> {
        self.sessions
            .read()
            .await
            .values()
            .find(|s| s.get_tracker_url() == url)
            .cloned()
    }
    pub async fn shutdown(&self) -> Result<()> {
        Ok(())
    }
}

impl Drop for TrackerHttpClient {
    fn drop(&mut self) {
        // todo: stop async task
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_announce() -> Result<()> {
        let http_client = reqwest::Client::new();
        let _session = Session::new(http_client, "http://tr.bangumi.moe:6969/announce");
        // session.send_announce().await.unwrap();
        Ok(())
    }
}
