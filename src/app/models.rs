use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Ping {
    pub timestamp_ms: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Pong {
    pub ping_timestamp_ms: u64,
    pub timestamp_ms: u64,
    pub elapsed_ms: u64,
}

impl From<&Ping> for Pong {
    fn from(value: &Ping) -> Self {
        let now = SystemTime::now();
        Self {
            ping_timestamp_ms: value.timestamp_ms,
            timestamp_ms: now
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or(Duration::from_millis(0))
                .as_millis() as u64,
            elapsed_ms: now
                .duration_since(
                    SystemTime::UNIX_EPOCH.add(Duration::from_millis(value.timestamp_ms)),
                )
                .unwrap_or(Duration::from_millis(0))
                .as_millis() as u64,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddTorrentReq {}

#[derive(Serialize, Deserialize, Debug)]
pub struct AddTorrentResp {}

pub struct PauseTorrentReq {}
