use crate::host_port::HostAddr;
use crate::torrent::HashId;
use crate::{Error, Result};
use axum::extract::Host;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use url::Url;

const BROADCAST_ADDR: &str = "239.192.152.143:6771";

#[derive(Clone)]
pub struct LocalServiceDiscovery {
    sock: Arc<UdpSocket>,
}

impl LocalServiceDiscovery {
    pub fn new(sock: Arc<UdpSocket>) -> Self {
        Self { sock }
    }

    pub async fn announce(&self, info_hash: &HashId, host_addr: &HostAddr) -> Result<()> {
        let buf = self.build_packet(info_hash, host_addr);
        self.sock.send_to(&buf, BROADCAST_ADDR).await?;
        Ok(())
    }

    fn build_packet(&self, info_hash: &HashId, host_addr: &HostAddr) -> Vec<u8> {
        let mut buf = String::with_capacity(64);
        buf.push_str("BT-SEARCH * HTTP/1.1\r\n");
        buf.push_str(&format!("Host: {}\r\n", host_addr.host()));
        buf.push_str(&format!("Port: {}\r\n", host_addr.port()));
        buf.push_str(&format!("Infohash: {}\r\n", info_hash.hex()));
        buf.push_str("\r\n\r\n");
        buf.into_bytes()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_lsd() {
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sock = Arc::new(sock);
        let host_addr = HostAddr::new("127.0.0.1", 1080).unwrap();
        let lsd = LocalServiceDiscovery::new(sock);
        lsd.announce(&HashId::ZERO_V1, &host_addr).await.unwrap();
    }
}
