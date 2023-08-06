use crate::torrent::HashId;
use crate::Result;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

const BROADCAST_ADDR: &str = "239.192.152.143:6771";

#[derive(Clone)]
pub struct LSD {
    sock: Arc<UdpSocket>,
    _cancel: CancellationToken,
    host_port: Arc<(String, u16)>,
}

impl LSD {
    pub fn new(sock: Arc<UdpSocket>, host: &str, port: u16) -> Self {
        Self {
            sock,
            _cancel: CancellationToken::new(),
            host_port: Arc::new((host.to_string(), port)),
        }
    }

    #[instrument(skip(self))]
    pub async fn announce(&self, info_hash: &HashId) -> Result<()> {
        let buf = self.build_packet(info_hash);
        self.sock.send_to(&buf, BROADCAST_ADDR).await?;
        Ok(())
    }

    fn build_packet(&self, info_hash: &HashId) -> Vec<u8> {
        let mut buf = String::with_capacity(64);
        buf.push_str("BT-SEARCH * HTTP/1.1\r\n");
        buf.push_str(&format!("Host: {}\r\n", self.host_port.0));
        buf.push_str(&format!("Port: {}\r\n", self.host_port.1));
        buf.push_str(&format!("Infohash: {}\r\n", info_hash.hex()));
        buf.push_str("\r\n\r\n");
        buf.into_bytes()
    }
}
