use crate::torrent::HashId;
use crate::Result;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio_util::sync::CancellationToken;
use tracing::debug;

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

    pub async fn announce(&self, info_hash: &HashId) -> Result<()> {
        debug!(info_hash = ?info_hash, "announce lsd");
        let buf = self.build_packet(info_hash);
        self.sock.send_to(&buf, "239.192.152.143:6771").await?;
        Ok(())
    }

    fn build_packet(&self, info_hash: &HashId) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.extend(b"BT-SEARCH * HTTP/1.1\r\n");
        buf.append(&mut format!("Host: {}\r\n", self.host_port.0).into_bytes());
        buf.append(&mut format!("Port: {}\r\n", self.host_port.1).into_bytes());
        buf.append(&mut format!("Infohash: {}\r\n", info_hash.hex()).into_bytes());
        buf.extend(b"\r\n\r\n");
        buf
    }
}
