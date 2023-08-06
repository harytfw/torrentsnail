use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{error::Result, Error};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tracing::debug;

#[derive(Debug, Clone)]
pub enum Socks5Addr {
    IPv4(Ipv4Addr),
    Domain(String),
    IPv6(Ipv6Addr),
}

impl From<SocketAddr> for Socks5Addr {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4) => Self::IPv4(*v4.ip()),
            SocketAddr::V6(v6) => Self::IPv6(*v6.ip()),
        }
    }
}

impl Socks5Addr {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Domain(domain) => {
                let mut buf = Vec::with_capacity(1 + domain.len());
                buf.extend(&[0x03]);
                buf.extend(&[domain.len() as u8]);
                buf.extend(domain.as_bytes());
                buf
            }
            Self::IPv4(addr) => {
                let mut buf = Vec::with_capacity(1 + 4);
                buf.extend(&[0x01]);
                buf.extend(&addr.octets());
                buf
            }
            Self::IPv6(addr) => {
                let mut buf = Vec::with_capacity(1 + 16);
                buf.extend(&[0x04]);
                buf.extend(&addr.octets());
                buf
            }
        }
    }

    pub async fn from_async_reader(mut reader: impl tokio::io::AsyncRead + Unpin) -> Result<Self> {
        let t = reader.read_u8().await?;
        match t {
            0x01 => {
                let mut buf = [0; 4];
                reader.read_exact(&mut buf).await?;
                Ok(Self::IPv4(Ipv4Addr::from(buf)))
            }
            0x03 => {
                let len = reader.read_u8().await?;
                let mut buf = vec![0; len as usize];
                reader.read_exact(&mut buf).await?;
                Ok(Self::Domain(String::from_utf8_lossy(&buf).to_string()))
            }
            0x04 => {
                let mut buf = [0; 16];
                reader.read_exact(&mut buf).await?;
                Ok(Self::IPv6(Ipv6Addr::from(buf)))
            }
            _ => Err(Error::Socks5(format!("unknown type: {}", t))),
        }
    }
}

pub struct Socks5Client {
    pub addr: SocketAddr,
}

impl Socks5Client {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<tokio::net::TcpStream> {
        let mut stream = tokio::net::TcpStream::connect(self.addr).await?;
        self.handshake(&mut stream).await?;

        {
            let mut connect_command = vec![0x05, 0x01, 0x00];
            connect_command.extend(Socks5Addr::from(addr).to_bytes());
            connect_command.extend(&addr.port().to_be_bytes());
            // connect
            stream.write_all(&connect_command).await?;
        }

        {
            let mut first_3_bytes = [0; 3];
            stream.read_exact(&mut first_3_bytes).await?;
            let [ver, status, _rsv] = first_3_bytes;
            if status != 0x00 {
                return Err(Error::Socks5(format!("unable connect: {}", status)));
            }
            let addr = Socks5Addr::from_async_reader(&mut stream).await?;
            let port = stream.read_u16().await?;

            debug!(?addr, ?port, ?ver, ?status, "socks5 connect done");
        }

        Ok(stream)
    }

    async fn handshake(&self, stream: &mut TcpStream) -> Result<()> {
        stream.write_all(&[0x05, 0x01, 0x00]).await?;

        let ver = stream.read_u8().await?;
        let cauth = stream.read_u8().await?;

        println!("server ver: {:?}, cauth: {:?}", ver, cauth);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{ToSocketAddrs};

    #[tokio::test]
    #[allow(unreachable_code)]
    async fn test_simple_http() -> Result<()> {
        return Ok(());

        let mut stream = TcpStream::connect("example.com:80").await?;
        stream.write_all(
            "GET / HTTP/1.0\nHost: example.com\nAccept-Language: en-US\nConnection: close\n\n"
            .as_bytes(),
        ).await?;
        let mut buf = vec![];
        stream.read_to_end(&mut buf).await?;
        println!("{}", String::from_utf8(buf).unwrap());
        Ok(())
    }

    #[tokio::test]
    #[allow(unreachable_code)]
    async fn test_connect() -> Result<()> {
        return Ok(());
        
        let mut server_addr = "127.0.0.1:7890".to_socket_addrs().unwrap();
        let client = Socks5Client::new(server_addr.next().unwrap());
        let mut to = "example.com:80".to_socket_addrs().unwrap();
        let mut stream = client.connect(to.next().unwrap()).await?;
        stream
            .write_all(
                "GET / HTTP/1.0\nHost: example.com\nAccept-Language: en-US\nConnection: close\n\n".as_bytes(),
            )
            .await?;
        let mut buf = vec![];
        stream.read_to_end(&mut buf).await?;
        println!("{}", String::from_utf8_lossy(&buf));
        Ok(())
    }
}
