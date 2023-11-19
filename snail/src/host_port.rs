use std::net::IpAddr;

use tower::util::Optional;

use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HostAddr {
    host: String,
    port: u16,
}

impl HostAddr {
    pub fn new(host: &str, port: u16) -> Result<Self> {
        // TODO: validate host
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn ip(&self) -> Option<IpAddr> {
        todo!()
    }
}
