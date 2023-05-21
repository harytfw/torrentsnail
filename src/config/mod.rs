use std::{sync::{Arc}, net::SocketAddr};
use tokio::sync::RwLock;
use crate::Result;

#[derive(Debug, Clone, Default)]
pub struct Config {
	pub proxy: Option<ProxyConfig>,
}

#[derive(Debug, Clone)]
pub enum ProxyType {
	Socks5,
	Http,
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
	pub r#type: ProxyType,
	pub addr: String,
	pub port: u16,
	pub dns_query: bool,	
	pub auth: Option<ProxyAuthConfig>,
}

impl ProxyConfig {
	pub fn to_socks5_addr(&self) -> Result<SocketAddr> {
		let sa = format!("{}:{}", self.addr, self.port).parse()?;
		Ok(sa)
	}
}

#[derive(Debug, Clone)]
pub struct ProxyAuthConfig {
	pub username: String,
	pub password: String,
}

pub type MutableConfig = Arc<RwLock<Config>>;