use std::sync::{Arc, RwLock};

pub struct Config {
	proxy: Option<ProxyConfig>,
}

pub enum ProxyType {
	Socks5,
	Http,
}

pub struct ProxyConfig {
	typ: ProxyType,
	addr: String,
	port: u16,
	dns_query: bool,	
	auth: Option<ProxyAuthConfig>,
}

pub struct ProxyAuthConfig {
	username: (),
	password: (),
}

pub type MutableConfig = Arc<RwLock<Config>>;