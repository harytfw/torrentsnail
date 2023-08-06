use crate::Result;
use std::fs;
use std::{
    net::SocketAddr,
    path::{PathBuf},
};

#[derive(Debug, Clone)]
pub struct Config {
    pub network: NetworkConfig,
    pub data_dir: String,
}

impl Config {
    pub fn load_json_file(path: String) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let value = content.parse::<serde_json::Value>().unwrap();

        let mut config = Self::default();
        config.merge_json(&value)?;
        Ok(config)
    }

    fn merge_json(&mut self, val: &serde_json::Value) -> Result<()> {
        if let serde_json::Value::Object(table) = val {
            if let Some(val) = table.get("network") {
                self.network.merge_json(val)?;
            }
            if let Some(val) = table.get("data_dir") {
                self.data_dir = val.as_str().unwrap().to_string();
            }
        }
        Ok(())
    }

    pub fn id_path(&self) -> PathBuf {
        [&self.data_dir, "id.bin"].iter().collect()
    }

    pub fn routing_table_path(&self) -> PathBuf {
        [&self.data_dir, "routing_table.bin"].iter().collect()
    }
}

impl Default for Config {
    fn default() -> Self {
        let home_dir = home::home_dir().unwrap();
        Self {
            network: NetworkConfig::default(),
            data_dir: format!("{}/.local/torrentsnail", home_dir.to_str().unwrap()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub proxy: Option<ProxyConfig>,
    pub bind_interface: String,
    pub torrent_port: u16,
    pub http_port: u16,
    pub tcp_backlog: u32,
}

impl NetworkConfig {
    fn merge_json(&mut self, val: &serde_json::Value) -> Result<()> {
        if let serde_json::Value::Object(table) = val {
            if let Some(val) = table.get("proxy") {
                let mut proxy = ProxyConfig::default();
                proxy.merge_json(val)?;
                self.proxy = Some(proxy);
            }
            if let Some(val) = table.get("bind_interface") {
                self.bind_interface = val.as_str().unwrap().to_string();
            }
            if let Some(val) = table.get("torrent_port") {
                self.torrent_port = val.as_u64().unwrap() as u16;
            }
            if let Some(val) = table.get("http_port") {
                self.http_port = val.as_u64().unwrap() as u16;
            }
        }
        Ok(())
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            proxy: None,
            bind_interface: "0.0.0.0".to_string(),
            torrent_port: 9010,
            http_port: 9011,
            tcp_backlog: 20,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub r#type: String,
    pub addr: String,
    pub port: u16,
    pub dns_query: bool,
    pub auth: Option<ProxyAuthConfig>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            r#type: "socks5".to_string(),
            addr: "127.0.0.1".to_string(),
            port: 1080,
            dns_query: false,
            auth: None,
        }
    }
}

impl ProxyConfig {
    pub fn merge_json(&mut self, val: &serde_json::Value) -> Result<()> {
        if let serde_json::Value::Object(table) = val {
            if let Some(val) = table.get("type") {
                self.r#type = val.as_str().unwrap().to_string();
            }
            if let Some(val) = table.get("addr") {
                self.addr = val.as_str().unwrap().to_string();
            }
            if let Some(val) = table.get("port") {
                self.port = val.as_u64().unwrap() as u16;
            }
            if let Some(val) = table.get("dns_query") {
                self.dns_query = val.as_bool().unwrap();
            }
            if let Some(val) = table.get("auth") {
                let mut auth = ProxyAuthConfig {
                    username: "".to_string(),
                    password: "".to_string(),
                };
                auth.merge_json(val)?;
                self.auth = Some(auth);
            }
        }
        Ok(())
    }

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

impl ProxyAuthConfig {
    pub fn merge_json(&mut self, val: &serde_json::Value) -> Result<()> {
        if let serde_json::Value::Object(obj) = val {
            if let Some(val) = obj.get("username") {
                self.username = val.as_str().unwrap().to_string();
            }
            if let Some(val) = obj.get("password") {
                self.password = val.as_str().unwrap().to_string();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        Config::default();
    }

    #[test]
    fn test_merge_json() {
        let mut config = Config::default();

        let json_val = r#"
        {
            "data_dir": "/tmp/torrentsnail",
            "network": {
                "bind_interface": "192.168.1.1"
            }
        }
		"#
        .parse::<serde_json::Value>()
        .unwrap();

        config.merge_json(&json_val).unwrap();

        assert_eq!(config.data_dir, "/tmp/torrentsnail");
        assert_eq!(config.network.bind_interface, "192.168.1.1");
    }
}
