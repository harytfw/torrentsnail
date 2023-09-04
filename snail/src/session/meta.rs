use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const META_FILE: &'static str = "meta.json";

#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentSessionMeta {
    pub(crate) info_hash: String,
    pub(crate) name: String,
}

impl TorrentSessionMeta {
    pub fn from_json_file(file: &Path) -> Result<TorrentSessionMeta> {
        let meta_json = std::fs::read_to_string(file)?;
        let meta = serde_json::from_str(&meta_json)?;
        Ok(meta)
    }

    pub fn write_as_json_file(&self, file: &Path) -> Result<()> {
        let meta_json = serde_json::to_string(self)?;
        std::fs::write(file, meta_json)?;
        Ok(())
    }
}
