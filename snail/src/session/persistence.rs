use crate::error::{Error, Result};
use crate::session::meta;
use crate::session::TorrentSession;
use serde::Serialize;

impl TorrentSession {
    pub async fn persist(&self) -> Result<()> {
        self.persist_meta().await?;
        Ok(())
    }

    async fn persist_meta(&self) -> Result<()> {
        let meta = meta::TorrentSessionMeta {
            info_hash: self.info_hash.to_string(),
            name: self.name.clone(),
        };

        let meta_json = serde_json::to_string(&meta)?;

        let meta_path = self.data_dir.join(meta::META_FILE);
        tokio::fs::write(meta_path, meta_json).await?;
        Ok(())
    }
}
