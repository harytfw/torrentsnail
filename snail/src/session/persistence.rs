use crate::error::Result;
use crate::session::meta;
use crate::session::TorrentSession;

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

        let meta_path = self.data_dir.join(meta::META_FILE);
        meta.write_as_json_file(&meta_path)?;
        Ok(())
    }
}
