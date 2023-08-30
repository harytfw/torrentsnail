use crate::config::Config;
use crate::dht::DHT;
use crate::lsd::LSD;
use crate::magnet::MagnetURI;
use crate::message::{BTHandshake, MSG_UT_METADATA};
use crate::session::storage::StorageManager;
use crate::session::TorrentSessionStatus;
use crate::torrent::TorrentFile;
use crate::tracker::TrackerClient;
use crate::{torrent, Error, Result, SNAIL_VERSION};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use tokio::fs::symlink;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use torrent::HashId;
use tracing::info;

use crate::session::manager::AtomicPieceActivityManager;

use crate::session::TorrentSession;

const METADATA_PIECE_SIZE: usize = 16384;

fn compute_torrent_path(data_dir: &Path, info_hash: &HashId) -> PathBuf {
    data_dir.join(format!("{}.torrent", info_hash.hex()))
}

#[derive(Default)]
pub struct TorrentSessionBuilder {
    info_hash: Option<HashId>,
    torrent_path: Option<PathBuf>,
    torrent: Option<TorrentFile>,
    check_files: bool,
    dht: Option<DHT>,
    lsd: Option<LSD>,
    my_id: Option<HashId>,
    listen_addr: Option<SocketAddr>,
    config: Option<Arc<Config>>,
}

impl TorrentSessionBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_uri(self, uri: &str) -> Result<Self> {
        if uri.starts_with("magnet:") {
            let magnet = MagnetURI::from_uri(uri)?;
            let xt_hash = magnet
                .xt_hash()
                .ok_or_else(|| Error::Generic("no info hash".to_string()))?;
            let info_hash = HashId::from_hex(xt_hash)?;
            let mut b = self.with_info_hash(info_hash);
            if let Some(dn) = magnet.dn.as_ref() {
                b = b.with_display_name(dn);
            }
            Ok(b)
        } else if uri.starts_with("file:") {
            Err(Error::Generic("not support file protocol".to_string()))
        } else {
            Err(Error::Generic(format!("not support uri: {uri}")))
        }
    }

    pub fn with_display_name(self, _name: &str) -> Self {
        // TODO:
        self
    }

    pub fn with_info_hash(self, info_hash: HashId) -> Self {
        Self {
            info_hash: Some(info_hash),
            ..self
        }
    }

    pub fn info_hash(&self) -> Option<HashId> {
        self.info_hash
    }

    pub fn with_torrent_path(self, path: impl AsRef<Path>) -> Self {
        Self {
            torrent_path: Some(path.as_ref().to_path_buf()),
            ..self
        }
    }

    pub fn with_torrent(self, torrent: TorrentFile) -> Self {
        Self {
            torrent: Some(torrent),
            ..self
        }
    }

    pub fn with_check_files(self, check: bool) -> Self {
        Self {
            check_files: check,
            ..self
        }
    }

    pub fn with_dht(self, dht: DHT) -> Self {
        Self {
            dht: Some(dht),
            ..self
        }
    }

    pub fn with_lsd(self, lsd: LSD) -> Self {
        Self {
            lsd: Some(lsd),
            ..self
        }
    }

    pub fn with_my_id(self, my_id: HashId) -> Self {
        Self {
            my_id: Some(my_id),
            ..self
        }
    }

    pub fn with_listen_addr(self, listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr: Some(listen_addr),
            ..self
        }
    }

    pub fn with_config(self, config: Arc<Config>) -> Self {
        Self {
            config: Some(config),
            ..self
        }
    }

    fn build_handshake(my_id: &HashId, info_hash: &HashId) -> BTHandshake {
        let mut handshake = BTHandshake::new(my_id, info_hash);

        handshake.set_ext_handshake(true);
        {
            let ext = handshake.ext_handshake.as_mut().unwrap();
            ext.set_msg(2, MSG_UT_METADATA).set_version(SNAIL_VERSION);
        }
        handshake
    }

    pub async fn build(mut self) -> Result<TorrentSession> {
        let my_id = self
            .my_id
            .ok_or_else(|| Error::Generic("no my_id".to_string()))?;

        let mut main_storage_manager = StorageManager::empty();
        let mut aux_storage_manager = StorageManager::empty();

        let mut torrent = self.torrent.take();
        let mut info_hash = HashId::ZERO_V1;

        if let Some(hash) = self.info_hash {
            info_hash = hash;
        }

        let data_dir: PathBuf =
            PathBuf::from_str(self.config.as_ref().unwrap().data_dir.as_str()).unwrap();

        let torrent_path = if let Some(path) = self.torrent_path {
            let path_link = compute_torrent_path(&data_dir, &info_hash);
            symlink(&path, &path_link).await?;
            path_link
        } else {
            compute_torrent_path(&data_dir, &info_hash)
        };

        if torrent_path.try_exists()? {
            let torrent_file = TorrentFile::from_path(&torrent_path)?;
            torrent = Some(torrent_file);
        }

        if let Some(torrent) = torrent.as_ref() {
            info_hash = torrent.info_hash().unwrap();
            main_storage_manager =
                StorageManager::from_torrent_data_directory(&data_dir, &torrent.info).await?;
            let metadata_buf = bencode::to_bytes(torrent.get_origin_info().unwrap())?;
            aux_storage_manager = StorageManager::from_single_file(
                &torrent_path,
                metadata_buf.len(),
                METADATA_PIECE_SIZE,
            )
            .await?;
            aux_storage_manager.assume_checked().await;
        }

        let handshake_template = Self::build_handshake(&my_id, &info_hash);

        let (peer_conn_req_tx, peer_addr_rx) = mpsc::channel(10);
        let (peer_piece_req_tx, peer_piece_req_rx) = mpsc::channel(50);
        let (peer_to_poll_tx, peer_to_poll_rx) = mpsc::channel(100);

        if info_hash.is_zero() {
            return Err(Error::Generic("no info hash".into()));
        }

        let tracker = TrackerClient::new();

        let ts = TorrentSession {
            name: torrent
                .as_ref()
                .map(|t| t.info.name.to_string())
                .unwrap_or_else(|| info_hash.hex()),
            tracker,
            peers: Arc::new(dashmap::DashMap::new()),
            info_hash: Arc::new(info_hash),
            aux_sm: aux_storage_manager,
            handshake_template: Arc::new(handshake_template),
            peer_conn_req_tx,
            peer_piece_req_tx,
            main_sm: main_storage_manager,
            main_am: AtomicPieceActivityManager::new(),
            aux_am: AtomicPieceActivityManager::new(),
            long_term_tasks: Default::default(),
            short_term_tasks: Default::default(),
            cancel: CancellationToken::new(),
            status: Arc::new(AtomicU32::new(TorrentSessionStatus::Started as u32)),
            data_dir: Arc::new(data_dir),
            lsd: self.lsd.unwrap(),
            dht: self.dht.unwrap(),
            my_id: self.my_id.unwrap(),
            listen_addr: self.listen_addr.unwrap(),
            cfg: self.config.unwrap(),
            peer_to_poll_tx,
        };
        {
            let ts_clone = ts.clone();
            tokio::spawn(async move {
                ts_clone
                    .start_tick(peer_addr_rx, peer_piece_req_rx, peer_to_poll_rx)
                    .await;
            });
        }
        info!(?info_hash, "new torrent session");
        Ok(ts)
    }

	pub async fn from_data_dir(data_dir: &Path) -> Self {
		todo!()
	}
}
