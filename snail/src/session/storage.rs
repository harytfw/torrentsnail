use crate::session::file::{FileFragment, FilePieceMap, FilePieceMapBuilder};
use crate::session::piece::Piece;
use crate::session::utils::calc_piece_num;
use crate::torrent::TorrentInfo;
use crate::{Error, Result};
use bit_vec::BitVec;
use std::collections::{BTreeMap, HashSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error};

#[derive(Debug, Default)]
struct Inner {
    name: String,
    cache: BTreeMap<usize, Piece>,
    first_piece_len: usize,
    piece_num: usize,
    last_piece_len: usize,
    maps: Vec<Arc<FilePieceMap>>,
    cache_size: usize,
    checked_bits: BitVec,
    checked_bits_path: PathBuf,
    total_size: usize,
}

impl Inner {
    pub async fn from_torrent_info(data_dir: impl AsRef<Path>, info: &TorrentInfo) -> Result<Self> {
        let mut builder = FilePieceMapBuilder::new();

        builder
            .set_total_len(info.total_length())
            .set_piece_len(info.piece_length);

        let join_path = |components: &Vec<String>| {
            components
                .iter()
                .fold(data_dir.as_ref().to_path_buf(), |acc, p| acc.join(p))
        };

        for (len, path) in info
            .get_files_meta()
            .iter()
            .map(|meta| (meta.length, join_path(&meta.path)))
        {
            builder.push_file(len, path);
        }

        let maps = builder.build().unwrap();
        let maps = maps.into_iter().map(Arc::new).collect();

        let (piece_num, last_piece_len) = calc_piece_num(info.total_length(), info.piece_length);

        let mut cache = Self {
            name: info.name.clone(),
            cache: Default::default(),
            first_piece_len: info.piece_length,
            piece_num,
            last_piece_len,
            maps,
            cache_size: 64 << 20, // 64 MB
            checked_bits_path: data_dir.as_ref().join(".snail_checked_bits"),
            total_size: info.total_length(),
            checked_bits: BitVec::from_elem(piece_num, false),
        };
        cache.load_bits().await?;
        Ok(cache)
    }

    pub async fn from_single_file(
        path: impl AsRef<Path>,
        total_size: usize,
        piece_len: usize,
    ) -> Result<Self> {
        let mut builder = FilePieceMapBuilder::new();
        builder
            .push_file(total_size, path.as_ref().to_path_buf())
            .set_piece_len(piece_len)
            .set_total_len(total_size);

        let (piece_num, last_piece_len) = calc_piece_num(total_size, piece_len);

        let maps = builder.build().unwrap();
        let maps = maps.into_iter().map(Arc::new).collect();

        let mut cache = Self {
            name: path
                .as_ref()
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap(),

            cache: Default::default(),
            first_piece_len: piece_len,
            piece_num,
            last_piece_len,
            cache_size: total_size,
            maps,
            checked_bits: BitVec::from_elem(piece_num, false),
            checked_bits_path: path.as_ref().join(".snail_checked_bits"),
            total_size,
        };
        cache.load_bits().await?;
        Ok(cache)
    }

    pub fn empty() -> Self {
        Default::default()
    }

    pub async fn fetch(&mut self, index: usize) -> Result<Piece> {
        if self.cache.contains_key(&index) {
            return Ok(self.cache.get_mut(&index).cloned().unwrap());
        }

        if index >= self.piece_num {
            return Err(Error::PieceNotFound(index));
        }

        let piece_len = self.piece_len(index).await;
        let mut pieces = [Piece::new(index, piece_len)];
        let mut handles = vec![];

        // TODO: avoid unnecessary loop
        for m in self.maps.iter() {
            if m.contains_index(index) {
                let m_arc = Arc::clone(m);
                let handle: JoinHandle<Result<(FileFragment, Vec<u8>)>> =
                    tokio::task::spawn_blocking(move || {
                        let r = m_arc.read(index)?;
                        Ok(r)
                    });
                handles.push(handle);
            }
        }

        for handle in handles {
            let ret = handle.await.unwrap()?;
            let (mut frag, buf) = ret;
            // TODO: avoid unnecessary loop
            for piece in pieces.iter_mut() {
                if piece.index() == frag.piece_index() {
                    frag.apply(&buf, piece);
                }
            }
        }

        let [piece] = pieces;

        // self.expire();
        self.cache.insert(index, piece);
        Ok(self.cache.get_mut(&index).unwrap().clone())
    }

    pub async fn write(&mut self, piece: Piece) -> Result<()> {
        self.cache.insert(piece.index(), piece);
        if self.cache.len() > 100 {
            self.flush().await?;
        }
        Ok(())
    }

    pub async fn read(&mut self, index: usize) -> Result<Piece> {
        self.fetch(index).await
    }

    pub async fn clear_all_checked_bits(&mut self) -> Result<()> {
        self.checked_bits.clear();
        Ok(())
    }

    pub fn all_checked(&self) -> bool {
        self.checked_bits.all()
    }

    pub fn is_checked(&self, index: usize) -> bool {
        self.checked_bits[index]
    }

    pub fn assume_checked(&mut self) {
        for i in 0..self.piece_num {
            self.checked_bits.set(i, true);
        }
    }

    pub async fn fix_file_size(&mut self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(m) = self.maps.iter().find(|m| m.path() == path.as_ref()) {
            let metadata = m.path().metadata()?;
            if metadata.len() as usize > m.size() {
                let f = fs::OpenOptions::new().write(true).open(path.as_ref())?;
                f.set_len(m.size() as u64)?;
            }
        }
        Ok(())
    }

    pub async fn check(&mut self, index: usize, checksum: &[u8]) -> Result<bool> {
        let piece = self.fetch(index).await?;
        let b = match checksum.len() {
            20 => piece.sha1() == checksum,
            32 => piece.sha256() == checksum,
            size => return Err(Error::BadChecksumSize(size)),
        };
        self.checked_bits.set(index, b);
        Ok(b)
    }

    pub async fn check_file<'a>(
        &mut self,
        path: impl AsRef<Path>,
        checksum_fn: impl Fn(usize) -> &'a [u8],
    ) -> Result<bool> {
        let indices = self
            .maps
            .iter()
            .find(|m| m.path() == path.as_ref())
            .map(|m| m.piece_indices().clone())
            .unwrap_or_else(HashSet::new);
        let mut b = true;
        for index in indices {
            self.cache.remove(&index);
            b &= self.check(index, checksum_fn(index)).await?;
        }
        Ok(b)
    }

    pub async fn flush(&mut self) -> Result<()> {
        debug!(?self.name, "piece manager flush");

        let mut handles = vec![];

        // TODO: avoid unnecessary loop
        for (_, piece) in self.cache.iter() {
            for m in self.maps.iter() {
                if m.contains_index(piece.index()) {
                    let write_reqs = m.prepare_write(piece);
                    for req in write_reqs {
                        let m_arc = Arc::clone(m);
                        let handle = tokio::task::spawn_blocking(move || m_arc.write(req));
                        handles.push(handle);
                    }
                }
            }
        }

        for handle in handles {
            handle.await.unwrap()?;
        }

        self.save_bits().await?;
        self.cache.clear();
        Ok(())
    }

    pub async fn piece_len(&self, index: usize) -> usize {
        if index >= self.piece_num {
            panic!("index out of bound");
        }
        if index == self.piece_num - 1 {
            self.last_piece_len
        } else {
            self.first_piece_len
        }
    }

    pub async fn piece_num(&self) -> usize {
        self.piece_num
    }

    pub async fn total_size(&self) -> usize {
        self.total_size
    }

    pub async fn cache_size(&self) -> usize {
        self.cache_size
    }

    pub async fn update_cache_size(&mut self, size: usize) -> usize {
        let old = self.cache_size;
        self.cache_size = size;
        old
    }

    pub async fn save_bits(&mut self) -> Result<()> {
        debug!(name=?self.name, "save bits");
        fs::write(&self.checked_bits_path, self.checked_bits.to_bytes())?;
        Ok(())
    }

    pub async fn load_bits(&mut self) -> Result<()> {
        debug!(name=?self.name, "load bits");

        if let Some(bits) = self
            .read_bit_vec(&self.checked_bits_path, self.checked_bits.len())
            .await
        {
            self.checked_bits = bits;
        }

        debug!(bits = ?self.checked_bits);
        Ok(())
    }

    pub async fn read_bit_vec(&self, path: &Path, bit_len: usize) -> Option<BitVec> {
        let data = match fs::read(path) {
            Ok(data) => Ok(data),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(vec![]),
            Err(e) => Err(Error::Io(e)),
        };
        match data {
            Ok(data) => {
                let mut tmp = BitVec::from_bytes(&data);
                tmp.truncate(bit_len);

                if tmp.len() == bit_len {
                    Some(tmp)
                } else {
                    None
                }
            }
            Err(err) => {
                error!(?path, ?err, "read bit vec");
                None
            }
        }
    }

    pub async fn checked_bits(&self) -> &BitVec {
        &self.checked_bits
    }

    pub async fn paths(&self) -> Vec<PathBuf> {
        self.maps.iter().map(|m| m.path().to_path_buf()).collect()
    }

    pub async fn maps(&self) -> Vec<Arc<FilePieceMap>> {
        self.maps.clone()
    }
}

#[derive(Debug, Clone)]
pub struct StorageManager {
    inner: Arc<RwLock<Inner>>,
}

impl StorageManager {
    pub async fn from_torrent_info(data_dir: impl AsRef<Path>, info: &TorrentInfo) -> Result<Self> {
        let inner = Inner::from_torrent_info(data_dir, info).await?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn reinit_from_torrent(
        &self,
        data_dir: impl AsRef<Path>,
        info: &TorrentInfo,
    ) -> Result<()> {
        let inner = Inner::from_torrent_info(data_dir, info).await?;
        let mut self_inner = self.inner.write().await;
        *self_inner = inner;
        Ok(())
    }

    pub async fn from_single_file(
        path: impl AsRef<Path>,
        total_size: usize,
        piece_len: usize,
    ) -> Result<Self> {
        let inner = Inner::from_single_file(path, total_size, piece_len).await?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn reinit_from_file(
        &self,
        path: impl AsRef<Path>,
        total_size: usize,
        piece_len: usize,
    ) -> Result<()> {
        let new_inner = Inner::from_single_file(path, total_size, piece_len).await?;
        let mut inner = self.inner.write().await;
        *inner = new_inner;
        Ok(())
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::empty())),
        }
    }

    pub async fn fetch(&self, index: usize) -> Result<Piece> {
        let mut inner = self.inner.write().await;
        inner.fetch(index).await
    }

    pub async fn write(&self, piece: Piece) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.write(piece).await
    }

    pub async fn read(&self, index: usize) -> Result<Piece> {
        let mut inner = self.inner.write().await;
        inner.read(index).await
    }

    pub async fn clear_all_checked_bits(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.clear_all_checked_bits().await
    }

    pub async fn all_checked(&self) -> bool {
        let inner = self.inner.write().await;
        inner.all_checked()
    }

    pub async fn is_checked(&self, index: usize) -> bool {
        let inner = self.inner.read().await;
        inner.is_checked(index)
    }

    pub fn blocking_is_checked(&self, index: usize) -> bool {
        let inner = self.inner.blocking_read();
        inner.is_checked(index)
    }

    pub async fn assume_checked(&self) {
        let mut inner = self.inner.write().await;
        inner.assume_checked()
    }

    pub async fn fix_file_size(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.fix_file_size(path).await
    }

    pub async fn check(&self, index: usize, checksum: &[u8]) -> Result<bool> {
        let mut inner = self.inner.write().await;
        inner.check(index, checksum).await
    }

    pub async fn check_file<'a>(
        &mut self,
        path: impl AsRef<Path>,
        checksum_fn: impl Fn(usize) -> &'a [u8],
    ) -> Result<bool> {
        let mut inner = self.inner.write().await;
        inner.check_file(path, checksum_fn).await
    }

    pub async fn flush(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.flush().await
    }

    pub async fn piece_len(&self, index: usize) -> usize {
        let inner = self.inner.write().await;
        inner.piece_len(index).await
    }

    pub async fn piece_num(&self) -> usize {
        let inner = self.inner.read().await;
        inner.piece_num().await
    }

    pub async fn total_size(&self) -> usize {
        let inner = self.inner.write().await;
        inner.total_size().await
    }

    pub async fn cache_size(&self) -> usize {
        let inner = self.inner.write().await;
        inner.cache_size().await
    }

    pub async fn update_cache_size(&self, size: usize) -> usize {
        let mut inner = self.inner.write().await;
        inner.update_cache_size(size).await
    }

    pub async fn save_bits(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.save_bits().await
    }

    pub async fn load_bits(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.load_bits().await
    }

    async fn read_bit_vec(&self, path: &Path, bit_len: usize) -> Option<BitVec> {
        let inner = self.inner.write().await;
        inner.read_bit_vec(path, bit_len).await
    }

    pub async fn checked_bits(&self) -> BitVec {
        let inner = self.inner.write().await;
        inner.checked_bits().await.clone()
    }

    pub async fn paths(&self) -> Vec<PathBuf> {
        let inner = self.inner.write().await;
        inner.paths().await
    }

    pub async fn maps(&self) -> Vec<Arc<FilePieceMap>> {
        let inner = self.inner.read().await;
        inner.maps().await
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        io::{Seek, Write},
        os::unix::prelude::FileExt,
    };

    use super::*;
    use crate::torrent::TorrentFile;
    use glob::glob;

    pub fn setup_test_torrent() -> PathBuf {
        let test_files_dst = PathBuf::from("/tmp/snail/ancient-poetry");
        {
            fs::create_dir_all(&test_files_dst).unwrap();
            for p in glob("tests/ancient-poetry/*").unwrap() {
                let p = p.unwrap();
                fs::copy(&p, test_files_dst.join(p.file_name().unwrap())).unwrap();
            }
        }
        PathBuf::from("/tmp/snail/ancient-poetry/ancient-poetry.torrent")
    }

    pub async fn setup_storage_manager() -> Result<(TorrentFile, StorageManager)> {
        let torrent_path = setup_test_torrent();

        let torrent = TorrentFile::from_path(&torrent_path)?;

        let pm = StorageManager::from_torrent_info(torrent_path.parent().unwrap(), &torrent.info)
            .await?;
        Ok((torrent, pm))
    }

    #[tokio::test]
    async fn empty_piece_manager() -> Result<()> {
        let mut pm = StorageManager::empty();

        assert!(pm.all_checked().await);
        assert!(pm.check(0, &[0]).await.is_err());
        assert!(pm.fetch(0).await.is_err());
        assert_eq!(pm.piece_num().await, 0);

        Ok(())
    }

    #[tokio::test]
    async fn piece_manager_single_file() -> Result<()> {
        let mut pm = StorageManager::from_single_file("/tmp/snail.torrent", 51413, 16384).await?;
        for i in 0..pm.piece_num().await {
            let p = pm.fetch(i).await?;
            assert!(p.buf().iter().all(|byte| *byte == 0));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_setup_storage_manager() -> Result<()> {
        setup_storage_manager().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_manager() -> Result<()> {
        let torrent_path = setup_test_torrent();

        let torrent = TorrentFile::from_path(&torrent_path)?;

        let sha1_fn = |index: usize| torrent.info.get_piece_sha1(index);

        let mut pm =
            StorageManager::from_torrent_info(torrent_path.parent().unwrap(), &torrent.info)
                .await?;

        // read and write same piece
        for i in 0..pm.piece_num().await {
            let p = pm.read(i).await?;
            pm.write(p).await?;
        }

        for i in 0..pm.piece_num().await {
            assert!(pm.check(i, sha1_fn(i)).await?);
        }

        for i in 0..pm.piece_num().await {
            assert!(pm.is_checked(i).await);
        }

        {
            let origin_piece = pm.read(0).await?;
            let mut piece = origin_piece.clone();
            piece.buf_mut()[0..100].copy_from_slice(&[1; 100]);
            pm.write(piece).await?;
            assert!(!pm.check(0, sha1_fn(0)).await?);
            pm.write(origin_piece).await?;
            assert!(pm.check(0, sha1_fn(0)).await?);
        }

        {
            let paths = pm
                .paths()
                .await
                .iter()
                .map(|p| p.to_path_buf())
                .collect::<Vec<PathBuf>>();
            let mut first_byte_vec = vec![];
            for p in paths.iter() {
                let file = fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .open(p)
                    .unwrap();
                let mut byte: u8 = 0;
                file.read_at(std::slice::from_mut(&mut byte), 0).unwrap();
                first_byte_vec.push(byte);
                file.write_at(b"0", 0).unwrap();
            }

            for p in paths.iter() {
                assert!(!pm.check_file(p, sha1_fn).await.unwrap());
            }

            for (p, byte) in paths.iter().zip(first_byte_vec.iter()) {
                let file = fs::OpenOptions::new()
                    .write(true)
                    .read(true)
                    .open(p)
                    .unwrap();
                file.write_at(std::slice::from_ref(byte), 0).unwrap();
            }

            for p in paths.iter() {
                assert!(pm.check_file(p, sha1_fn).await.unwrap());
            }
        }

        {
            let origin_size = pm.maps().await[0].size() as u64;
            let path = pm.maps().await[0].path().to_path_buf();
            let mut file = fs::OpenOptions::new()
                .write(true)
                .append(true)
                .open(&path)
                .unwrap();
            assert_eq!(file.metadata().unwrap().len(), origin_size);
            file.seek(io::SeekFrom::End(0)).unwrap();
            file.write_all(b"12345").unwrap();
            file.flush().unwrap();
            assert_eq!(file.metadata().unwrap().len(), origin_size + 5);
            pm.fix_file_size(path).await.unwrap();
            assert_eq!(file.metadata().unwrap().len(), origin_size);
        }

        Ok(())
    }
}
