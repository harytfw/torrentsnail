use crate::bt::types::PieceInfo;
use crate::torrent::{HashId, TorrentInfo};
use crate::{Error, Result};
use bit_vec::BitVec;
use rand::Rng;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{cmp, fs, io};
use tracing::{debug, error};

#[derive(Clone)]
pub struct Piece {
    buf: Vec<u8>,
    index: usize,
}

impl Debug for Piece {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece").field("index", &self.index).finish()
    }
}

impl Piece {
    pub fn new(index: usize, piece_len: usize) -> Self {
        Self {
            buf: vec![0; piece_len],
            index,
        }
    }

    pub fn from_buf(index: usize, buf: Vec<u8>) -> Self {
        Self { index, buf }
    }

    pub fn sha1(&self) -> [u8; 20] {
        let info_hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &self.buf);
        let result: [u8; 20] = info_hash.as_ref().try_into().unwrap();
        result
    }

    pub fn sha256(&self) -> [u8; 32] {
        let info_hash = ring::digest::digest(&ring::digest::SHA256, &self.buf);
        let result: [u8; 32] = info_hash.as_ref().try_into().unwrap();
        result
    }

    pub fn buf(&self) -> &[u8] {
        &self.buf
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn into_buf(self) -> Vec<u8> {
        self.buf
    }
}

fn calc_piece_num(total_len: usize, piece_len: usize) -> (usize, usize) {
    let (mut piece_num, last_piece_len) = (total_len / piece_len, total_len % piece_len);

    if last_piece_len != 0 {
        piece_num += 1
    }
    (piece_num, last_piece_len)
}

pub struct PieceLenIter {
    total_len: usize,
    piece_len: usize,
}

impl PieceLenIter {
    pub fn new(total_len: usize, piece_len: usize) -> Self {
        if piece_len > total_len {
            panic!("piece len > total_len")
        }
        Self {
            total_len,
            piece_len,
        }
    }
}

impl Iterator for PieceLenIter {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        if self.total_len == 0 {
            return None;
        }
        let min = cmp::min(self.total_len, self.piece_len);
        self.total_len -= min;
        Some(min)
    }
}

#[derive(Debug, Clone)]
struct FileFragment {
    piece_index: usize,
    piece_offset: usize,
    file_offset: usize,
    len: usize,
}

impl FileFragment {
    fn new(piece_index: usize, piece_offset: usize, file_offset: usize, len: usize) -> Self {
        Self {
            piece_index,
            piece_offset,
            len,
            file_offset,
        }
    }

    fn cut(self, at: usize) -> (FileFragment, FileFragment) {
        let left = FileFragment::new(self.piece_index, self.piece_offset, self.file_offset, at);

        let right = FileFragment::new(
            self.piece_index,
            self.piece_offset + at,
            self.file_offset + at,
            self.len - at,
        );

        assert_eq!(self.len, left.len + right.len);

        (left, right)
    }
}

struct FilePieceMap {
    fragments: Vec<FileFragment>,
    // the file path
    path: PathBuf,
    // the size of complete file
    size: usize,
    // TODO: use rang to represent used range
    piece_indices: HashSet<usize>,
}

impl Debug for FilePieceMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first_frag = self.fragments.first().unwrap();
        let last_frag = self.fragments.last().unwrap();
        f.debug_struct("FileMap")
            .field("path", &self.path)
            .field(
                "first_frag",
                &(first_frag.piece_index, first_frag.piece_offset),
            )
            .field(
                "last_frag",
                &(
                    last_frag.piece_index,
                    last_frag.piece_offset + last_frag.len,
                ),
            )
            .finish()
    }
}

impl FilePieceMap {
    fn new(path: impl AsRef<Path>, fragments: Vec<FileFragment>) -> Self {
        assert!(!fragments.is_empty());
        let size = fragments.iter().map(|f| f.len).sum();
        let piece_indices = HashSet::from_iter(fragments.iter().map(|f| f.piece_index));
        Self {
            path: path.as_ref().to_owned(),
            fragments,
            size,
            piece_indices,
        }
    }

    fn write(&self, pieces: &[Piece]) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }

        let f = fs::File::options()
            .create(true)
            .write(true)
            .open(self.path.as_path())?;

        let mut write_cnt = 0usize;

        for piece in pieces {
            if let Some(frag) = self.fragments.iter().find(|f| f.piece_index == piece.index) {
                let data = &piece.buf[frag.piece_offset..frag.piece_offset + frag.len];
                debug!(?frag, "accept piece");
                f.write_at(data, frag.file_offset as u64)?;
                write_cnt += 1;
            }
        }

        debug!(path = ?self.path, "save data");
        if write_cnt != pieces.len() {
            Err(Error::Generic("some file fragments can not accept".into()))
        } else {
            Ok(())
        }
    }

    fn read(&self, pieces: &mut [Piece]) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }

        if !self.path.as_path().try_exists()? {
            return Ok(());
        }

        let f = fs::File::options().read(true).open(&self.path)?;

        let mut read_cnt = 0usize;

        for piece in pieces.iter_mut() {
            if let Some(frag) = self.fragments.iter().find(|f| f.piece_index == piece.index) {
                let buf = &mut piece.buf[frag.piece_offset..frag.piece_offset + frag.len];
                debug!(?frag, "fill piece");
                f.read_exact_at(buf, frag.file_offset as u64)?;
                read_cnt += 1
            }
        }

        if read_cnt != pieces.len() {
            Err(Error::Generic("some file fragments not found".into()))
        } else {
            Ok(())
        }
    }

    fn contains_index(&self, index: usize) -> bool {
        self.piece_indices.contains(&index)
    }
}

#[derive(Debug, Default)]
struct FilePieceMapBuilder {
    files: Vec<(usize, PathBuf)>,
    total_len: usize,
    piece_len: usize,
}

impl FilePieceMapBuilder {
    fn new() -> Self {
        Default::default()
    }

    fn push_file(&mut self, len: usize, path: PathBuf) -> &mut Self {
        self.files.push((len, path));
        self
    }

    fn set_total_len(&mut self, total_len: usize) -> &mut Self {
        self.total_len = total_len;
        self
    }

    fn set_piece_len(&mut self, piece_len: usize) -> &mut Self {
        self.piece_len = piece_len;
        self
    }

    fn build(self) -> Result<Vec<FilePieceMap>> {
        let mut piece_frag_que: VecDeque<FileFragment> =
            PieceLenIter::new(self.total_len, self.piece_len)
                .enumerate()
                .map(|(index, len)| FileFragment::new(index, 0, 0, len))
                .collect();

        let mut ret = vec![];
        for (file_len, path) in self.files {
            let mut items: Vec<FileFragment> = vec![];

            let mut file_frag = FileFragment::new(0, 0, 0, file_len);

            while file_frag.len > 0 {
                let piece_frag = piece_frag_que.pop_front().unwrap();

                //    min_len
                //          V  V      V         V         V    V
                // [    F1     ][            F2                ][F3 ]
                // [   P1   ][  !P2   ][   P3   ][   P4   ][P5      ]

                let min_len = cmp::min(file_frag.len, piece_frag.len);
                let (mut file_left, file_right) = file_frag.cut(min_len);
                let (piece_left, piece_right) = piece_frag.cut(min_len);

                file_left.piece_index = piece_left.piece_index;
                file_left.piece_offset = piece_left.piece_offset;

                assert_eq!(file_left.len, min_len);
                assert_eq!(piece_left.len, min_len);

                items.push(file_left);
                file_frag = file_right;

                if piece_right.len > 0 {
                    piece_frag_que.push_front(piece_right);
                }
            }

            assert_eq!(file_frag.len, 0);

            ret.push(FilePieceMap::new(path, items))
        }
        Ok(ret)
    }
}

#[derive(Debug, Default)]
pub struct PieceManager {
    name: String,
    cache: BTreeMap<usize, Piece>,
    first_piece_len: usize,
    piece_num: usize,
    last_piece_len: usize,
    maps: Vec<FilePieceMap>,
    cache_size: usize,
    checked_bits: BitVec,
    checked_bits_path: PathBuf,
    total_size: usize,
}

impl PieceManager {
    pub fn from_torrent_info(data_dir: impl AsRef<Path>, info: &TorrentInfo) -> Result<Self> {
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
        cache.load_bits()?;
        Ok(cache)
    }

    pub fn from_single_file(
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
            maps: builder.build().unwrap(),
            checked_bits: BitVec::from_elem(piece_num, false),
            checked_bits_path: path.as_ref().join(".snail_checked_bits"),
            total_size,
        };
        cache.load_bits()?;
        Ok(cache)
    }

    pub fn empty() -> Self {
        Default::default()
    }

    pub fn fetch(&mut self, index: usize) -> Result<&Piece> {
        if self.cache.contains_key(&index) {
            return Ok(self.cache.get_mut(&index).unwrap());
        }

        if index >= self.piece_num {
            return Err(Error::PieceNotFound(index));
        }

        let piece_len = self.piece_len(index);
        let mut piece = [Piece::new(index, piece_len)];
        for m in self.maps.iter() {
            if m.contains_index(index) {
                // TODO: fill more piece at once
                m.read(&mut piece)?;
            }
        }
        let [piece] = piece;

        // self.expire();
        self.cache.insert(index, piece);
        Ok(self.cache.get_mut(&index).unwrap())
    }

    pub fn write(&mut self, piece: Piece) -> Result<()> {
        self.cache.insert(piece.index, piece);
        if self.cache.len() > 100 {
            self.flush()?;
        }
        Ok(())
    }

    pub fn read(&mut self, index: usize) -> Result<Piece> {
        self.fetch(index).cloned()
    }

    pub fn clear_all_checked_bits(&mut self) -> Result<()> {
        self.checked_bits.clear();
        Ok(())
    }

    pub fn all_checked(&mut self) -> bool {
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

    pub fn fix_file_size(&mut self, path: impl AsRef<Path>) -> Result<()> {
        if let Some(m) = self.maps.iter().find(|m| m.path == path.as_ref()) {
            let metadata = m.path.metadata()?;
            if metadata.len() as usize > m.size {
                let f = fs::OpenOptions::new().write(true).open(path.as_ref())?;
                f.set_len(m.size as u64)?;
            }
        }
        Ok(())
    }

    pub fn check(&mut self, index: usize, checksum: &[u8]) -> Result<bool> {
        let piece = self.fetch(index)?;
        let b = match checksum.len() {
            20 => piece.sha1() == checksum,
            32 => piece.sha256() == checksum,
            size => return Err(Error::BadChecksumSize(size)),
        };
        self.checked_bits.set(index, b);
        Ok(b)
    }

    pub fn check_file<'a>(
        &mut self,
        path: impl AsRef<Path>,
        checksum_fn: impl Fn(usize) -> &'a [u8],
    ) -> Result<bool> {
        let indices = self
            .maps
            .iter()
            .find(|m| m.path == path.as_ref())
            .map(|m| m.piece_indices.clone())
            .unwrap_or_else(HashSet::new);
        let mut b = true;
        for index in indices {
            self.cache.remove(&index);
            b &= self.check(index, checksum_fn(index))?;
        }
        Ok(b)
    }

    pub fn flush(&mut self) -> Result<()> {
        debug!(?self.name, "piece manager flush");
        for (_, piece) in self.cache.iter() {
            for m in self.maps.iter() {
                if m.contains_index(piece.index) {
                    let slice = std::slice::from_ref(piece);
                    m.write(slice)?;
                }
            }
        }
        self.save_bits()?;
        self.cache.clear();
        Ok(())
    }

    pub fn piece_len(&self, index: usize) -> usize {
        if index >= self.piece_num {
            panic!("index out of bound");
        }
        if index == self.piece_num - 1 {
            self.last_piece_len
        } else {
            self.first_piece_len
        }
    }

    pub fn piece_num(&self) -> usize {
        self.piece_num
    }

    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn cache_size(&self) -> usize {
        self.cache_size
    }

    pub fn update_cache_size(&mut self, size: usize) -> usize {
        let old = self.cache_size;
        self.cache_size = size;
        old
    }

    fn save_bits(&mut self) -> Result<()> {
        debug!(name=?self.name, "save bits");
        fs::write(&self.checked_bits_path, self.checked_bits.to_bytes())?;
        Ok(())
    }

    fn load_bits(&mut self) -> Result<()> {
        debug!(name=?self.name, "load bits");

        if let Some(bits) = self.read_bit_vec(&self.checked_bits_path, self.checked_bits.len()) {
            self.checked_bits = bits;
        }

        debug!(bits = ?self.checked_bits);
        Ok(())
    }

    fn read_bit_vec(&self, path: &Path, bit_len: usize) -> Option<BitVec> {
        let data = match fs::read(path) {
            Ok(data) => Ok(data),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(vec![]),
            Err(e) => Err(Error::Io(e)),
        };
        match data {
            Ok(data) => {
                let mut tmp = BitVec::from_bytes(&data);
                while tmp.len() > bit_len {
                    tmp.pop();
                }
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

    pub fn checked_bits(&self) -> &BitVec {
        &self.checked_bits
    }

    pub fn paths(&self) -> Vec<&Path> {
        self.maps.iter().map(|m| m.path.as_path()).collect()
    }
}

struct PieceLog {
    index: usize,
    peer: Option<(Arc<HashId>, Instant)>,
    offset: usize,
    bits: BitVec,
    buf: Vec<u8>,
    reject: uluru::LRUCache<Arc<HashId>, 20>,
}

impl Debug for PieceLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceLog")
            .field("index", &self.index)
            .field("peer", &self.peer)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl PieceLog {
    fn new(index: usize, piece_len: usize) -> Self {
        Self {
            index,
            peer: None,
            offset: 0,
            bits: BitVec::from_elem(piece_len, false),
            buf: vec![0u8; piece_len],
            reject: Default::default(),
        }
    }
}

fn shuffle_slice<T>(mut slice: &mut [T]) {
    let mut rng = rand::thread_rng();
    while !slice.is_empty() {
        let n: usize = rng.gen_range(0..slice.len());
        slice.swap(0, n);
        slice = &mut slice[1..];
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpeedRecord {
    max_req_bytes: usize,
    req_bytes: usize,
}

pub struct PieceLogManager {
    max_req: usize,
    logs: HashMap<usize, PieceLog>,
    all_checked: bool,
    fragment_len: usize,
    adaptive_speed: HashMap<Arc<HashId>, SpeedRecord>,
    sync_adaptive_at: Instant,
}

impl PieceLogManager {
    pub fn new() -> Self {
        Self {
            max_req: 100,
            logs: HashMap::new(),
            all_checked: false,
            fragment_len: 16 << 10,
            adaptive_speed: HashMap::default(),
            sync_adaptive_at: Instant::now(),
        }
    }

    fn clear(&mut self) {
        self.logs.clear();
        self.adaptive_speed.clear();
    }

    pub fn sync(&mut self, pm: &mut PieceManager) -> Result<()> {
        self.all_checked = pm.all_checked();

        if self.all_checked {
            self.clear();
            return Ok(());
        }

        for i in 0..pm.piece_num() {
            if pm.is_checked(i) {
                debug!(index=?i, "piece is finished, remove log");
                self.logs.remove(&i);
            }
        }

        let remain_req = self.max_req.saturating_sub(self.logs.len());

        let candidate_indices: Vec<usize> = {
            let mut list: Vec<usize> = (0..pm.piece_num())
                .filter(|&i| !pm.is_checked(i) && !self.logs.contains_key(&i))
                .collect();
            shuffle_slice(&mut list);
            list.truncate(remain_req);
            list
        };

        for index in candidate_indices {
            self.logs
                .insert(index, PieceLog::new(index, pm.piece_len(index)));
        }

        self.sync_adaptive_max_req();

        Ok(())
    }

    fn sync_adaptive_max_req(&mut self) {
        const STEP: usize = 128 << 10;
        if self.sync_adaptive_at.elapsed() < Duration::from_secs(1) {
            return;
        }
        self.sync_adaptive_at = Instant::now();

        for (k, v) in self.adaptive_speed.iter_mut() {
            if v.max_req_bytes == 0 {
                v.max_req_bytes = STEP;
            }
            if v.req_bytes == 0 {
                v.max_req_bytes += STEP;
            }
            if v.req_bytes >= v.max_req_bytes {
                v.max_req_bytes.checked_sub(STEP).unwrap_or(STEP);
            }
            debug!(?k, ?v);
        }
    }

    pub fn pull(&mut self, peer_id: Arc<HashId>) -> Vec<PieceInfo> {
        if self.all_checked {
            return vec![];
        }

        let mut ret = vec![];

        let req_record = self
            .adaptive_speed
            .entry(Arc::clone(&peer_id))
            .or_insert_with(Default::default);

        for log in self.logs.values_mut() {
            if req_record.req_bytes > req_record.max_req_bytes {
                debug!(?req_record, "exceed max req bytes capacity");
                break;
            }

            let mut skip =
                matches!(log.peer, Some((_, at)) if at.elapsed() < Duration::from_secs(15));
            skip |= log.reject.find(|id| id == &peer_id).is_some();
            if !skip {
                log.peer = Some((Arc::clone(&peer_id), Instant::now()));
                let len = cmp::min(log.bits.len() - log.offset, self.fragment_len);
                ret.push(PieceInfo::new(log.index, log.offset, len));
                req_record.req_bytes += len
            }
        }

        ret
    }

    pub fn on_piece_data(
        &mut self,
        index: usize,
        begin: usize,
        buf: &[u8],
        peer_id: Arc<HashId>,
    ) -> Option<Piece> {
        if self.all_checked {
            return None;
        }

        if let Some(record) = self.adaptive_speed.get_mut(&peer_id) {
            record.req_bytes = record.req_bytes.saturating_sub(buf.len());
        }

        let complete = if let Some(log) = self.logs.get_mut(&index) {
            let end = cmp::min(begin + buf.len(), log.buf.len());
            log.buf[begin..end].copy_from_slice(buf);
            for i in begin..end {
                log.bits.set(i, true);
            }

            // find first index of data which is not received
            if let Some((offset, _)) = log.bits.iter().enumerate().find(|(_, b)| !b) {
                // request the rest data
                log.offset = offset;
                log.peer = None;
                false
            } else {
                true
            }
        } else {
            false
        };
        if complete {
            debug!(?index, "complete piece");
            let log = self.logs.get(&index).unwrap();
            let piece = Piece::from_buf(log.index, log.buf.to_vec());
            Some(piece)
        } else {
            None
        }
    }

    pub fn used_indices(&self) -> Vec<usize> {
        self.logs.keys().copied().collect()
    }

    pub fn on_reject(&mut self, index: usize, peer_id: Arc<HashId>) {
        if let Some(log) = self.logs.get_mut(&index) {
            log.reject.insert(peer_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::torrent::TorrentFile;
    use glob::glob;
    use std::{
        fs,
        io::{Seek, Write},
        path::PathBuf,
    };

    use super::*;

    #[test]
    fn test_shuffle() {
        let mut a = [0, 1, 2, 3];
        shuffle_slice(&mut a);
        println!("{a:?}");
    }

    #[test]
    fn test_piece() {
        let p = Piece::new(0, 10);
        assert_eq!(p.index, 0);
        assert_eq!(p.buf.len(), 10);
        p.sha1();
    }

    #[test]
    fn test_piece_len_iter() {
        let iter = PieceLenIter::new(10, 2);
        assert_eq!(iter.count(), 5);

        let iter = PieceLenIter::new(10, 3);
        assert_eq!(iter.collect::<Vec<usize>>(), vec![3, 3, 3, 1]);

        let iter = PieceLenIter::new(0, 0);
        assert_eq!(iter.collect::<Vec<usize>>(), vec![0usize; 0]);

        let iter = PieceLenIter::new(8, 8);
        assert_eq!(iter.collect::<Vec<usize>>(), vec![8]);
    }

    #[test]
    fn empty_piece_manager() -> Result<()> {
        let mut pm = PieceManager::empty();

        assert!(pm.all_checked());
        assert!(!pm.check(0, &[0])?);
        assert!(pm.fetch(0).is_err());
        assert_eq!(pm.piece_num(), 0);

        Ok(())
    }

    #[test]
    fn piece_manager_single_file() -> Result<()> {
        let mut pm =
            PieceManager::from_single_file("/tmp/snail/single_file.torrent", 20242, 16384)?;
        for i in 0..pm.piece_num() {
            let p = pm.fetch(i)?;
            assert!(p.buf.iter().all(|byte| *byte == 0));
        }
        Ok(())
    }

    fn setup_test_torrent() -> PathBuf {
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

    fn setup_piece_manager() -> Result<(TorrentFile, PieceManager)> {
        let torrent_path = setup_test_torrent();

        let torrent = TorrentFile::from_path(&torrent_path)?;

        let pm = PieceManager::from_torrent_info(torrent_path.parent().unwrap(), &torrent.info)?;
        Ok((torrent, pm))
    }

    #[test]
    fn piece_manager_torrent_info() -> Result<()> {
        setup_piece_manager()?;
        Ok(())
    }

    #[test]
    fn piece_manager() -> Result<()> {
        let torrent_path = setup_test_torrent();

        let torrent = TorrentFile::from_path(&torrent_path)?;

        let sha1_fn = |index: usize| torrent.info.get_piece_sha1(index);

        let mut pm =
            PieceManager::from_torrent_info(torrent_path.parent().unwrap(), &torrent.info)?;

        for i in 0..pm.piece_num() {
            let p = pm.read(i)?;
            pm.write(p)?;
        }

        for i in 0..pm.piece_num() {
            assert!(pm.check(i, sha1_fn(i))?);
        }

        for i in 0..pm.piece_num() {
            assert!(pm.is_checked(i));
        }

        {
            let origin_piece = pm.read(0)?;
            let mut piece = origin_piece.clone();
            piece.buf[0..100].copy_from_slice(&[1; 100]);
            pm.write(piece)?;
            assert!(!pm.check(0, sha1_fn(0))?);
            pm.write(origin_piece)?;
            assert!(pm.check(0, sha1_fn(0))?);
        }

        {
            let paths = pm
                .paths()
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
                assert!(!pm.check_file(p, sha1_fn).unwrap());
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
                assert!(pm.check_file(p, sha1_fn).unwrap());
            }
        }

        {
            let origin_size = pm.maps[0].size as u64;
            let path = pm.maps[0].path.clone();
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
            pm.fix_file_size(path).unwrap();
            assert_eq!(file.metadata().unwrap().len(), origin_size);
        }

        Ok(())
    }

    #[test]
    fn piece_log() {
        let log = PieceLog::new(0, 100);
        assert_eq!(log.bits.len(), 100);
        assert_eq!(log.buf.len(), 100);
        assert_eq!(log.peer, None);
    }

    #[test]
    fn piece_log_man() -> Result<()> {
        let peer_id = Arc::new(HashId::ZERO_V1);

        let mut plm = PieceLogManager::new();
        plm.clear();

        let (torrent, mut pm) = setup_piece_manager()?;
        let pieces: Vec<Piece> = (0..pm.piece_num()).map(|i| pm.read(i).unwrap()).collect();
        pm.clear_all_checked_bits()?;

        plm.sync(&mut pm)?;

        assert!(!plm.pull(peer_id.clone()).is_empty());
        assert!(plm.pull(peer_id.clone()).is_empty());

        plm.clear();
        plm.sync(&mut pm)?;

        assert!(!plm.pull(peer_id.clone()).is_empty());

        for piece in pieces {
            let ret_piece = plm
                .on_piece_data(piece.index, 0, piece.buf(), Arc::clone(&peer_id))
                .unwrap();
            pm.write(ret_piece)?;
            assert!(pm.check(piece.index, torrent.info.get_piece_sha1(piece.index))?);
        }

        Ok(())
    }
}
