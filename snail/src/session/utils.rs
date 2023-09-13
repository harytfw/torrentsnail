use crate::session::TorrentSession;
use crate::torrent::HashId;
use crate::Result;
use rand::Rng;
use serde::Serialize;
use std::{
    cmp,
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    time::SystemTime,
};

pub fn shuffle_slice<T>(mut slice: &mut [T]) {
    let mut rng = rand::thread_rng();
    while !slice.is_empty() {
        let n: usize = rng.gen_range(0..slice.len());
        slice.swap(0, n);
        slice = &mut slice[1..];
    }
}

pub fn calc_piece_num(total_len: usize, piece_len: usize) -> (usize, usize) {
    let (mut piece_num, last_piece_len) = (total_len / piece_len, total_len % piece_len);

    if last_piece_len != 0 {
        piece_num += 1
    }
    (piece_num, last_piece_len)
}

// bep007 -  `The key should remain the same for a particular infohash during a torrent session. `
pub fn make_announce_key(my_id: &HashId, info_hash: &HashId) -> u32 {
    let mut hasher = DefaultHasher::new();
    my_id.hash(&mut hasher);
    info_hash.hash(&mut hasher);
    let hash = hasher.finish();
    (hash >> 32) as u32
}

pub fn timestamp_sec(t: SystemTime) -> u64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(dur) => dur.as_secs(),
        _ => 0,
    }
}

pub fn calc_speed(m: &BTreeMap<u64, usize>) -> usize {
    let mut start = u64::MAX;
    let mut end = u64::MIN;
    let mut bytes = 0usize;
    for (k, v) in m.iter().rev().take(10) {
        start = cmp::min(start, *k);
        end = cmp::max(end, *k);
        bytes += v;
    }
    let elapse = (end.saturating_sub(start) + 1) as usize;
    bytes / elapse
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_shuffle() {
        let mut a = [0, 1, 2, 3];
        shuffle_slice(&mut a);
        println!("{a:?}");
    }

    #[test]
    fn test_calc_piece_num() {
        assert_eq!((3, 200), calc_piece_num(1000, 400));
    }

    #[test]
    fn test_timestamp_sec() {
        assert_eq!(timestamp_sec(SystemTime::UNIX_EPOCH), 0);
        assert_eq!(
            timestamp_sec(SystemTime::UNIX_EPOCH - Duration::from_secs(100)),
            0
        );
        assert_eq!(
            timestamp_sec(SystemTime::UNIX_EPOCH + Duration::from_secs(100)),
            100
        );
    }

    #[test]
    fn test_calc_speed() {
        let mut bytes = vec![
            (0, 100),
            (1, 100),
            (2, 100),
            (3, 100),
            (4, 100),
            (5, 100),
            (6, 100),
            (7, 100),
            (8, 100),
            (9, 100),
        ];
        assert_eq!(calc_speed(&BTreeMap::from_iter(bytes.iter().cloned())), 100);
        bytes.push((10, 1100));
        assert_eq!(
            calc_speed(&BTreeMap::from_iter(bytes.iter().cloned())),
            (900 + 1100) / 10
        );

        assert_eq!(
            calc_speed(&BTreeMap::from_iter(bytes.iter().cloned())),
            (900 + 1100) / 10
        );

        assert_eq!(calc_speed(&BTreeMap::new()), 0);
        assert_eq!(calc_speed(&BTreeMap::from_iter([(1, 100)])), 100);
    }
}

async fn persistent_piece_state(session: &TorrentSession) -> Result<()> {
    {
        session.main_sm.save_bits().await?;
    }
    {
        session.aux_sm.save_bits().await?;
    }
    Ok(())
}


pub fn compute_torrent_path(data_dir: &Path, info_hash: &HashId) -> PathBuf {
    data_dir.join(format!("{}.torrent", info_hash.hex()))
}


pub const METADATA_PIECE_SIZE: usize = 16384;

