use crate::message::PieceInfo;
use crate::ratelimiter::RateLimiter;
use crate::session::piece::Piece;
use crate::session::storage::StorageManager;
use crate::session::utils::shuffle_slice;
use crate::torrent::HashId;
use crate::Result;
use bit_vec::BitVec;
use std::cmp;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

use super::storage::{PieceSnapshot, StorageSnapshot};

struct PieceActivityState {
    index: usize,
    peer: Option<(HashId, Instant)>,
    offset: usize,
    bits: BitVec,
    buf: Vec<u8>,
    reject: uluru::LRUCache<HashId, 20>,
}

impl Debug for PieceActivityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PieceLog")
            .field("index", &self.index)
            .field("peer", &self.peer)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl PieceActivityState {
    fn new(index: usize, piece_len: usize) -> Self {
        Self {
            index,
            peer: None,
            offset: 0,
            bits: BitVec::from_elem(piece_len, false),
            buf: vec![0; piece_len],
            reject: Default::default(),
        }
    }
}

const ADAPTIVE_BYTES_STEP: f64 = (128 << 10) as f64;

pub struct PieceActivityManager {
    max_req: usize,
    piece_state: HashMap<usize, PieceActivityState>,
    all_checked: bool,
    fragment_len: usize,
    adaptive_speed: HashMap<HashId, RateLimiter>,
    peer_owned_pieces: HashMap<HashId, BitVec>,
}

impl PieceActivityManager {
    pub fn new() -> Self {
        Self {
            max_req: 100,
            piece_state: HashMap::new(),
            all_checked: false,
            fragment_len: 16 << 10,
            adaptive_speed: Default::default(),
            peer_owned_pieces: HashMap::default(),
        }
    }

    fn clear(&mut self) {
        self.piece_state.clear();
        self.peer_owned_pieces.clear();
    }

    pub fn sync_snapshot(&mut self, snapshot: &StorageSnapshot) -> Result<()> {
        self.all_checked = snapshot.all_checked();

        if self.all_checked {
            self.clear();
            return Ok(());
        }

        for piece in snapshot.pieces.iter() {
            if !piece.checked {
                continue;
            }
            debug!(index=?piece.index, "piece is finished, remove log");
            self.piece_state.remove(&piece.index);
        }

        let remain_req = self.max_req.saturating_sub(self.piece_state.len());

        let candidate_piece: Vec<PieceSnapshot> = {
            let mut list: Vec<PieceSnapshot> = snapshot
                .pieces
                .iter()
                .filter(|piece| !piece.checked && !self.piece_state.contains_key(&piece.index))
                .cloned()
                .collect();
            shuffle_slice(&mut list);
            list.truncate(remain_req);
            list
        };

        for piece in candidate_piece {
            self.piece_state.insert(
                piece.index,
                PieceActivityState::new(piece.index, piece.piece_len),
            );
        }

        Ok(())
    }

    pub fn sync_peer_pieces(&mut self, peer_id: &HashId, pieces: &BitVec) -> Result<()> {
        if !pieces.is_empty() {
            self.peer_owned_pieces.insert(*peer_id, pieces.clone());
        }
        Ok(())
    }

    pub fn request_piece_from_peer(&mut self, peer_id: &HashId) -> Vec<PieceInfo> {
        let mut ret = vec![];

        if self.all_checked {
            return ret;
        }

        let limiter = self
            .adaptive_speed
            .entry(*peer_id)
            .or_insert_with(|| RateLimiter::new(ADAPTIVE_BYTES_STEP, ADAPTIVE_BYTES_STEP));

        for p in self.piece_state.values_mut() {
            let len = cmp::min(p.bits.len() - p.offset, self.fragment_len);

            let no_limit = limiter.take(len as f64);

            if !no_limit {
                if limiter.burst() > ADAPTIVE_BYTES_STEP {
                    limiter.set_burst(limiter.burst() - ADAPTIVE_BYTES_STEP);
                }
                break;
            }

            let have_this_piece = self
                .peer_owned_pieces
                .get(peer_id)
                .and_then(|owned| owned.get(p.index))
                .unwrap_or(true);

            let timeout = match p.peer {
                Some((_, at)) if at.elapsed() < Duration::from_secs(15) => false,
                _ => true,
            };

            let reject = p.reject.find(|id| id == peer_id).is_some();

            let pick = !reject && timeout && have_this_piece;

            if pick {
                p.peer = Some((*peer_id, Instant::now()));
                ret.push(PieceInfo::new(p.index, p.offset, len));
            }
        }

        ret
    }

    pub fn on_piece_data(
        &mut self,
        index: usize,
        begin: usize,
        buf: &[u8],
        peer_id: &HashId,
    ) -> Option<Piece> {
        if self.all_checked {
            return None;
        }

        if let Some(limiter) = self.adaptive_speed.get_mut(peer_id) {
            limiter.put(buf.len() as f64);
            if limiter.can_take(limiter.burst()) {
                limiter.set_burst(limiter.burst() + ADAPTIVE_BYTES_STEP)
            }
        }

        let complete = if let Some(log) = self.piece_state.get_mut(&index) {
            let end = cmp::min(begin + buf.len(), log.buf.len());

            log.buf[begin..end].copy_from_slice(buf);

            for i in begin..end {
                log.bits.set(i, true);
            }

            // find first index of data which is not received
            // TODO: record first index of not-received byte, then we don't need to loop over bitmap
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
            let log = self.piece_state.get(&index).unwrap();
            let piece = Piece::from_buf(log.index, &log.buf);
            Some(piece)
        } else {
            None
        }
    }

    pub fn used_indices(&self) -> Vec<usize> {
        self.piece_state.keys().copied().collect()
    }

    pub fn on_reject(&mut self, index: usize, peer_id: &HashId) {
        if let Some(log) = self.piece_state.get_mut(&index) {
            log.reject.insert(*peer_id);
        }
    }
}

#[derive(Clone)]
pub struct AtomicPieceActivityManager {
    inner: Arc<RwLock<PieceActivityManager>>,
}

impl AtomicPieceActivityManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(PieceActivityManager::new())),
        }
    }

    pub async fn sync(&self, ss: &StorageSnapshot) -> Result<()> {
        self.inner.write().await.sync_snapshot(ss)
    }

    pub async fn sync_peer_pieces(&self, peer_id: &HashId, pieces: &BitVec) -> Result<()> {
        self.inner.write().await.sync_peer_pieces(peer_id, pieces)
    }

    pub async fn make_piece_request(&self, peer_id: &HashId) -> Vec<PieceInfo> {
        self.inner.write().await.request_piece_from_peer(peer_id)
    }

    pub async fn on_piece_data(
        &self,
        index: usize,
        begin: usize,
        buf: &[u8],
        peer_id: &HashId,
    ) -> Option<Piece> {
        self.inner
            .write()
            .await
            .on_piece_data(index, begin, buf, peer_id)
    }

    pub async fn used_indices(&self) -> Vec<usize> {
        self.inner.read().await.used_indices()
    }

    pub async fn on_reject(&self, index: usize, peer_id: &HashId) {
        self.inner.write().await.on_reject(index, peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state() {
        let log = PieceActivityState::new(0, 100);
        assert_eq!(log.bits.len(), 100);
        assert_eq!(log.buf.len(), 100);
        assert_eq!(log.peer, None);
    }

    #[tokio::test]
    async fn test_activity_manager() -> Result<()> {
        let storage_snapshot = StorageSnapshot::new(vec![
            PieceSnapshot::new(0, false, 100, bytes::Bytes::from_static(&HashId::ZERO_V1)),
            PieceSnapshot::new(1, false, 100, bytes::Bytes::from_static(&HashId::ZERO_V1)),
            PieceSnapshot::new(2, false, 100, bytes::Bytes::from_static(&HashId::ZERO_V1)),
        ]);

        let peer_id = HashId::ZERO_V1;

        let mut plm = PieceActivityManager::new();
        plm.clear();

        plm.sync_snapshot(&storage_snapshot)?;

        assert!(!plm.request_piece_from_peer(&peer_id).is_empty());
        assert!(plm.request_piece_from_peer(&peer_id).is_empty());

        plm.clear();

        plm.sync_snapshot(&storage_snapshot)?;

        assert!(!plm.request_piece_from_peer(&peer_id).is_empty());

        Ok(())
    }
}
