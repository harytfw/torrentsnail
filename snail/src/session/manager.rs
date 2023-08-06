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
use std::time::{Duration, Instant};
use tracing::debug;

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

    pub fn sync(&mut self, sm: &mut StorageManager) -> Result<()> {
        self.all_checked = sm.all_checked();

        if self.all_checked {
            self.clear();
            return Ok(());
        }

        for i in 0..sm.piece_num() {
            if sm.is_checked(i) {
                debug!(index=?i, "piece is finished, remove log");
                self.piece_state.remove(&i);
            }
        }

        let remain_req = self.max_req.saturating_sub(self.piece_state.len());

        let candidate_indices: Vec<usize> = {
            let mut list: Vec<usize> = (0..sm.piece_num())
                .filter(|&i| !sm.is_checked(i) && !self.piece_state.contains_key(&i))
                .collect();
            shuffle_slice(&mut list);
            list.truncate(remain_req);
            list
        };

        for index in candidate_indices {
            self.piece_state
                .insert(index, PieceActivityState::new(index, sm.piece_len(index)));
        }

        Ok(())
    }

    pub fn sync_peer_pieces(&mut self, peer_id: &HashId, pieces: &BitVec) -> Result<()> {
        if !pieces.is_empty() {
            self.peer_owned_pieces.insert(*peer_id, pieces.clone());
        }
        Ok(())
    }

    pub fn pull_req(&mut self, peer_id: &HashId) -> Vec<PieceInfo> {
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::session::storage::tests::setup_storage_manager;

    #[test]
    fn test_state() {
        let log = PieceActivityState::new(0, 100);
        assert_eq!(log.bits.len(), 100);
        assert_eq!(log.buf.len(), 100);
        assert_eq!(log.peer, None);
    }

    #[tokio::test]
    async fn test_manager() -> Result<()> {
        let peer_id = HashId::ZERO_V1;

        let mut plm = PieceActivityManager::new();
        plm.clear();

        let (torrent, mut pm) = setup_storage_manager()?;
        let mut pieces: Vec<Piece> = vec![];
        for i in 0..pm.piece_num() {
            pieces.push(pm.read(i).await.unwrap());
        }
        pm.clear_all_checked_bits()?;

        plm.sync(&mut pm)?;

        assert!(!plm.pull_req(&peer_id).is_empty());
        assert!(plm.pull_req(&peer_id).is_empty());

        plm.clear();
        plm.sync(&mut pm)?;

        assert!(!plm.pull_req(&peer_id).is_empty());

        for piece in pieces {
            let ret_piece = plm
                .on_piece_data(piece.index(), 0, piece.buf(), &peer_id)
                .unwrap();
            pm.write(ret_piece).await?;
            assert!(
                pm.check(piece.index(), torrent.info.get_piece_sha1(piece.index()))
                    .await?
            );
        }

        Ok(())
    }
}
