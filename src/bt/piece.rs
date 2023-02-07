use tracing::debug;

use crate::bt::types::PieceInfo;
use crate::torrent::HashId;
use crate::Result;
use std::borrow::Borrow;
use std::cmp;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::peer::Peer;

const MAX_FRAGMENT_LENGTH: usize = 16 << 10;

#[derive(Debug, Default, Clone, Copy)]
pub enum PieceState {
    #[default]
    Pending,
    Downloading,
    Verified,
}

#[derive(Clone)]
pub struct Piece {
    state: PieceState,
    buf: Vec<u8>,
    bv: bit_vec::BitVec,
    weight: i32,
    last_new_data_at: Instant,
    last_req_at: Instant,
    index: usize,
    ban_peer_id: HashSet<Arc<HashId>>,
}

impl Debug for Piece {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Piece")
            .field("state", &self.state)
            .field("len", &self.bv.len())
            .field("index", &self.index)
            .field("done", &self.is_all_received())
            .field("percent", &self.percent())
            .finish()
    }
}

impl Piece {
    pub fn new(index: usize, piece_len: usize) -> Self {
        Self {
            state: PieceState::Pending,
            buf: vec![],
            bv: bit_vec::BitVec::from_elem(piece_len, false),
            weight: 0,
            last_new_data_at: Instant::now(),
            last_req_at: Instant::now(),
            index,
            ban_peer_id: Default::default(),
        }
    }

    pub fn from_length(mut total_len: usize, piece_len: usize) -> Vec<Self> {
        if piece_len == 0 || total_len == 0 {
            return vec![];
        }
        let mut r = Vec::with_capacity(total_len / piece_len);
        while total_len > 0 {
            r.push(Self::new(r.len(), cmp::min(total_len, piece_len)));
            total_len -= cmp::min(total_len, piece_len);
        }
        r
    }

    pub fn percent(&self) -> f32 {
        self.received() as f32 / self.size() as f32
    }

    pub fn received(&self) -> usize {
        self.bv.iter().map(|b| b as usize).sum::<usize>()
    }

    pub fn get_weight(&self) -> i32 {
        self.weight
    }

    pub fn get_state(&self) -> PieceState {
        self.state
    }

    pub fn set_state(&mut self, state: PieceState) {
        self.state = state;
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn should_send_req(&self) -> bool {
        if self.is_verified() {
            return false;
        }
        if self.is_pending() {
            return true;
        }
        self.is_downloading() && self.last_new_data_at.elapsed() > Duration::from_secs(30)
    }

    pub fn should_send_req_to_peer(&self, peer: &Peer) -> bool {
        self.should_send_req() && !self.ban_peer_id.contains(&peer.peer_id)
    }

    pub fn is_pending(&self) -> bool {
        matches!(self.state, PieceState::Pending)
    }

    pub fn is_downloading(&self) -> bool {
        matches!(self.state, PieceState::Downloading)
    }

    pub fn is_verified(&self) -> bool {
        matches!(self.state, PieceState::Verified)
    }

    pub fn is_all_received(&self) -> bool {
        self.bv.all()
    }

    pub fn on_data(&mut self, begin: usize, data: &[u8]) -> Result<()> {
        let end = {
            let tmp_end = begin + data.len();
            if tmp_end > self.bv.len() {
                debug!(
                    "exceed piece length: {}, actually: {}",
                    self.bv.len(),
                    tmp_end
                );
                self.bv.len()
            } else {
                tmp_end
            }
        };

        if self.buf.len() < end {
            self.buf.resize(end, 0);
        }

        for i in begin..end {
            self.bv.set(i, true);
        }

        self.buf[begin..end].copy_from_slice(data);

        self.last_new_data_at = Instant::now();

        Ok(())
    }

    pub fn on_send_req(&mut self) {
        self.set_state(PieceState::Downloading);
        self.last_req_at = Instant::now();
    }

    pub fn on_verify_sha1(&mut self, expected: &[u8]) -> bool {
        if self.sha1() == expected {
            self.set_state(PieceState::Verified);
            true
        } else {
            self.set_state(PieceState::Pending);
            self.bv.clear();
            false
        }
    }

    fn compute_fragments(&self, f: impl Fn(usize, usize) -> bool) -> Vec<PieceFragment> {
        let mut res = vec![];
        let mut i = 0;
        while i < self.bv.len() {
            if !f(i, 1) {
                i += 1;
                continue;
            }
            let mut j = i;
            while j < self.bv.len() && f(j, j - i + 1) {
                j += 1;
            }
            res.push((i, j - i));
            i = j;
        }
        res.into_iter()
            .map(|(begin, len)| PieceFragment {
                index: self.get_index(),
                weight: self.get_weight(),
                begin,
                len,
            })
            .collect()
    }

    pub fn finished_fragments(&self, req_begin: usize, req_len: usize) -> Vec<PieceFragment> {
        let req_len = cmp::min(req_len, MAX_FRAGMENT_LENGTH);
        self.compute_fragments(|begin, len| begin >= req_begin && len <= req_len && self.bv[begin])
    }

    pub fn pending_fragments(&self) -> Vec<PieceFragment> {
        self.compute_fragments(|begin, len| !self.bv[begin] && len < MAX_FRAGMENT_LENGTH)
    }

    pub fn size(&self) -> usize {
        self.bv.len()
    }

    pub fn get_buf(&self) -> &[u8] {
        &self.buf
    }

    pub fn get_buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn add_band_peer_id(&mut self, peer_id: Arc<HashId>) {
        self.ban_peer_id.insert(peer_id);
    }

    pub fn sha1(&self) -> [u8; 20] {
        let info_hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &self.buf);
        let result: [u8; 20] = info_hash.as_ref().try_into().unwrap();
        result
    }
}

#[derive(Debug, Clone)]
pub struct PieceFragment {
    index: usize,
    begin: usize,
    len: usize,
    weight: i32,
}

impl<T: Borrow<PieceFragment>> From<T> for PieceInfo {
    fn from(t: T) -> Self {
        let b = t.borrow();
        Self {
            index: u32::try_from(b.index).unwrap(),
            begin: u32::try_from(b.begin).unwrap(),
            length: u32::try_from(b.len).unwrap(),
        }
    }
}

impl PieceFragment {
    pub fn get_index(&self) -> usize {
        self.index
    }
    pub fn get_begin(&self) -> usize {
        self.begin
    }
    pub fn get_len(&self) -> usize {
        self.len
    }
    pub fn get_weight(&self) -> i32 {
        self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compute_fragments() {
        let mut piece = Piece::new(0, 10); // [0 0 0 0 0 0 0 0 0 0]

        assert_eq!(piece.compute_fragments(|i, _| !piece.bv[i]).len(), 1);

        piece.on_data(0, &[1]).unwrap(); // [1 0 0 0 0 0 0 0 0 0]

        assert_eq!(piece.compute_fragments(|i, _| !piece.bv[i]).len(), 1);

        piece.on_data(4, &[0]).unwrap(); // [1 0 0 0 1 0 0 0 0 0]

        assert_eq!(piece.compute_fragments(|i, _| !piece.bv[i]).len(), 2);

        assert_eq!(piece.compute_fragments(|i, _| piece.bv[i]).len(), 2);

        assert_eq!(
            piece
                .compute_fragments(|i, len| !piece.bv[i] && len == 1)
                .len(),
            8
        );
        assert_eq!(
            piece
                .compute_fragments(|i, len| !piece.bv[i] && len <= 4)
                .len(),
            3
        );

        piece.on_data(5, &[0]).unwrap(); // [1 0 0 0 1 1 0 0 0 0]

        assert_eq!(piece.compute_fragments(|i, _| piece.bv[i]).len(), 2);
    }
}
