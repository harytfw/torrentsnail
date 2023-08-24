use std::fmt::Debug;
use std::cmp;
use bytes::BytesMut;

#[derive(Clone)]
pub struct Piece {
    buf: BytesMut,
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
            buf: BytesMut::zeroed(piece_len),
            index,
        }
    }

    pub fn from_buf(index: usize, buf: &[u8]) -> Self {
        Self {
            index,
            buf: BytesMut::from_iter(buf),
        }
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

    pub fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn index(&self) -> usize {
        self.index
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(iter.collect::<Vec<usize>>(), vec![0; 0]);

        let iter = PieceLenIter::new(8, 8);
        assert_eq!(iter.collect::<Vec<usize>>(), vec![8]);
    }
}
