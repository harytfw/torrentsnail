use tracing::debug;

use crate::bt::piece::Piece;
use crate::torrent::TorrentInfo;
use crate::{Error, Result};
use std::borrow::Borrow;
use std::cmp;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fs::{create_dir_all, File};
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct Fragment {
    piece_index: usize,
    piece_offset: usize,
    file_offset: usize,
    len: usize,
}

impl Fragment {
    pub fn new(piece_index: usize, piece_offset: usize, file_offset: usize, len: usize) -> Self {
        Self {
            piece_index,
            piece_offset,
            len,
            file_offset,
        }
    }

    pub fn cut(self, at: usize) -> (Fragment, Fragment) {
        let left = Fragment::new(self.piece_index, self.piece_offset, self.file_offset, at);

        let right = Fragment::new(
            self.piece_index,
            self.piece_offset + at,
            self.file_offset + at,
            self.len - at,
        );

        assert_eq!(self.len, left.len + right.len);

        (left, right)
    }
}

impl<T: Borrow<Piece>> From<T> for Fragment {
    fn from(piece: T) -> Self {
        let piece = piece.borrow();
        Fragment::new(piece.get_index(), 0, 0, piece.size())
    }
}

pub struct FilePieceMap {
    fragments: Vec<Fragment>,
    path: PathBuf,
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
    pub fn new(path: impl AsRef<Path>, fragments: Vec<Fragment>) -> Self {
        assert!(!fragments.is_empty());
        Self {
            path: path.as_ref().to_owned(),
            fragments,
        }
    }

    pub fn support_piece(&self, piece: &Piece) -> bool {
        piece.get_index() >= self.fragments.first().unwrap().piece_index
            && piece.get_index() <= self.fragments.last().unwrap().piece_index
    }

    pub fn accept(&self, pieces: &[&Piece]) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            create_dir_all(dir)?;
        }

        let f = File::options()
            .create(true)
            .write(true)
            .open(self.path.as_path())?;

        let mut write_cnt = 0usize;

        for piece in pieces {
            if let Some(frag) = self
                .fragments
                .iter()
                .find(|f| f.piece_index == piece.get_index())
            {
                let data = &piece.get_buf()[frag.piece_offset..frag.piece_offset + frag.len];
                debug!(?frag, "accept piece");
                f.write_at(data, frag.file_offset as u64)?;
                write_cnt += 1;
            }
        }

        if write_cnt != pieces.len() {
            Err(Error::Generic("some file fragments can not accept".into()))
        } else {
            Ok(())
        }
    }

    pub fn fill(&mut self, pieces: &mut [Piece]) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            create_dir_all(dir)?;
        }

        let f = File::options().read(true).open(self.path.as_path())?;

        let mut write_cnt = 0usize;

        for piece in pieces.iter_mut() {
            if let Some(frag) = self
                .fragments
                .iter()
                .find(|f| f.piece_index == piece.get_index())
            {
                let buf = &mut piece.get_buf_mut()[frag.piece_offset..frag.piece_offset + frag.len];
                debug!(?frag, "fill piece");
                f.read_exact_at(buf, frag.file_offset as u64)?;
                write_cnt += 1
            }
        }

        if write_cnt != pieces.len() {
            Err(Error::Generic("some file fragments not found".into()))
        } else {
            Ok(())
        }
    }
}

pub fn build_file_piece_map(info: &TorrentInfo) -> Vec<FilePieceMap> {
    let base = PathBuf::from("/tmp/snail/").join(&info.name);

    let mut piece_frag_que: VecDeque<Fragment> = {
        let mut que = VecDeque::with_capacity(info.pieces_num());
        let mut len = info.total_length();
        while len > 0 {
            que.push_back(Fragment {
                piece_index: que.len(),
                piece_offset: 0,
                file_offset: 0,
                len: cmp::min(len, info.piece_length),
            });
            len -= cmp::min(len, info.piece_length);
        }
        que
    };

    let mut ret = vec![];
    for meta in info.get_files_meta() {
        let mut items: Vec<Fragment> = vec![];
        let mut file_frag = Fragment::new(0, 0, 0, meta.length);
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

        let full_path = meta.path.iter().fold(base.clone(), |acc, p| acc.join(p));
        ret.push(FilePieceMap::new(full_path, items))
    }
    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::TorrentFile;
    use glob::glob;

    #[test]
    fn test_build_file_map() {
        for path in glob("tests/**/*.torrent").unwrap() {
            let path = path.unwrap();
            let torrent = TorrentFile::from_path(path).unwrap();
            let file_maps = build_file_piece_map(&torrent.info);
        }
    }
}
