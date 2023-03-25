use crate::session::piece::{Piece, PieceLenIter};
use crate::Result;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::{cmp, fs};

#[derive(Debug, Clone)]
pub struct FileFragment {
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

    // put data to piece according to fragment info
    // data must be the same length as fragment
    pub fn apply(&mut self, data: &[u8], piece: &mut Piece) {
        assert_eq!(self.len, data.len());

        piece.buf_mut()[self.piece_offset..self.piece_offset + self.len].copy_from_slice(data);
    }

    pub fn piece_index(&self) -> usize {
        self.piece_index
    }
}

pub struct FilePieceMap {
    fragments: Vec<FileFragment>,
    // the file path
    path: PathBuf,
    // the size of complete file
    size: usize,
    // TODO: use rang to represent used indices
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

    pub fn prepare_write(&self, piece: &Piece) -> Vec<(FileFragment, Vec<u8>)> {
        let mut frags = vec![];

        if let Some(frag) = self
            .fragments
            .iter()
            .find(|f| f.piece_index == piece.index())
        {
            let b = piece.buf()[frag.piece_offset..frag.piece_offset + frag.len].to_vec();
            frags.push((frag.clone(), b));
        }
        frags
    }

    // write piece to file
    // FileFragment is mapping info about piece and file
    pub fn write(&self, req: (FileFragment, Vec<u8>)) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }

        let f = fs::File::options()
            .create(true)
            .write(true)
            .open(self.path.as_path())?;

        let (frag, buf) = req;
        f.write_at(&buf, frag.file_offset as u64)?;
        Ok(())
    }

    // read specific piece from file
    // after successful read, return the fragment info and data
    pub fn read(&self, piece_index: usize) -> Result<(FileFragment, Vec<u8>)> {
        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir)?;
        }

        let frag = self
            .fragments
            .iter()
            .find(|f| f.piece_index == piece_index)
            .expect("can not find fragment");

        if !self.path.try_exists()? {
            return Ok((frag.clone(), vec![0u8; frag.len]));
        }

        let f = fs::File::options().read(true).open(&self.path)?;

        let mut data = vec![0u8; frag.len];
        f.read_exact_at(&mut data, frag.file_offset as u64)?;
        Ok((frag.clone(), data))
    }

    pub fn contains_index(&self, index: usize) -> bool {
        self.piece_indices.contains(&index)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn piece_indices(&self) -> &HashSet<usize> {
        &self.piece_indices
    }
}

#[derive(Debug, Default)]
pub struct FilePieceMapBuilder {
    files: Vec<(usize, PathBuf)>,
    total_len: usize,
    piece_len: usize,
}

impl FilePieceMapBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn push_file(&mut self, len: usize, path: PathBuf) -> &mut Self {
        self.files.push((len, path));
        self
    }

    pub fn set_total_len(&mut self, total_len: usize) -> &mut Self {
        self.total_len = total_len;
        self
    }

    pub fn set_piece_len(&mut self, piece_len: usize) -> &mut Self {
        self.piece_len = piece_len;
        self
    }

    pub fn build(self) -> Result<Vec<FilePieceMap>> {
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
