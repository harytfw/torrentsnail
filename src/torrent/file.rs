use std::path::Path;

use crate::{bencode, Result};
use crate::{bencode::to_bytes, Error};
use serde::{Deserialize, Serialize};

use super::HashId;

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct FileMeta {
    #[serde(default)]
    pub length: usize,
    #[serde(default)]
    pub path: Vec<String>,
}

impl FileMeta {}

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct TorrentInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<FileMeta>>,

    #[serde(default)]
    pub name: String,

    #[serde(default, rename = "piece length")]
    pub piece_length: usize,

    #[serde(default, with = "serde_bytes")]
    pub pieces: Vec<u8>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<usize>,
}

impl std::fmt::Debug for TorrentInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TorrentInfo")
            .field("files", &self.files)
            .field("name", &self.name)
            .field("pieces_length", &self.piece_length)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

impl TorrentInfo {
    pub fn set_name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_owned();
        self
    }

    pub fn set_pieces(&mut self, pieces: Vec<u8>) -> &mut Self {
        self.piece_length = pieces.len();
        self.pieces = pieces;
        self
    }

    pub fn get_files_meta(&self) -> Vec<FileMeta> {
        match self.files.as_ref() {
            Some(files) => files.clone(),
            None => vec![FileMeta {
                length: self.length.unwrap_or(0),
                path: vec![self.name.clone()],
            }],
        }
    }

    pub fn get_pieces_sha1(&self, index: usize) -> &[u8] {
        &self.pieces[index * 20..(index + 1) * 20]
    }

    pub fn info_hash(&self) -> Result<HashId> {
        let buf = to_bytes(&self)?;
        let info_hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &buf);
        let hash = info_hash
            .as_ref()
            .try_into()
            .map_err(|_| Error::BytesToHashId)?;
        Ok(hash)
    }

    pub fn pieces_num(&self) -> usize {
        self.pieces.len() / 20
    }

    pub fn total_length(&self) -> usize {
        if let Some(len) = self.length {
            return len;
        }
        if let Some(files) = self.files.as_ref() {
            return files.iter().map(|f| f.length).sum();
        }
        0
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TorrentFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announce: Option<String>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "announce-list"
    )]
    pub announce_list: Option<Vec<Vec<String>>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "created by"
    )]
    pub created_by: Option<String>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "creation date"
    )]
    pub creation_date: Option<i64>,

    pub info: TorrentInfo,

    // bep-0017
    #[serde(default)]
    pub httpseeds: Vec<String>,

    #[serde(skip)]
    origin_content: Option<Box<bencode::Value>>,
}

impl TorrentFile {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn from_info(info: TorrentInfo) -> Self {
        Self {
            info,..Default::default()
        }
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let data = std::fs::read(path)?;
        let val: bencode::Value = bencode::from_bytes(&data)?; 
        let mut tf: Self = bencode::from_bytes(&data)?;
        tf.origin_content = Some(val.into());
        Ok(tf)
    }

    pub fn set_origin_content(&mut self, val: bencode::Value) -> &mut Self {
        self.origin_content = Some(val.into());
        self
    }

    pub fn info_hash(&self) -> Option<HashId> {
        self.origin_content
            .as_ref()
            .and_then(|val| val.as_dict())
            .and_then(|dict| dict.get("info"))
            .and_then(|val| bencode::to_bytes(val).ok())
            .and_then(|buf| {
                let result = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &buf);
                HashId::from_slice(result.as_ref()).ok()
            })
            .or_else(|| self.info.info_hash().ok())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{fs, path};

    use crate::bencode::{from_bytes};

    #[test]
    fn parse_torrent() {
        let p = path::Path::new("tests/archlinux.torrent");
        let data = fs::read(p).expect("read torrent");
        let tf: TorrentFile = from_bytes(&data).expect("decode");

        assert_eq!(
            tf.info_hash().unwrap().hex(),
            "375ae3280cd80a8e9d7212e11dfaf7c45069dd35"
        );

        assert_eq!(tf.info.get_files_meta()[0].path[0], "archlinux-2023.02.01-x86_64.iso")
    }
}
