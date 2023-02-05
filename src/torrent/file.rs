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
    pub fn name(&mut self, name: &str) -> &mut Self {
        self.name = name.to_owned();
        self
    }

    pub fn set_pieces(&mut self, pieces: Vec<u8>) -> &mut Self {
        self.piece_length = pieces.len();
        self.pieces = pieces;
        self
    }

    pub fn get_file_meta(&self) -> Vec<FileMeta> {
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
    origin_val: Option<Box<bencode::Value>>,
}

impl TorrentFile {
    pub fn new(info: TorrentInfo) -> Self {
        Self {
            info,
            ..Default::default()
        }
    }

    pub fn set_origin_info_hash(&mut self, val: bencode::Value) -> &mut Self {
        self.origin_val = Some(val.into());
        self
    }

    pub fn info_hash(&self) -> Option<HashId> {
        self.origin_val
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

    use crate::bencode::{from_bytes, to_bytes, Value};

    #[test]
    fn parse_file() {
        let p = path::Path::new("demo.torrent");
        let data = fs::read(p).expect("read torrent");
        let v: TorrentFile = from_bytes(&data).expect("decode");
        let out = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open("demo.torrent.json")
            .unwrap();
        serde_json::to_writer_pretty(out, &v).unwrap();
    }

    #[test]
    fn info_hash_for_bencode() {
        let p = path::Path::new("./static/v1.b.torrent");
        let data = fs::read(p).expect("read torrent");
        let ben_val: Value = from_bytes(&data).expect("decode");
        let tf: TorrentFile = from_bytes(&data).expect("decode");
        let info = &ben_val["info"];
        let buf = to_bytes(info).unwrap();
        let info_hash = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &buf);

        println!("{:?}", tf.info_hash().unwrap());
        assert_eq!(
            tf.info_hash().unwrap().hex(),
            hex::encode(info_hash.as_ref())
        )
    }
}
