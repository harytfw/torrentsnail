use std::collections::BTreeMap;
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

    pub fn get_piece_sha1(&self, index: usize) -> &[u8] {
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
            info,
            ..Default::default()
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

    pub fn set_origin_info(&mut self, val: bencode::Value) -> &mut Self {
        let dict = self
            .origin_content
            .get_or_insert_with(|| bencode::Value::Dictionary(BTreeMap::new()).into());
        match dict.as_mut() {
            bencode::Value::Dictionary(dict) => {
                dict.insert("info".into(), val);
            }
            _ => todo!("not dictionary"),
        }
        self
    }

    pub fn get_origin_content(&self) -> Option<&bencode::Value> {
        self.origin_content.as_deref()
    }

    pub fn get_origin_info(&self) -> Option<&bencode::Value> {
        self.origin_content
            .as_ref()
            .and_then(|v| v.dict_get("info"))
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
    use std::fs;

    use crate::bencode::from_bytes;

    #[test]
    fn parse_torrent() {
        struct TorrentTest {
            path: &'static str,
            info_hash: HashId,
        }

        impl TorrentTest {
            fn new(path: &'static str, info_hash: HashId) -> Self {
                Self { path, info_hash }
            }
        }

        for test_case in [
            TorrentTest::new(
                "tests/archlinux.torrent",
                HashId::from_hex("375ae3280cd80a8e9d7212e11dfaf7c45069dd35").unwrap(),
            ),
            TorrentTest::new(
                "tests/big-buck-bunny.torrent",
                HashId::from_hex("dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c").unwrap(),
            ),
            TorrentTest::new(
                "tests/sintel.torrent",
                HashId::from_hex("08ada5a7a6183aae1e09d831df6748d566095a10").unwrap(),
            ),
        ] {
            let data = fs::read(test_case.path).expect("read torrent");
            let tf: TorrentFile = from_bytes(&data).expect("decode");

            assert_eq!(tf.info_hash().unwrap(), test_case.info_hash);
        }
    }
}
