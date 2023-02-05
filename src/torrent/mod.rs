mod file;
mod hash;
use crate::bencode;
use crate::{Error, Result};
pub use file::{FileMeta, TorrentFile, TorrentInfo};
pub use hash::HashId;

pub fn torrent_info_hash(torrent: &[u8]) -> Result<HashId> {
    let v: bencode::Value = bencode::from_bytes(torrent).expect("decode");
    match v {
        bencode::Value::Dictionary(d) => {
            let info = d.get("info").ok_or_else(|| Error::InvalidTorrent)?;
            let buf = bencode::to_bytes(info)?;
            let data = ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &buf);
            let data: [u8; 20] = data.as_ref().try_into().map_err(|_| Error::BytesToHashId)?;
            Ok(data.into())
        }
        _ => Err(Error::InvalidTorrent),
    }
}

pub fn parse_torrent(buf: impl AsRef<[u8]>) -> Result<TorrentFile> {
    let buf = buf.as_ref();
    let origin: bencode::Value = bencode::from_bytes(buf)?;
    let mut parsed: TorrentFile = bencode::from_bytes(buf)?;
    parsed.set_origin_info_hash(origin);
    Ok(parsed)
}
