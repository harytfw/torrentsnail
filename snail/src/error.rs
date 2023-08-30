// use crate::tracker::{types::Action, TrackerError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("url: {0}")]
    Url(#[from] url::ParseError),
    #[error("{0}")]
    SendError(String),
    #[error("{0}")]
    RecvError(String),
    // #[error("unknown action: {0:?}")]
    // UnexpectedAction(Action),
    #[error("bad event value")]
    BadEventValue,
    #[error("validate failed: {0}")]
    Validate(String),
    #[error("generic: {0}")]
    Generic(String),
    #[error("cancel")]
    Cancel,
    // #[error("tracker error: {0}")]
    // Tracker(Box<TrackerError>),
    #[error("no address")]
    NoAddress,
    #[error("timeout")]
    Timeout,
    #[error("skip announce")]
    SkipAnnounce,
    #[error("validation")]
    Validation,
    #[error("invalid torrent")]
    InvalidTorrent,
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("bytes to hash id")]
    BytesToHashId,
    #[error("{0}")]
    Deserialize(String),
    #[error("{0}")]
    Serialize(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("can not convert {0}")]
    TryFrom(String),
    #[error("unknown message type: {0}")]
    UnknownMessageType(u8),
    #[error("handshake: {0}")]
    Handshake(String),
    #[error("piece {0} not found")]
    PieceNotFound(usize),
    #[error("bad checksum size: {0}")]
    BadChecksumSize(usize),
    #[error("magnet: {0}")]
    Magnet(String),
    #[error("socks5: {0}")]
    Socks5(String),
    #[error("bencode: {0}")]
    Bencode(#[from] bencode::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Error::Serialize(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Error::Deserialize(msg.to_string())
    }
}

use tokio::sync::mpsc::error::SendError;

impl<T> From<SendError<T>> for Error {
    fn from(e: SendError<T>) -> Self {
        Self::SendError(e.to_string())
    }
}