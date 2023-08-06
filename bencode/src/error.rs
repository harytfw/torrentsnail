// use crate::tracker::{types::Action, TrackerError};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("validate failed: {0}")]
    Validate(String),
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
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("can not convert {0}")]
    TryFrom(String),
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
