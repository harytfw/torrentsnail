use std::{fmt, str::FromStr};

use num::{FromPrimitive, ToPrimitive};
use serde::{Serialize, Deserialize, Serializer};

use crate::Error;


#[derive(Debug, Clone, Copy)]
pub enum TorrentSessionStatus {
    Stopped = 0,
    Started,
    Seeding,
}

impl FromPrimitive for TorrentSessionStatus {
    fn from_i64(num: i64) -> Option<Self> {
        Self::from_u64(num as u64)
    }

    fn from_u64(num: u64) -> Option<Self> {
        let s = match num {
            0 => Self::Started,
            1 => Self::Stopped,
            2 => Self::Seeding,
            _ => return None,
        };
        Some(s)
    }
}

impl ToPrimitive for TorrentSessionStatus {
    fn to_i64(&self) -> Option<i64> {
        Some(*self as i64)
    }

    fn to_u64(&self) -> Option<u64> {
        Some(*self as u64)
    }
}

impl fmt::Display for TorrentSessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Started => "started",
            Self::Stopped => "stopped",
            Self::Seeding => "seeding",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for TorrentSessionStatus {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "started" => Ok(Self::Started),
            "stopped" => Ok(Self::Stopped),
            "seeding" => Ok(Self::Seeding),
            _ => Err(Error::Generic(format!("invalid status: {}", s))),
        }
    }
}

impl Serialize for TorrentSessionStatus {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TorrentSessionStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}