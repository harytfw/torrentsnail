use crate::{
    message::PieceInfo,
    torrent::HashId,
    tracker::{self, types::AnnounceResponse},
};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum SessionEvent {
    AnnounceTrackerRequest(tracker::Event),
    AnnounceTrackerResponse(Box<AnnounceResponse>),
    ConnectPeer(Box<SocketAddr>),
    CheckPeer,
    PollPeer(Box<HashId>),
    OnPeerRequest(Box<(HashId, PieceInfo)>),
}

impl From<AnnounceResponse> for SessionEvent {
    fn from(resp: AnnounceResponse) -> Self {
        Self::AnnounceTrackerResponse(Box::new(resp))
    }
}
