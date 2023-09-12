use crate::{
    message::PieceInfo,
    torrent::HashId,
    tracker::{self, types::AnnounceResponse},
};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum SessionAction {
    AnnounceTracker(tracker::Event),
    OnAnnounceResponse(Box<AnnounceResponse>),
    ConnectPeer(Box<SocketAddr>),
    PollPeer(Box<HashId>),
    OnPeerRequest(Box<(HashId, PieceInfo)>),
}

impl From<AnnounceResponse> for SessionAction {
    fn from(resp: AnnounceResponse) -> Self {
        Self::OnAnnounceResponse(Box::new(resp))
    }
}
