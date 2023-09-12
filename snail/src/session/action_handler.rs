use crate::session::utils::make_announce_key;
use crate::tracker::types::AnnounceResponse;
use crate::{
    session::{action::SessionAction, TorrentSession},
    tracker,
};
use tokio::sync::mpsc;
use tracing::{debug, error};

impl TorrentSession {
    pub fn handle_action(&self, action: SessionAction) {
        match action {
            SessionAction::AnnounceTracker(event) => {
                self.announce_tracker(event);
            }
            SessionAction::OnAnnounceResponse(resp) => {
                //
            }
            SessionAction::ConnectPeer(addr) => {
                self.connect_peer(addr);
            }
            SessionAction::PollPeer(info_hash) => {
                self.poll_peer(info_hash);
            }
            SessionAction::OnPeerRequest((info_hash, info)) => {
                self.peer_request(info_hash, info);
            }
        }
    }

    async fn on_announce_tracker_event(&self, event: tracker::Event) {
        let mut req = tracker::AnnounceRequest::new();
        req.set_key(make_announce_key(&self.my_id, &self.info_hash))
            .set_ip_address(&self.listen_addr)
            .set_info_hash(&self.info_hash)
            .set_peer_id(&self.my_id)
            .set_num_want(10)
            .set_event(event);
        debug!(req = ?req, "announce tracker");
        self.tracker.send_announce(&req, todo!()).await;
    }

    async fn on_announce_tracker_response(&self, resp: AnnounceResponse) {
        // Ok(result) => match result {
        //     Ok((url, rsp)) => {
        //         for peer in rsp.peers() {
        //             debug!(peer = ?peer, url = ?url, "new peer from tracker");
        //             self.dht.send_get_peers(&peer, &self.info_hash).await?;
        //         }

        //         // TODO:
        //     }
        //     Err(err) => {
        //         // TODO: ignore skip announce
        //         error!(?err, "tracker send announce");
        //     }
        // },
        // Err(mpsc::error::TryRecvError::Empty) => tokio::task::yield_now().await,

        // Err(mpsc::error::TryRecvError::Disconnected) => return,
    }
}
