use crate::message::{BTMessage, LTDontHaveMessage, PieceData, PieceInfo, MSG_LT_DONTHAVE};
use crate::proxy::socks5::Socks5Client;
use crate::session::utils::make_announce_key;
use crate::torrent::HashId;
use crate::tracker::types::AnnounceResponse;
use crate::{
    session::{types::SessionEvent, TorrentSession},
    tracker,
};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument};

mod handshake;
mod message;
mod peer;

impl TorrentSession {
    pub async fn handle_event(&self, action: SessionEvent) {
        match action {
            SessionEvent::AnnounceTrackerRequest(event) => {
                self.on_event_announce_tracker_request(event).await;
            }
            SessionEvent::AnnounceTrackerResponse(resp) => {
                self.on_event_announce_tracker_response(&resp).await;
            }
            SessionEvent::ConnectPeer(addr) => {
                self.on_event_connect_peer_addr(&addr).await;
            }
            SessionEvent::PollPeer(peer_id) => {
                self.on_event_poll_peer(&peer_id).await;
            }
            SessionEvent::OnPeerRequest(ref r) => {
                let peer_id = &r.0;
                let info = &r.1;
                self.on_event_peer_request(&peer_id, &info).await;
            }
            SessionEvent::CheckPeer => {
                self.on_event_check_peer().await;
            }
        }
    }

    async fn on_event_check_peer(&self) {
        let candidate_peer_id = { self.peers.iter().map(|p| p.peer_id).collect::<Vec<_>>() };

        for peer_id in candidate_peer_id {
            self.event_tx
                .send(SessionEvent::PollPeer(Box::new(peer_id)))
                .await
                .unwrap();
        }
    }

    async fn on_event_poll_peer(&self, peer_id: &HashId) {
        debug!(?peer_id, "poll peer");
        if let Some(peer) = self.peers.get(&peer_id) {
            let state = peer.state.read().await;
            self.main_am
                .sync_peer_pieces(&peer.peer_id, &state.owned_pieces)
                .await
                .unwrap();
        }
        self.handle_peer(peer_id).await.unwrap();

        let mut is_broken = false;

        if let Some(peer) = self.peers.get(&peer_id) {
            if let Some(broken_reason) = peer.state.read().await.broken.as_ref() {
                info!(?broken_reason, ?peer_id, "peer broken");
                peer.shutdown().await;
                is_broken = true;
            }

            match peer.poll_message().await {
                Ok(Some(msg)) => match self.handle_message(&peer, &msg).await {
                    Ok(()) => {}
                    Err(e) => {
                        error!("handle message error: {:?}", e)
                    }
                },
                Ok(None) => {}
                Err(e) => {
                    error!("poll message error: {:?}", e);
                }
            }
        }
        if is_broken {
            self.peers.remove(&peer_id);
        }
    }

    async fn on_event_announce_tracker_request(&self, event: tracker::Event) {
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

    async fn on_event_announce_tracker_response(&self, resp: &AnnounceResponse) {
        for peer in resp.peers() {
            debug!(peer = ?peer, "new peer from tracker");
            // FIXME: handle error
            self.dht
                .send_get_peers(&peer, &self.info_hash)
                .await
                .unwrap();
        }
    }

    #[instrument(skip_all, fields(peer_id=?peer_id, info_hash=?self.info_hash))]
    async fn on_event_peer_request(&self, peer_id: &HashId, info: &PieceInfo) {
        let index = info.index;
        let peer = self.peers.get(peer_id);
        if let Some(peer) = peer {
            let msg = if self.main_sm.is_checked(index).await {
                let piece = self.main_sm.fetch(index).await.unwrap();
                let piece_data = PieceData::new(
                    index,
                    info.begin,
                    &piece.buf()[info.begin..info.begin + info.length],
                );
                Some(BTMessage::from(piece_data))
            } else if let Some(id) = peer.get_msg_id(MSG_LT_DONTHAVE) {
                let donthave = LTDontHaveMessage::new(info.index);
                Some(BTMessage::from((id, donthave)))
            } else {
                None
            };

            if let Some(msg) = msg {
                peer.send_message_now(msg).await.unwrap();
            }
        }
    }

    async fn on_event_connect_peer_addr(&self, addr: &SocketAddr) {
        let proxy_config = self.cfg.network.proxy.clone();
        let tcp: TcpStream;
        debug!("try active handshake");
        if let Some(proxy_config) = proxy_config.as_ref() {
            match proxy_config.r#type.as_str() {
                "socks5" => {
                    let proxy_client = Socks5Client::new(proxy_config.to_socks5_addr().unwrap());
                    tcp = proxy_client.connect(addr).await.unwrap();
                }
                _ => {
                    panic!("not support proxy type");
                }
            }
        } else {
            // TODO: handle io error
            tcp = TcpStream::connect(addr).await.unwrap();
        }
        match self.active_handshake_with_tcp(tcp).await {
            Ok(_peer) => {
                debug!("active handshake success");
            }
            Err(e) => {
                error!(err = ?e, "connect peer");
            }
        };
    }
}
