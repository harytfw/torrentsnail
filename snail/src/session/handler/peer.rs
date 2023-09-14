use std::collections::HashSet;
use std::net::SocketAddr;

use tracing::{debug, error, info, instrument};

use crate::message::{PieceInfo, UTMetadataMessage, MSG_UT_METADATA};
use crate::session::types::SessionEvent;
use crate::session::utils::{compute_torrent_path, METADATA_PIECE_SIZE};
use crate::{message::BTMessage, torrent::HashId};
use crate::{Error, Result};

use crate::session::{Peer, TorrentSession};

impl TorrentSession {
    fn support_ut_metadata(&self, peer_id: &HashId) -> bool {
        let peer = match self.peers.get(peer_id) {
            Some(peer) => peer,
            None => return false,
        };
        match (
            &self.handshake_template.ext_handshake,
            &peer.handshake.ext_handshake,
        ) {
            (Some(self_ext), Some(peer_ext)) => {
                self_ext.get_m().contains_key(MSG_UT_METADATA)
                    && peer_ext.get_msg_id(MSG_UT_METADATA).is_some()
            }
            (_, _) => false,
        }
    }

    async fn is_missing_torrent_file(&self) -> bool {
        let size = self.aux_sm.total_size().await;
        size == 0
    }

    async fn handle_req_metadata(&self, peer_id: &HashId) -> Result<()> {
        // bep-009
        let peer = self
            .peers
            .get(peer_id)
            .ok_or(Error::Generic("peer not found".to_owned()))?;

        if self.aux_sm.total_size().await == 0 {
            if let Some(total_len) = peer
                .handshake
                .ext_handshake
                .as_ref()
                .and_then(|ext| ext.get_metadata_size())
            {
                debug!(?total_len, "create torrent metadata piece manager");
                self.aux_sm
                    .reinit_from_file(
                        compute_torrent_path(&self.data_dir, &self.info_hash),
                        total_len,
                        METADATA_PIECE_SIZE,
                    )
                    .await?;
            }
        }

        let snapshot = self.aux_sm.snapshot().await;

        if snapshot.all_checked() {
            return Ok(());
        }

        let piece_reqs = {
            self.aux_am.sync(&snapshot).await?;
            self.aux_am.make_piece_request(&peer.peer_id).await
        };

        let msg_id = peer
            .handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
            .unwrap();

        for req in piece_reqs {
            debug!(?req);
            let req = UTMetadataMessage::Request(req.index);
            peer.send_message((msg_id, req)).await?;
        }
        peer.flush().await?;

        Ok(())
    }

    #[instrument(skip_all, fields(peer_id=?peer_id))]
    pub async fn handle_peer(&self, peer_id: &HashId) -> Result<()> {
        if self.support_ut_metadata(peer_id) && self.is_missing_torrent_file().await {
            self.handle_req_metadata(peer_id).await?;
            return Ok(());
        }

        // FIXME: handle not found
        let peer = self.peers.iter().find(|p| &p.peer_id == peer_id).unwrap();

        if !self.check_peer_state(&peer).await {
            return Ok(());
        }

        let state = { peer.state.read().await.clone() };

        self.main_am
            .sync_peer_pieces(&peer.peer_id, &state.owned_pieces)
            .await
            .unwrap();

        let pending_piece_info = self.main_am.make_piece_request(&peer.peer_id).await;

        if state.choke {
            if !pending_piece_info.is_empty() {
                peer.send_message_now(BTMessage::Interested).await?;
            }
            return Ok(());
        }

        if pending_piece_info.is_empty() {
            return Ok(());
        }

        peer.send_message(BTMessage::Interested).await?;
        for piece_info in pending_piece_info {
            peer.send_message(BTMessage::Request(piece_info)).await?;
        }
        peer.flush().await?;

        Ok(())
    }

    pub async fn check_peer_state(&self, peer: &Peer) -> bool {
        let mut is_broken = false;

        if let Some(broken_reason) = peer.state.read().await.broken.as_ref() {
            info!(?broken_reason, ?peer.peer_id, "peer broken");
            peer.shutdown().await.unwrap();
            is_broken = true;
        }

        match peer.poll_message().await {
            Ok(Some(msg)) => match self.handle_bt_message(&peer, &msg).await {
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

        if is_broken {
            self.peers.remove(&peer.peer_id);
            return false;
        }
        return true;
    }

    pub async fn search_peer_and_connect(&mut self) -> Result<()> {
        let exists_peers = {
            let m: HashSet<SocketAddr> = self.peers.iter().map(|peer| peer.addr).collect();
            m
        };
        let peers = self.dht.get_peers(&self.info_hash).await;

        'inner: for addr in peers {
            if exists_peers.contains(&addr) {
                continue 'inner;
            }
            self.event_tx
                .send(SessionEvent::ConnectPeer(Box::new(addr)))
                .await
                .unwrap();
        }
        Ok(())
    }
}
