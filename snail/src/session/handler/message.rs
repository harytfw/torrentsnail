use crate::message::PieceData;
use crate::{
    message::{
        BTExtMessage, BTMessage, LTDontHaveMessage, UTMetadataMessage, UTMetadataPieceData,
        MSG_LT_DONTHAVE, MSG_UT_METADATA,
    },
    session::{Peer, TorrentSession},
    torrent::{HashId, TorrentInfo},
};
use crate::{Error, Result};
use tracing::{debug, instrument, warn};

use crate::session::types::SessionEvent;

impl TorrentSession {
    #[instrument(skip_all, fields(peer_id=?peer.peer_id))]
    pub(crate) async fn handle_bt_message(&self, peer: &Peer, msg: &BTMessage) -> Result<()> {
        match msg {
            BTMessage::Choke => {
                let mut state = peer.state.write().await;
                state.choke = true;
            }
            BTMessage::Unchoke => {
                let mut state = peer.state.write().await;
                state.choke = false;
            }
            BTMessage::Interested => {
                let mut state = peer.state.write().await;
                state.interested = true
            }
            BTMessage::NotInterested => {
                let mut state = peer.state.write().await;
                state.interested = false
            }
            BTMessage::Request(info) => {
                self.event_tx
                    .send(SessionEvent::OnPeerRequest(Box::new((
                        peer.peer_id,
                        info.clone(),
                    ))))
                    .await?;
            }
            BTMessage::Piece(data) => {
                self.on_piece_arrived(peer, data).await?;
            }
            BTMessage::Have(index) => {
                let index = *index as usize;
                let mut state = peer.state.write().await;
                if state.owned_pieces.is_empty() {
                    let num = self.main_sm.piece_num().await;
                    state.owned_pieces = bit_vec::BitVec::from_elem(num, false)
                }
                if index < state.owned_pieces.len() {
                    state.owned_pieces.set(index, true);
                }
            }
            BTMessage::Cancel(_info) => {}
            BTMessage::BitField(bits) => {
                let mut state = peer.state.write().await;
                let mut bits = bit_vec::BitVec::from_bytes(bits);
                let piece_num = { self.main_sm.piece_num().await };

                bits.truncate(piece_num);

                if bits.len() == piece_num {
                    state.owned_pieces = bits;
                } else {
                    debug!(
                        "peer send wrong BitField, incoming bits len: {}, expected len: {}",
                        bits.len(),
                        piece_num
                    );
                    // FIXME: peer send wrong BitField messages, we should terminate connection
                }
            }
            BTMessage::Ping => {
                peer.send_message_now(BTMessage::Ping).await?;
            }
            BTMessage::Ext(ext_msg) => {
                self.handle_ext_msg(peer, ext_msg).await?;
            }
            BTMessage::Unknown(id) => {
                warn!(?id, "unknown msg id")
            }
        }
        Ok(())
    }

    async fn handle_ext_msg(&self, peer: &Peer, ext_msg: &BTExtMessage) -> Result<()> {
        let msg_name = peer
            .handshake
            .ext_handshake
            .as_ref()
            .and_then(|ext| ext.get_msg_name(ext_msg.id));
        if let Some(msg_name) = msg_name {
            match msg_name {
                MSG_UT_METADATA => {
                    let metadata_msg = UTMetadataMessage::from_bytes(&ext_msg.payload)?;
                    self.handle_ut_metadata_msg(peer, &metadata_msg).await?;
                }
                MSG_LT_DONTHAVE => {
                    let dont_have_msg = LTDontHaveMessage::from_bytes(&ext_msg.payload)?;
                    {
                        let mut state = peer.state.write().await;
                        state.owned_pieces.set(dont_have_msg.piece, false);
                    }
                }
                msg_name => {
                    debug!(?msg_name, "ignore unsupported ext msg");
                }
            }
        }
        Ok(())
    }

    async fn handle_ut_metadata_msg(&self, peer: &Peer, msg: &UTMetadataMessage) -> Result<()> {
        debug!(?msg, MSG_UT_METADATA);

        let mut torrent_metadata_buf: Option<Vec<u8>> = None;
        match msg {
            UTMetadataMessage::Request(index) => {
                let msg_id = self
                    .handshake_template
                    .ext_handshake
                    .as_ref()
                    .and_then(|ext| ext.get_msg_id(MSG_UT_METADATA))
                    .unwrap();

                let msg: UTMetadataMessage = {
                    let index = *index;
                    let total_size = self.aux_sm.total_size().await;

                    if self.aux_sm.is_checked(index).await {
                        let piece = self.aux_sm.read(index).await?;
                        let piece_data = UTMetadataPieceData {
                            piece: index,
                            total_size,
                            payload: piece.buf().to_vec(),
                        };
                        UTMetadataMessage::Data(piece_data)
                    } else {
                        UTMetadataMessage::Reject(index)
                    }
                };

                peer.send_message_now((msg_id, msg)).await?;
            }

            UTMetadataMessage::Reject(index) => self.aux_am.on_reject(*index, &peer.peer_id).await,

            UTMetadataMessage::Data(piece_data) => {
                let piece = {
                    self.aux_am
                        .on_piece_data(piece_data.piece, 0, &piece_data.payload, &peer.peer_id)
                        .await
                        .unwrap()
                };
                let index = piece.index();
                // TODO: handle error
                self.aux_sm.write(piece).await?;
                let checked = self.aux_sm.verify_checksum(index).await?;
                let snapshot = self.aux_sm.snapshot().await;

                debug!(index, checked);
                if self.aux_sm.all_checked().await {
                    {
                        self.aux_am.sync(&snapshot).await?;
                    }
                    let mut buf = Vec::with_capacity(self.aux_sm.total_size().await);
                    for i in 0..self.aux_sm.piece_num().await {
                        let piece = self.aux_sm.fetch(i).await?;
                        buf.extend(piece.buf())
                    }
                    torrent_metadata_buf = Some(buf);
                }
            }
        }

        if let Some(metadata_buf) = torrent_metadata_buf {
            // FIXME: support sha256
            let computed_info_hash = {
                HashId::try_from(
                    ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, &metadata_buf)
                        .as_ref(),
                )
                .unwrap()
            };

            if self.info_hash != computed_info_hash {
                debug!("info hash not match");
                self.aux_sm.clear_all_checked_bits().await?;
                return Ok(());
            }

            let info: TorrentInfo = bencode::from_bytes(&metadata_buf)?;

            // TODO: add atomic bool to prevent session from using partial state

            let total_len = info.total_length();
            let piece_len = info.piece_length;
            {
                debug!(?total_len, ?piece_len, "construct pieces");
                self.main_sm
                    .reinit_from_torrent(self.data_dir.as_ref(), &info)
                    .await?;

                let snapshot = self.main_sm.snapshot().await;
                self.main_am.sync(&snapshot).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn on_piece_arrived(
        &self,
        peer: &Peer,
        data: &PieceData,
    ) -> Result<(), Error> {
        let index = data.index;
        let _begin = data.begin;

        let mut all_checked = false;

        let complete_piece = {
            self.main_am
                .on_piece_data(data.index, data.begin, &data.fragment, &peer.peer_id)
                .await
        };

        if let Some(piece) = complete_piece {
            self.main_sm.write(piece).await?;
            let checked = self.main_sm.verify_checksum(index).await?;
            all_checked = self.main_sm.all_checked().await;
            if checked {
                peer.send_message_now(BTMessage::Have(index as u32)).await?;
            }
            if all_checked {
                self.main_sm.flush().await?;
            }
        }
        if all_checked {
            self.stop().await?;
        }
        Ok(())
    }
}
