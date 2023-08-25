use crate::Result;
use crate::{
    message::{
        BTExtMessage, BTMessage, LTDontHaveMessage, UTMetadataMessage, UTMetadataPieceData,
        MSG_LT_DONTHAVE, MSG_UT_METADATA,
    },
    session::{storage::StorageManager, Peer, TorrentSession},
    torrent::{HashId, TorrentFile, TorrentInfo},
};
use tracing::{debug, instrument, warn};

impl TorrentSession {
    #[instrument(skip_all, fields(peer_id=?peer.peer_id))]
    pub(crate) async fn handle_message(&self, peer: &Peer, msg: &BTMessage) -> Result<()> {
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
                self.peer_piece_req_tx
                    .send((peer.clone(), info.clone()))
                    .await?;
            }
            BTMessage::Piece(data) => {
                self.on_piece_arrived(peer, data).await?;
            }
            BTMessage::Have(index) => {
                let index = *index as usize;
                let mut state = peer.state.write().await;
                if state.owned_pieces.is_empty() {
                    let num = self.sm.piece_num().await;
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
                let piece_num = { self.sm.piece_num().await };

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
                    let total_size = self.metadata_sm.total_size().await;

                    if self.metadata_sm.is_checked(index).await {
                        let piece = self.metadata_sm.read(index).await?;
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

            UTMetadataMessage::Reject(index) => {
                let mut plm = self.metadata_piece_activity_man.write().await;
                plm.on_reject(*index, &peer.peer_id)
            }

            UTMetadataMessage::Data(piece_data) => {
                let piece = {
                    let mut log_man = self.metadata_piece_activity_man.write().await;
                    log_man
                        .on_piece_data(piece_data.piece, 0, &piece_data.payload, &peer.peer_id)
                        .unwrap()
                };
                let index = piece.index();
                let sha1 = piece.sha1();
                // TODO: handle error
                self.metadata_sm.write(piece).await?;
                let checked = self.metadata_sm.check(index, &sha1).await?;
                debug!(index, checked);
                if self.metadata_sm.all_checked().await {
                    {
                        let mut log_man = self.metadata_piece_activity_man.write().await;
                        log_man.sync(&self.metadata_sm).await?;
                    }
                    let mut buf = Vec::with_capacity(self.metadata_sm.total_size().await);
                    for i in 0..self.metadata_sm.piece_num().await {
                        let piece = self.metadata_sm.fetch(i).await?;
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
                self.metadata_sm.clear_all_checked_bits().await?;
                return Ok(());
            }

            let val: bencode::Value = bencode::from_bytes(&metadata_buf)?;
            let info: TorrentInfo = bencode::from_bytes(&metadata_buf)?;

            // TODO: add atomic bool to prevent session from using partial state

            let total_len = info.total_length();
            let piece_len = info.piece_length;
            {
                debug!(?total_len, ?piece_len, "construct pieces");
                self.sm
                    .reinit_from_torrent(self.storage_dir.as_ref(), &info).await?;

                self.piece_activity_man.sync(&self.sm).await?;
            }
            {
                debug!("save torrent info");
                let mut torrent = TorrentFile::from_info(info.clone());
                torrent.set_origin_info(val);
                let mut torrent_lock = self.torrent.write().await;
                *torrent_lock = Some(torrent)
            }
        }
        Ok(())
    }
}
