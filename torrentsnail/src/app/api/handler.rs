use crate::app::Application;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum::{extract::Path, extract::State};
use snail::torrent::HashId;
use std::sync::Arc;
use tracing::debug;
use tracing::error;

use crate::model::AddSessionPeerRequest;
use crate::model::CreateTorrentSessionRequest;
use crate::model::OkResponse;
use crate::model::PeerInfo;
use crate::model::UpdateTorrentSessionRequest;
use crate::model::{CursorPagination, ErrorResponse, TorrentSessionModel};

pub async fn handler_echo(d: Json<String>) -> Json<String> {
    d
}

pub async fn list_torrent_sessions(State(app): State<Arc<Application>>) -> Response {
    let session = app.sessions();
    let session_info_list: Vec<TorrentSessionModel> = session
        .iter()
        .map(|s| TorrentSessionModel::from_session(&s))
        .collect();
    Json(CursorPagination::new(session_info_list, None)).into_response()
}

pub async fn create_torrent_session(
    State(app): State<Arc<Application>>,
    req: Json<CreateTorrentSessionRequest>,
) -> Response {
    let mut info_hash_vec: Vec<String> = vec![];

    for uri in req.uri.iter() {
        let builder = match app.acquire_builder().with_uri(&uri) {
            Ok(builder) => builder,
            Err(e) => {
                return Json(ErrorResponse::from(e)).into_response();
            }
        };

        match builder.info_hash() {
            Some(info_hash) => {
                info_hash_vec.push(info_hash.hex());
            }
            None => {
                error!(uri=?uri, "no info hash");
            }
        };

        let app = app.clone();
        tokio::spawn(async move {
            match app.consume_builder(builder).await {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        err = ?e,
                        "consume builder failed"
                    )
                }
            };
        });
    }

    Json(OkResponse::new(info_hash_vec)).into_response()
}

pub async fn get_session(
    State(app): State<Arc<Application>>,
    Path(info_hash): Path<String>,
) -> Response {
    debug!(info_hash=?info_hash, "get session");

    let id = HashId::from_hex(info_hash).unwrap();

    let session = match app.get_session_by_info_hash(&id) {
        Some(session) => session,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::from_str("session not found")),
            )
                .into_response();
        }
    };
    Json(OkResponse::new(TorrentSessionModel::from_session(&session))).into_response()
}

pub async fn update_torrent_session(
    State(_app): State<Arc<Application>>,
    _req: Json<UpdateTorrentSessionRequest>,
) -> Response {
    unimplemented!()
}

pub async fn list_session_peers(
    State(app): State<Arc<Application>>,
    Path(info_hash): Path<String>,
) -> Response {
    let id = HashId::from_hex(info_hash).unwrap();
    app.get_session_by_info_hash(&id)
        .map(|session| {
            Json(CursorPagination::new(
                session
                    .peers()
                    .iter()
                    .map(|p| PeerInfo::from_peer(&p))
                    .collect::<Vec<_>>(),
                None,
            ))
            .into_response()
        })
        .unwrap_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::from_str("session not found")),
            )
                .into_response()
        })
}
pub async fn add_session_peer(
    State(app): State<Arc<Application>>,
    Path(info_hash): Path<String>,
    req: Json<AddSessionPeerRequest>,
) -> Response {
    let id = HashId::from_hex(&info_hash).unwrap();
    let session = match app.get_session_by_info_hash(&id) {
        Some(session) => session,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::from_str("session not found")),
            )
                .into_response();
        }
    };
    for addr in req.parse_peers().unwrap_or_default() {
        {
            let session = session.clone();
            tokio::spawn(async move {
                match session.add_peer_with_addr(addr).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(err = ?e, "add peer");
                    }
                };
            });
        }
    }
    StatusCode::ACCEPTED.into_response()
}
