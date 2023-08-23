use crate::app::middleware;
use crate::app::Application;
use anyhow::{anyhow, Error};
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::{extract::Path, extract::State, Router};
use snail::torrent::HashId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tracing::debug;
use tracing::error;

use crate::app::models::CreateTorrentSessionRequest;
use crate::app::models::{CursorPagination, ErrorResponse, Pong, TorrentSessionModel};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};

use super::models::AddSessionPeerRequest;
use super::models::OkResponse;
use super::models::PeerInfo;
use super::models::UpdateTorrentSessionRequest;

// A `MakeRequestId` that increments an atomic counter
#[derive(Clone, Default)]
struct MyMakeRequestId {
    counter: Arc<AtomicU64>,
}

impl MakeRequestId for MyMakeRequestId {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let request_id = self
            .counter
            .fetch_add(1, Ordering::SeqCst)
            .to_string()
            .parse()
            .unwrap();

        Some(RequestId::new(request_id))
    }
}

pub async fn start_server(app: Arc<Application>) {
    let cancel_clone = app.cancel.clone();

    let router = Router::new()
        .route("/", get(|| async {}))
        .route("/ping", get(handler_ping))
        .nest(
            "/api/v1",
            Router::new()
                .route("/echo", post(handler_echo))
                .route(
                    "/torrent/sessions",
                    get(list_torrent_sessions)
                        .post(create_torrent_session)
                        .patch(update_torrent_session),
                )
                .route(
                    "/torrent/sessions/:info_hash",
                    get(get_session),
                )
                .route(
                    "/torrent/sessions/:info_hash/peers",
                    get(list_session_peers).post(add_session_peer),
                )
                .layer(SetRequestIdLayer::x_request_id(MyMakeRequestId::default()))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(middleware::auth::AuthLayer::with_username_password(
                    "torrentsnail",
                    "lianstnerrot",
                )),
        )
        .layer(
            TraceLayer::new_for_http()
                // FIXME: exclude authorization header from log
                .make_span_with(DefaultMakeSpan::new().include_headers(true))
                .on_response(DefaultOnResponse::new().include_headers(true)),
        )
        .with_state(Arc::clone(&app));

    let bind_addr = format!(
        "{}:{}",
        app.config.network.bind_interface, app.config.network.http_port
    );

    debug!(bind_addr, "listen http server");

    let t = axum::Server::bind(&bind_addr.parse().unwrap())
        .serve(router.into_make_service())
        .with_graceful_shutdown(async move { cancel_clone.cancelled().await });

    match t.await {
        Ok(()) => {}
        Err(e) => {
            error!(err = ?e, "http server")
        }
    };
}

async fn handler_echo(d: Json<String>) -> Json<String> {
    d
}

async fn handler_ping(State(_app): State<Arc<Application>>) -> Json<Pong> {
    Json(Pong::new())
}

async fn list_torrent_sessions(State(app): State<Arc<Application>>) -> Response {
    let session = app.sessions();
    let session_info_list: Vec<TorrentSessionModel> = session
        .iter()
        .map(|s| TorrentSessionModel::from_session(&s))
        .collect();
    Json(CursorPagination::new(session_info_list, None)).into_response()
}

async fn create_torrent_session(
    State(app): State<Arc<Application>>,
    req: Json<CreateTorrentSessionRequest>,
) -> Response {
    let mut info_hash_vec:Vec<String> = vec![];

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

async fn get_session(
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

async fn update_torrent_session(
    State(_app): State<Arc<Application>>,
    _req: Json<UpdateTorrentSessionRequest>,
) -> Response {
    unimplemented!()
}

async fn list_session_peers(
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
async fn add_session_peer(
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
#[cfg(test)]
mod tests {}
