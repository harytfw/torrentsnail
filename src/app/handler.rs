use crate::app::middleware;
use crate::app::Application;
use crate::torrent::HashId;
use crate::Error;
use axum::http::{HeaderName, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::{extract::Path, extract::State, Router};
use futures::future::join_all;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tracing::error;
use tracing::log::info;

use crate::app::models::CreateTorrentSessionRequest;
use crate::app::models::{ErrorResponse, Pagination, Pong, TorrentSessionInfo};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};

use super::models::AddSessionPeerRequest;
use super::models::PeerInfo;
use super::models::UpdateTorrentSessionRequest;

// A `MakeRequestId` that increments an atomic counter
#[derive(Clone, Default)]
struct MyMakeRequestId {
    counter: Arc<AtomicU64>,
}

impl MakeRequestId for MyMakeRequestId {
    fn make_request_id<B>(&mut self, request: &Request<B>) -> Option<RequestId> {
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
                    "/torrents/sessions/:info_hash/peers",
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

    info!("listen http server with addr: 0.0.0.0:3000");

    let t = axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
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
    return d;
}

async fn handler_ping(State(app): State<Arc<Application>>) -> Json<Pong> {
    Json(Pong::new())
}

async fn list_torrent_sessions(State(app): State<Arc<Application>>) -> Response {
    let session = app.sessions();
    let session_info_list: Vec<TorrentSessionInfo> = session
        .iter()
        .map(|s| TorrentSessionInfo::from_session(&s))
        .collect();
    Json(Pagination::new(session_info_list, None)).into_response()
}

async fn create_torrent_session(
    State(app): State<Arc<Application>>,
    req: Json<CreateTorrentSessionRequest>,
) -> Response {
    let builder = match app.builder().with_uri(&req.uri) {
        Ok(builder) => builder,
        Err(e) => {
            return Json(ErrorResponse::from(e)).into_response();
        }
    };
    match builder.build().await {
        Ok(session) => {
            return Json(TorrentSessionInfo::from_session(&session)).into_response();
        }
        Err(e) => {
            return Json(ErrorResponse::from(e)).into_response();
        }
    };
}

async fn update_torrent_session(
    State(app): State<Arc<Application>>,
    req: Json<UpdateTorrentSessionRequest>,
) -> Response {
    unimplemented!()
}

async fn list_session_peers(
    State(app): State<Arc<Application>>,
    Path(info_hash): Path<String>,
) -> Response {
    let id = HashId::from_hex(&info_hash).unwrap();
    app.get_session(&id)
        .map(|session| {
            Json(Pagination::new(
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
                Json(ErrorResponse::from(Error::Generic(
                    "session not found".to_string(),
                ))),
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
    let session = match app.get_session(&id) {
        Some(session) => session,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse::from(Error::Generic(
                    "session not found".to_string(),
                ))),
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
mod tests {
    use super::*;
}
