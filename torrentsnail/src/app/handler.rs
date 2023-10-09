use crate::app::middleware;
use crate::app::Application;
use axum::http::Request;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::{extract::State, Router};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tower_http::request_id::{
    MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer,
};
use tracing::debug;
use tracing::error;

use crate::model::Pong;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};

use crate::app::api;
use crate::app::ui;

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
        .route("/ping", get(handler_ping))
        .nest(
            app.ui_url_prefix.as_str(),
            Router::new().route("/", get(ui::handler::index)).nest(
                "/action",
                Router::new()
                    .route("/add-magnet-link", post(ui::handler::add_magnet_link))
                    .route("/add-torrent-file", post(ui::handler::add_torrent_file))
                    .route("/add-peer-address", post(ui::handler::add_peer_address))
                    .route("/pause-session", post(ui::handler::pause_session))
                    .route("/resume-session", post(ui::handler::resume_session))
                    .route("/remove-session", post(ui::handler::remove_session)),
            ),
        )
        .nest(
            "/api/v1",
            Router::new()
                .route("/echo", post(api::handler::handler_echo))
                .route(
                    "/torrent/sessions",
                    get(api::handler::list_torrent_sessions)
                        .post(api::handler::create_torrent_session)
                        .patch(api::handler::update_torrent_session),
                )
                .route(
                    "/torrent/sessions/:info_hash",
                    get(api::handler::get_session),
                )
                .route(
                    "/torrent/sessions/:info_hash/peers",
                    get(api::handler::list_session_peers).post(api::handler::add_session_peer),
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

pub async fn handler_ping(State(_app): State<Arc<Application>>) -> Json<Pong> {
    Json(Pong::new())
}


#[cfg(test)]
mod tests {}
