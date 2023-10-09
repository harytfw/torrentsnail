use crate::app::Application;

use askama_axum::IntoResponse;
use axum::body::BoxBody;
use axum::extract::Query;
use axum::extract::State;
use axum::http::status;
use axum::http::Response;
use axum::response::Redirect;
use axum::Form;
use serde::Deserialize;
use std::sync::Arc;
use tracing::debug;
use tracing::error;

use crate::app::ui::template;
use crate::model::TorrentSessionModel;

pub async fn index(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    let session = app.sessions();
    let sessions: Vec<TorrentSessionModel> = session
        .iter()
        .map(|s| TorrentSessionModel::from_session(&s))
        .collect();

    template::IndexTemplate { sessions: sessions }
}

#[derive(Deserialize)]
pub struct AddMagnetLinkForm {
    magnet_link: String,
}

pub async fn add_magnet_link(
    State(app): State<Arc<Application>>,
    form: Form<AddMagnetLinkForm>,
) -> Response<BoxBody> {
    let magnet_link = form.magnet_link.as_str();
    debug!(?magnet_link, "add magnet link");

    let builder = match app.acquire_builder().with_uri(&magnet_link) {
        Err(err) => {
            return (
                status::StatusCode::SEE_OTHER,
                template::ErrorTemplate::new(err.to_string()),
            )
                .into_response()
        }
        Ok(builder) => builder,
    };

    debug!(?magnet_link, "consume builder");
    match app.consume_builder(builder).await {
        Ok(_) => return Redirect::to(&app.ui_url_prefix).into_response(),
        Err(e) => {
            return (
                status::StatusCode::SEE_OTHER,
                template::ErrorTemplate::new(e.to_string()),
            )
                .into_response()
        }
    };
}

pub async fn add_torrent_file(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    todo!()
}

pub async fn add_peer_address(
    State(app): State<Arc<Application>>,
    peer_address: Form<String>,
) -> Response<BoxBody> {
    let peer_addr = peer_address.0.as_str();

    for session in app.sessions().iter() {
        match session.add_peer_with_addr(peer_addr).await {
            Ok(_) => {}
            Err(e) => {
                error!(err = ?e, "failed to add peer")
            }
        };
    }

    return Redirect::to(&app.ui_url_prefix).into_response();
}

pub async fn pause_session(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    todo!()
}

pub async fn resume_session(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    todo!()
}

pub async fn remove_session(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    todo!()
}
