use crate::app::Application;

use askama_axum::IntoResponse;
use axum::body::BoxBody;
use axum::extract::Query;
use axum::extract::State;
use axum::http::status;
use axum::http::Response;
use axum::response::Redirect;
use std::sync::Arc;

use crate::app::ui::template;
use crate::model::TorrentSessionModel;

pub async fn index(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    let session = app.sessions();
    let sessions: Vec<TorrentSessionModel> = session
        .iter()
        .map(|s| TorrentSessionModel::from_session(&s))
        .collect();

    template::IndexTemplate { sessions }
}

pub async fn add_magnet_link(
    State(app): State<Arc<Application>>,
    magnet_link: Query<String>,
) -> Response<BoxBody> {
	
    let builder = match app.acquire_builder().with_uri(&magnet_link) {
        Err(err) => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                template::ErrorTemplate::new(err.to_string()),
            )
                .into_response()
        }
        Ok(builder) => builder,
    };

    match app.consume_builder(builder).await {
        Ok(_) => return Redirect::temporary(&app.ui_url_prefix).into_response(),
        Err(e) => {
            return (
                status::StatusCode::INTERNAL_SERVER_ERROR,
                template::ErrorTemplate::new(e.to_string()),
            )
                .into_response()
        }
    };
}

pub async fn add_torrent_file(State(app): State<Arc<Application>>) -> template::IndexTemplate {
    todo!()
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
