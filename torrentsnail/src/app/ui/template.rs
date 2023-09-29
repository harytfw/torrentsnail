use crate::model::TorrentSessionModel;
use askama::Template;

#[derive(Template)]
#[template(path = "index.html")]

pub struct IndexTemplate {
    pub sessions: Vec<TorrentSessionModel>,
}

#[derive(Template)]
#[template(path = "error.html")]

pub struct ErrorTemplate {
    pub message: String,
}

impl ErrorTemplate {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}
