use crate::app::models::AddTorrentReq;
use crate::app::Application;
use crate::Result;
use axum::headers::{self, Header};
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum::{
    body::{Body, Bytes},
    extract::{Path, State, TypedHeader},
    http::{Request, StatusCode},
    routing::post,
    Router,
};
use futures::future::BoxFuture;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::error;
use tracing::log::info;

use super::models::{AddTorrentResp, Ping, Pong};

pub async fn start_server(app: Arc<Application>) {
    let cancel_clone = app.cancel.clone();
    // build our application with a single route

    let router = Router::new()
        .route("/api/v1/:method", post(handler))
        .layer(TraceIdLayer)
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

pub static TRACE_ID_HEADER: headers::HeaderName = headers::HeaderName::from_static("x-trace-id");

pub struct TraceId(u64);

impl TraceId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl headers::Header for TraceId {
    fn name() -> &'static headers::HeaderName {
        &TRACE_ID_HEADER
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i headers::HeaderValue>,
    {
        let value = values.next().ok_or_else(headers::Error::invalid)?;
        let id = value
            .to_str()
            .or_else(|_| Err(headers::Error::invalid()))?
            .parse::<u64>()
            .or_else(|_| Err(headers::Error::invalid()))?;
        Ok(TraceId::new(id))
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<headers::HeaderValue>,
    {
        let value = headers::HeaderValue::from_str(self.0.to_string().as_str())
            .expect("invalid header value");

        values.extend(std::iter::once(value));
    }
}

#[derive(Clone, Debug)]
pub struct TraceIdLayer;

impl<S> tower::Layer<S> for TraceIdLayer {
    type Service = TraceIdMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceIdMiddleware { inner }
    }
}

#[derive(Clone)]
pub struct TraceIdMiddleware<S> {
    inner: S,
}

impl<S> tower::Service<Request<Body>> for TraceIdMiddleware<S>
where
    S: tower::Service<Request<Body>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // `BoxFuture` is a type alias for `Pin<Box<dyn Future + Send + 'a>>`
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let trace_id = request.headers().get(TraceId::name()).cloned();
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response: Response = future.await?;
            if let Some(trace_id) = trace_id {
                response.headers_mut().insert(TraceId::name(), trace_id);
            }
            Ok(response)
        })
    }
}

macro_rules! dispatch_method {
    ($app: ident, $body: ident,  $method: ident, $req_type: ty) => {
        match serde_json::from_slice::<$req_type>($body.as_ref()) {
            Ok(req) => {
                let result = $method($app, &req).await;
                let response = match result {
                    Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
                    Err(e) => (
                        StatusCode::OK,
                        [(
                            headers::HeaderName::from_static("x-error"),
                            headers::HeaderValue::from_static("1"),
                        )],
                        Json(e.to_string()),
                    )
                        .into_response(),
                };
                response
            }
            Err(e) => (
                StatusCode::OK,
                [(
                    headers::HeaderName::from_static("x-error"),
                    headers::HeaderValue::from_static("1"),
                )],
                Json(e.to_string()),
            )
                .into_response(),
        }
    };
}

pub async fn handler(
    State(app): State<Arc<Application>>,
    method: Path<String>,
    body: Bytes,
) -> Response {
    match method.as_str() {
        "ping" => {
            return dispatch_method!(app, body, ping, Ping);
        }
        "add_torrent" => {
            return dispatch_method!(app, body, add_torrent, AddTorrentReq);
        }
        _ => (StatusCode::OK, Json(())).into_response(),
    }
}

pub async fn ping(app: Arc<Application>, req: &Ping) -> Result<Pong> {
    Ok(Pong::from(req))
}

pub async fn add_torrent(app: Arc<Application>, req: &AddTorrentReq) -> Result<AddTorrentResp> {
    // let resp = app.add_torrent(req).await?;
    // Ok(resp)
    Ok(AddTorrentResp {})
}

#[cfg(test)]
mod tests {
    use super::*;
}
