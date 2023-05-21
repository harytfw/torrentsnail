use crate::Result;
use axum::headers::{self, Header};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{body::Body, http::Request};
use base64::{engine::general_purpose, Engine as _};
use futures::future::BoxFuture;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub struct AuthLayer {
    username: String,
    password: String,
}

impl AuthLayer {
    pub fn with_username_password(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl<S> tower::Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            username: self.username.clone(),
            password: self.password.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    username: String,
    password: String,
    inner: S,
}

impl<S> tower::Service<Request<Body>> for AuthMiddleware<S>
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
        let auth_value = request
            .headers()
            .get("Authorization")
            .and_then(|h| h.to_str().ok());

        let basic_auth = auth_value.and_then(|v| parse_basic_authorization(v));

        let mut auth_exists = false;
        let mut auth_ok = false;

        if let Some(basic_auth) = basic_auth {
            auth_exists = true;
            if basic_auth.matches(&self.username, &self.password) {
                auth_ok = true;
            }
        }

        if !auth_exists {
            return Box::pin(async move {
                let response = (StatusCode::UNAUTHORIZED, "missing authorization").into_response();
                return Ok(response);
            });
        }

        if !auth_ok {
            return Box::pin(async move {
                let response = (
                    StatusCode::UNAUTHORIZED,
                    "bad username or password".to_string(),
                )
                    .into_response();
                return Ok(response);
            });
        }

        let future = self.inner.call(request);

        return Box::pin(async move {
            let response: Response = future.await?;
            Ok(response)
        });
    }
}

#[derive(Debug, Clone, PartialEq)]
struct BasicAuthorization {
    username: String,
    password: String,
}

impl BasicAuthorization {
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    pub fn matches(&self, username: &str, password: &str) -> bool {
        self.username == username && self.password == password
    }
}

fn parse_basic_authorization(value: &str) -> Option<BasicAuthorization> {
    let mut parts = value.splitn(2, ' ');
    if parts.next()? != "Basic" {
        return None;
    }

    let base64_value = parts.next()?;
    let decoded = general_purpose::STANDARD
        .decode(base64_value.as_bytes())
        .ok()?;

    let username_password = String::from_utf8_lossy(&decoded);
    let mut username_password_parts = username_password.splitn(2, ':');

    let username = username_password_parts.next()?;
    let password = username_password_parts.next()?;

    return Some(BasicAuthorization::new(username, password));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_authorization() {
        assert_eq!(
            parse_basic_authorization("Basic YWxhZGRpbjpvcGVuc2VzYW1l"),
            Some(BasicAuthorization {
                username: "aladdin".to_string(),
                password: "opensesame".to_string(),
            })
        );
    }
}
