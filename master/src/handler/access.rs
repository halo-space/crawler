use axum::extract::FromRequestParts;
use axum::http::{HeaderMap, header, request::Parts};

use crate::svc::Context;
use crate::{Config, Error};

pub(super) struct Worker;

impl FromRequestParts<Context> for Worker {
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, app: &Context) -> Result<Self, Self::Rejection> {
        authorize(&app.config, app.config.worker_token(), &parts.headers)?;
        Ok(Self)
    }
}

pub(super) struct Control;

impl FromRequestParts<Context> for Control {
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, app: &Context) -> Result<Self, Self::Rejection> {
        authorize(&app.config, app.config.control_token(), &parts.headers)?;
        Ok(Self)
    }
}

fn authorize(config: &Config, token: &str, headers: &HeaderMap) -> Result<(), Error> {
    let expected = format!("Bearer {token}");
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(expected.as_str())
    {
        return Err(Error::Unauthorized);
    }
    let namespace = headers
        .get("X-Crawler-Namespace")
        .and_then(|value| value.to_str().ok())
        .ok_or(Error::Unauthorized)?;
    if namespace != config.namespace() {
        return Err(Error::Unauthorized);
    }
    Ok(())
}
