use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;
use url::{Host, Url};

use super::Item;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub const DEFAULT_TIMEOUT_MILLIS: u64 = 60_000;
pub const DEFAULT_MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_REDIRECTS: usize = 10;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub store: Store,
    #[serde(default)]
    pub retry: Retry,
    #[serde(default = "default_timeout_millis")]
    pub timeout: u64,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            store: Store::default(),
            retry: Retry::default(),
            timeout: default_timeout_millis(),
            max_body_bytes: default_max_body_bytes(),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.timeout == 0 {
            return Err("spider.attachment.timeout must be positive".to_string());
        }
        if self.max_body_bytes == 0 {
            return Err("spider.attachment.max_body_bytes must be positive".to_string());
        }
        if self.store.path.trim().is_empty() {
            return Err("spider.attachment.store.path must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Store {
    #[serde(default = "default_store_kind")]
    pub kind: String,
    #[serde(default = "default_store_path")]
    pub path: String,
}

impl Default for Store {
    fn default() -> Self {
        Self {
            kind: default_store_kind(),
            path: default_store_path(),
        }
    }
}

fn default_timeout_millis() -> u64 {
    DEFAULT_TIMEOUT_MILLIS
}

fn default_max_body_bytes() -> u64 {
    DEFAULT_MAX_BODY_BYTES
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retry {
    #[serde(default)]
    pub count: usize,
    #[serde(default)]
    pub backoff: Vec<u64>,
}

fn default_store_kind() -> String {
    "local".to_string()
}

fn default_store_path() -> String {
    "data/media".to_string()
}

/// Downloads the media fields selected by Item state.
///
/// Implementations must honor `Config::timeout` and `Config::max_body_bytes` and
/// write the actual downloaded byte count to each media object's `size` field.
pub trait AttachmentDownloader: Send + Sync {
    fn download<'a>(
        &'a self,
        item: &'a mut dyn Item,
        config: &'a Config,
    ) -> BoxFuture<'a, Result<(), super::Error>>;
}

#[derive(Default)]
pub struct Local;

impl AttachmentDownloader for Local {
    fn download<'a>(
        &'a self,
        item: &'a mut dyn Item,
        config: &'a Config,
    ) -> BoxFuture<'a, Result<(), super::Error>> {
        Box::pin(async move {
            if config.store.kind != "local" {
                return Err(super::Error::Message(format!(
                    "unsupported attachment store: {}",
                    config.store.kind
                )));
            }

            let mut values = item.values()?;
            let fields = item
                .state()
                .download_fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            if fields.is_empty() {
                return Ok(());
            }

            if config.timeout == 0 {
                return Err(super::Error::Message(
                    "attachment timeout must be positive".to_string(),
                ));
            }
            if config.max_body_bytes == 0 {
                return Err(super::Error::Message(
                    "attachment max_body_bytes must be positive".to_string(),
                ));
            }
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_millis(config.timeout))
                .build()
                .map_err(|error| super::Error::Message(error.to_string()))?;
            let root = PathBuf::from(&config.store.path);
            tokio::fs::create_dir_all(&root).await?;
            for field in fields {
                let Some(Value::Array(media)) = values.get_mut(&field) else {
                    continue;
                };
                for (index, value) in media.iter_mut().enumerate() {
                    let Some(object) = value.as_object_mut() else {
                        continue;
                    };
                    let Some(source) = object
                        .get("url")
                        .or_else(|| object.get("src"))
                        .and_then(Value::as_str)
                    else {
                        continue;
                    };
                    let body = tokio::time::timeout(
                        Duration::from_millis(config.timeout),
                        read_body(&client, source, config.max_body_bytes),
                    )
                    .await
                    .map_err(|_| {
                        super::Error::Message("attachment download timed out".to_string())
                    })??;
                    let extension = object
                        .get("ext")
                        .and_then(Value::as_str)
                        .filter(|value| !value.is_empty())
                        .map(safe_extension)
                        .unwrap_or_else(|| "bin".to_string());
                    let path = root.join(format!(
                        "{}-{}-{index}.{extension}",
                        crate::utils::path::segment(item.id()),
                        crate::utils::path::segment(&field),
                    ));
                    tokio::fs::write(&path, &body).await?;
                    object.insert(
                        "url".to_string(),
                        Value::String(path.to_string_lossy().into_owned()),
                    );
                    object.insert("size".to_string(), Value::from(body.len() as i64));
                }
            }
            item.replace_values(values)
        })
    }
}

async fn read_body(
    client: &reqwest::Client,
    source: &str,
    limit: u64,
) -> Result<Vec<u8>, super::Error> {
    let mut url = parse_attachment_url(source)?;

    for redirect_count in 0..=MAX_REDIRECTS {
        validate_attachment_url(&url).await?;
        let response = client
            .get(url.clone())
            .send()
            .await
            .map_err(|error| super::Error::Message(error.to_string()))?;

        if response.status().is_redirection() {
            if redirect_count == MAX_REDIRECTS {
                return Err(super::Error::Message(
                    "attachment redirect limit exceeded".to_string(),
                ));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or_else(|| {
                    super::Error::Message("attachment redirect has no location".to_string())
                })?
                .to_str()
                .map_err(|_| {
                    super::Error::Message("attachment redirect location is invalid".to_string())
                })?;
            url = url.join(location).map_err(|error| {
                super::Error::Message(format!("invalid attachment redirect: {error}"))
            })?;
            continue;
        }

        let response = response
            .error_for_status()
            .map_err(|error| super::Error::Message(error.to_string()))?;
        return read_response_body(response, limit).await;
    }

    unreachable!("attachment redirect loop is bounded")
}

fn parse_attachment_url(source: &str) -> Result<Url, super::Error> {
    Url::parse(source)
        .map_err(|error| super::Error::Message(format!("invalid attachment URL: {error}")))
}

async fn validate_attachment_url(url: &Url) -> Result<(), super::Error> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(super::Error::Message(format!(
            "attachment URL scheme is not allowed: {}",
            url.scheme()
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(super::Error::Message(
            "attachment URL credentials are not allowed".to_string(),
        ));
    }

    let host = url
        .host()
        .ok_or_else(|| super::Error::Message("attachment URL must contain a host".to_string()))?;
    match host {
        Host::Ipv4(ip) => reject_forbidden_ip(IpAddr::V4(ip))?,
        Host::Ipv6(ip) => reject_forbidden_ip(IpAddr::V6(ip))?,
        Host::Domain(domain) => {
            if is_local_domain(domain) {
                return Err(super::Error::Message(format!(
                    "attachment host is not allowed: {domain}"
                )));
            }

            let port = url.port_or_known_default().ok_or_else(|| {
                super::Error::Message("attachment URL must contain a known port".to_string())
            })?;
            let host = domain.to_string();
            let addresses = tokio::task::spawn_blocking(move || {
                (host.as_str(), port)
                    .to_socket_addrs()
                    .map(|addresses| addresses.collect::<Vec<_>>())
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(|error| super::Error::Message(error.to_string()))?
            .map_err(|error| {
                super::Error::Message(format!("cannot resolve attachment host: {error}"))
            })?;

            if addresses.is_empty() {
                return Err(super::Error::Message(
                    "attachment host resolved to no addresses".to_string(),
                ));
            }
            for address in addresses {
                reject_forbidden_ip(address.ip())?;
            }
        }
    }
    Ok(())
}

fn is_local_domain(domain: &str) -> bool {
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();
    domain == "localhost" || domain.ends_with(".localhost")
}

fn reject_forbidden_ip(ip: IpAddr) -> Result<(), super::Error> {
    if is_forbidden_ip(ip) {
        return Err(super::Error::Message(format!(
            "attachment host resolves to a restricted address: {ip}"
        )));
    }
    Ok(())
}

fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_forbidden_ipv4(ip),
        IpAddr::V6(ip) => is_forbidden_ipv6(ip),
    }
}

fn is_forbidden_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 168)
        || (a == 198 && (18..=19).contains(&b))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224
}

fn is_forbidden_ipv6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    let octets = ip.octets();
    let mapped_ipv4 = (segments[..5] == [0, 0, 0, 0, 0] && segments[5] == 0xffff)
        .then(|| Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]));

    ip.is_unspecified()
        || ip.is_loopback()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] & 0xff00) == 0xff00
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || mapped_ipv4.is_some_and(is_forbidden_ipv4)
}

async fn read_response_body(
    response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, super::Error> {
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(super::Error::Message(format!(
            "attachment body exceeds {} bytes",
            limit
        )));
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| super::Error::Message(error.to_string()))?;
        let next = body.len() as u64 + chunk.len() as u64;
        if next > limit {
            return Err(super::Error::Message(format!(
                "attachment body exceeds {} bytes",
                limit
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn safe_extension(value: &str) -> String {
    let value = value.trim_matches('.');
    if !value.is_empty()
        && value.len() <= 16
        && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        value.to_ascii_lowercase()
    } else {
        "bin".to_string()
    }
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use super::*;

    #[tokio::test]
    async fn local_download_rejects_private_address() {
        let source = "http://127.0.0.1:8080/cover.txt";
        let mut item = crate::item::Map::new(IndexMap::from([(
            "cover".to_string(),
            serde_json::json!([{"src": source, "url": source, "ext": "txt"}]),
        )]));
        item.state_mut().set_download_fields(["cover"]);

        let config = Config {
            enabled: true,
            ..Default::default()
        };
        let error = Local.download(&mut item, &config).await.unwrap_err();
        assert!(error.to_string().contains("restricted address"));
    }

    #[tokio::test]
    async fn attachment_url_rejects_non_http_and_localhost() {
        for source in ["file:///tmp/cover.txt", "http://localhost/cover.txt"] {
            let error = validate_attachment_url(&parse_attachment_url(source).unwrap())
                .await
                .unwrap_err();
            assert!(error.to_string().contains("not allowed"));
        }
    }

    #[test]
    fn attachment_address_policy_rejects_private_and_reserved_ranges() {
        for address in [
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6("fc00::1".parse().unwrap()),
            IpAddr::V6("::ffff:127.0.0.1".parse().unwrap()),
        ] {
            assert!(is_forbidden_ip(address), "{address} should be rejected");
        }
        assert!(!is_forbidden_ip(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1))));
    }

    #[test]
    fn config_uses_one_default_limit_and_allows_an_override() {
        let default: Config = serde_json::from_value(serde_json::json!({
            "enabled": true
        }))
        .unwrap();
        assert_eq!(default.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(default.timeout, DEFAULT_TIMEOUT_MILLIS);
        assert!(default.validate().is_ok());

        let overridden: Config = serde_json::from_value(serde_json::json!({
            "enabled": true,
            "max_body_bytes": 1024,
            "timeout": 2500
        }))
        .unwrap();
        assert_eq!(overridden.max_body_bytes, 1024);
        assert_eq!(overridden.timeout, 2500);

        let invalid = Config {
            max_body_bytes: 0,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }
}
