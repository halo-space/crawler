use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::Value;

use super::Item;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub const DEFAULT_TIMEOUT_MILLIS: u64 = 60_000;
pub const DEFAULT_MAX_BODY_BYTES: u64 = 64 * 1024 * 1024;

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
    let response = client
        .get(source)
        .send()
        .await
        .map_err(|error| super::Error::Message(error.to_string()))?
        .error_for_status()
        .map_err(|error| super::Error::Message(error.to_string()))?;
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use indexmap::IndexMap;

    use super::*;

    #[tokio::test]
    async fn local_download_rewrites_url_and_keeps_src() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
                )
                .unwrap();
        });

        let source = format!("http://{address}/cover.txt");
        let mut item = crate::item::Map::new(IndexMap::from([(
            "cover".to_string(),
            serde_json::json!([{"src": source, "url": source, "ext": "txt"}]),
        )]));
        item.id_mut().push_str("attachment-test");
        item.state_mut().set_download_fields(["cover"]);

        let directory =
            std::env::temp_dir().join(format!("harbor-attachment-{}", uuid::Uuid::now_v7()));
        let config = Config {
            enabled: true,
            store: Store {
                kind: "local".to_string(),
                path: directory.to_string_lossy().into_owned(),
            },
            retry: Retry::default(),
            timeout: DEFAULT_TIMEOUT_MILLIS,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        };
        Local.download(&mut item, &config).await.unwrap();

        let values = item.values().unwrap();
        let media = &values["cover"][0];
        assert_eq!(media["src"], source);
        assert_ne!(media["url"], source);
        assert_eq!(
            tokio::fs::read(media["url"].as_str().unwrap())
                .await
                .unwrap(),
            b"hello"
        );

        tokio::fs::remove_dir_all(directory).await.unwrap();
        server.join().unwrap();
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
