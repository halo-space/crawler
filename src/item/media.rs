use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;
use serde_json::{Map, Value};

use super::config::Kind;

static CSS_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"url\(\s*['\"]?([^'\")]+)['\"]?\s*\)"#).expect("valid CSS URL regex")
});
static IMAGE_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)https?://[^\s\"'<>\\)]+?\.(?:jpg|jpeg|png|webp|gif|bmp|svg|avif)(?:\?[^\s\"'<>\\)]*)?"#,
    )
    .expect("valid image URL regex")
});
static VIDEO_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)https?://[^\s\"'<>\\)]+?\.(?:mp4|webm|mov|m4v|m3u8|avi|mkv)(?:\?[^\s\"'<>\\)]*)?"#,
    )
    .expect("valid video URL regex")
});
static AUDIO_URL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)https?://[^\s\"'<>\\)]+?\.(?:mp3|wav|aac|m4a|ogg|flac)(?:\?[^\s\"'<>\\)]*)?"#)
        .expect("valid audio URL regex")
});

const IMAGE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "webp", "gif", "bmp", "svg", "avif"];
const VIDEO_EXTENSIONS: &[&str] = &["mp4", "webm", "mov", "m4v", "m3u8", "avi", "mkv"];
const AUDIO_EXTENSIONS: &[&str] = &["mp3", "wav", "aac", "m4a", "ogg", "flac"];

pub(crate) fn normalize(value: Value, kind: Kind, base_url: &str) -> Value {
    if kind == Kind::Text {
        return value;
    }

    let mut media = normalize_value(&value, kind, base_url);
    let mut seen = HashSet::new();
    media.retain(|value| {
        value
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|url| seen.insert(url.to_string()))
    });
    Value::Array(media)
}

fn normalize_value(value: &Value, kind: Kind, base_url: &str) -> Vec<Value> {
    match value {
        Value::Array(values) => values
            .iter()
            .flat_map(|value| normalize_value(value, kind, base_url))
            .collect(),
        Value::String(value) => urls_in_text(value, kind)
            .into_iter()
            .filter_map(|src| from_url(&src, None, kind, base_url))
            .collect(),
        Value::Object(value) => normalize_map(value, kind, base_url),
        Value::Null | Value::Bool(_) | Value::Number(_) => Vec::new(),
    }
}

fn normalize_map(value: &Map<String, Value>, kind: Kind, base_url: &str) -> Vec<Value> {
    let attrs = value.get("attrs").and_then(Value::as_object);
    let source = attrs.unwrap_or(value);
    let metadata = Metadata::new(value, attrs);
    let urls = urls_in_attributes(source, kind);

    if urls.is_empty() {
        if matches!(kind, Kind::Video | Kind::Audio)
            && let Some(html) = value.get("html").and_then(Value::as_str)
        {
            let media = urls_in_html(html)
                .into_iter()
                .filter_map(|src| from_url(&src, Some(&metadata), kind, base_url))
                .collect::<Vec<_>>();
            if !media.is_empty() {
                return media;
            }
        }

        return ["html", "text"]
            .into_iter()
            .filter_map(|name| value.get(name).and_then(Value::as_str))
            .flat_map(|value| urls_in_text(value, kind))
            .filter_map(|src| from_url(&src, Some(&metadata), kind, base_url))
            .collect();
    }

    urls.into_iter()
        .filter_map(|src| from_url(&src, Some(&metadata), kind, base_url))
        .collect()
}

fn urls_in_html(html: &str) -> Vec<String> {
    let soup = scrape_core::Soup::parse(html);
    soup.select("source[src]")
        .expect("valid source selector")
        .into_iter()
        .filter_map(|element| element.get("src").map(str::to_owned))
        .collect()
}

fn urls_in_attributes(value: &Map<String, Value>, kind: Kind) -> Vec<String> {
    let names: &[&str] = match kind {
        Kind::Image => &[
            "src",
            "url",
            "href",
            "data-src",
            "data-original",
            "data-lazy-src",
            "srcset",
            "poster",
        ],
        Kind::Video | Kind::Audio => &["src", "url", "href", "data-src", "data-original"],
        Kind::Text => &[],
    };
    for name in names {
        if let Some(value) = value.get(*name).and_then(Value::as_str) {
            if *name == "srcset" {
                return value
                    .split(',')
                    .filter_map(|item| item.split_whitespace().next())
                    .filter(|item| !item.is_empty())
                    .map(ToOwned::to_owned)
                    .collect();
            }
            if !value.trim().is_empty() {
                return vec![value.to_string()];
            }
        }
    }
    Vec::new()
}

fn urls_in_text(value: &str, kind: Kind) -> Vec<String> {
    let css = CSS_URL
        .captures_iter(value)
        .filter_map(|captures| captures.get(1))
        .map(|value| value.as_str().to_string())
        .collect::<Vec<_>>();
    if !css.is_empty() {
        return css;
    }

    let regex = match kind {
        Kind::Image => &*IMAGE_URL,
        Kind::Video => &*VIDEO_URL,
        Kind::Audio => &*AUDIO_URL,
        Kind::Text => return Vec::new(),
    };
    let urls = regex
        .find_iter(value)
        .map(|value| value.as_str().to_string())
        .collect::<Vec<_>>();
    if urls.is_empty() && !value.trim().is_empty() {
        vec![value.trim().to_string()]
    } else {
        urls
    }
}

fn from_url(src: &str, metadata: Option<&Metadata>, kind: Kind, base_url: &str) -> Option<Value> {
    let base_url = url::Url::parse(base_url).ok()?;
    let src = src.trim();
    let mut url = base_url.join(src).ok()?;
    if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
        return None;
    }
    url.set_fragment(None);
    let ext = extension(&url);
    if !ext.is_empty() && !extensions(kind).contains(&ext.as_str()) {
        return None;
    }
    let url = url.to_string();
    let metadata = metadata.cloned().unwrap_or_default();
    Some(serde_json::json!({
        "name": metadata.name,
        "url": url,
        "src": src,
        "width": metadata.width,
        "height": metadata.height,
        "size": metadata.size,
        "ext": ext,
        "alt": metadata.alt,
    }))
}

fn extension(url: &url::Url) -> String {
    url.path_segments()
        .and_then(Iterator::last)
        .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext))
        .filter(|ext| !ext.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_default()
}

fn extensions(kind: Kind) -> &'static [&'static str] {
    match kind {
        Kind::Image => IMAGE_EXTENSIONS,
        Kind::Video => VIDEO_EXTENSIONS,
        Kind::Audio => AUDIO_EXTENSIONS,
        Kind::Text => &[],
    }
}

#[derive(Clone, Default)]
struct Metadata {
    name: String,
    width: i64,
    height: i64,
    size: i64,
    alt: String,
}

impl Metadata {
    fn new(value: &Map<String, Value>, attrs: Option<&Map<String, Value>>) -> Self {
        let source = attrs.unwrap_or(value);
        Self {
            name: read_text(value.get("name")),
            width: read_integer(source.get("width").or_else(|| value.get("width"))),
            height: read_integer(source.get("height").or_else(|| value.get("height"))),
            size: read_integer(source.get("size").or_else(|| value.get("size"))),
            alt: read_text(
                source
                    .get("alt")
                    .or_else(|| source.get("title"))
                    .or_else(|| value.get("alt")),
            ),
        }
    }
}

fn read_text(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn read_integer(value: Option<&Value>) -> i64 {
    value
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        })
        .filter(|value| *value >= 0)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_node_attributes_into_fixed_media_object() {
        let value = serde_json::json!({
            "html": "<img src=\"../cover.JPG#preview\" width=\"800\" alt=\"Cover\">",
            "text": "",
            "attrs": {
                "src": "../cover.JPG#preview",
                "width": "800",
                "alt": "Cover"
            }
        });

        let normalized = normalize(value, Kind::Image, "https://example.com/books/1");

        assert_eq!(normalized[0]["name"], "");
        assert_eq!(normalized[0]["url"], "https://example.com/cover.JPG");
        assert_eq!(normalized[0]["src"], "../cover.JPG#preview");
        assert_eq!(normalized[0]["width"], 800);
        assert_eq!(normalized[0]["height"], 0);
        assert_eq!(normalized[0]["size"], 0);
        assert_eq!(normalized[0]["ext"], "jpg");
        assert_eq!(normalized[0]["alt"], "Cover");
    }

    #[test]
    fn expands_srcset_filters_type_mismatches_and_deduplicates() {
        let value = serde_json::json!([
            {"attrs": {"srcset": "/a.jpg 1x, /a@2x.jpg 2x"}},
            "/a.jpg",
            "/movie.mp4"
        ]);

        let normalized = normalize(value, Kind::Image, "https://example.com/article");

        assert_eq!(normalized.as_array().unwrap().len(), 2);
        assert_eq!(normalized[0]["src"], "/a.jpg");
        assert_eq!(normalized[1]["src"], "/a@2x.jpg");
    }

    #[test]
    fn extracts_media_urls_from_text_and_css() {
        let value = serde_json::json!([
            "background-image: url('../cover.webp')",
            "first https://cdn.example.com/a.JPG then https://cdn.example.com/b.png"
        ]);

        let normalized = normalize(value, Kind::Image, "https://example.com/css/main.css");

        assert_eq!(normalized.as_array().unwrap().len(), 3);
        assert_eq!(normalized[0]["src"], "../cover.webp");
    }

    #[test]
    fn empty_or_invalid_media_becomes_empty_array() {
        let value = serde_json::json!([null, 1, "mailto:test@example.com", "/file.pdf"]);

        assert_eq!(
            normalize(value, Kind::Image, "https://example.com/article"),
            serde_json::json!([])
        );
    }

    #[test]
    fn extracts_relative_video_sources_from_element_html() {
        let value = serde_json::json!({
            "html": "<video poster=\"/cover.jpg\"><source src=\"../movie.MP4\"></video>",
            "text": "",
            "attrs": {"poster": "/cover.jpg"}
        });

        let normalized = normalize(value, Kind::Video, "https://example.com/videos/1");

        assert_eq!(normalized.as_array().unwrap().len(), 1);
        assert_eq!(normalized[0]["src"], "../movie.MP4");
        assert_eq!(normalized[0]["ext"], "mp4");
    }

    #[test]
    fn audio_uses_its_own_extensions_and_rejects_other_media_types() {
        let value = serde_json::json!(["/track.flac", "/movie.mp4", "/cover.jpg"]);

        let normalized = normalize(value, Kind::Audio, "https://example.com/article");

        assert_eq!(normalized.as_array().unwrap().len(), 1);
        assert_eq!(normalized[0]["src"], "/track.flac");
        assert_eq!(normalized[0]["ext"], "flac");
    }
}
