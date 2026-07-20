use encoding_rs::{Encoding, UTF_8, UTF_16BE, UTF_16LE, WINDOWS_1252, X_USER_DEFINED};

use crate::net::Headers;

const META_SCAN_LIMIT: usize = 1024;

pub(super) fn decode(body: &[u8], headers: &Headers) -> String {
    if let Some((encoding, bom_len)) = Encoding::for_bom(body) {
        return decode_with(encoding, &body[bom_len..]);
    }

    let content_type = content_type(headers);
    if let Some(encoding) = content_type
        .as_ref()
        .and_then(|content_type| charset(content_type))
    {
        return decode_with(encoding, body);
    }

    let inspect_meta = content_type.as_ref().is_none_or(|content_type| {
        content_type.type_() == mime::TEXT && content_type.subtype() == mime::HTML
    });
    if inspect_meta && let Some(encoding) = meta_charset(body) {
        return decode_with(encoding, body);
    }

    decode_with(UTF_8, body)
}

fn content_type(headers: &Headers) -> Option<mime::Mime> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .and_then(|(_, value)| value.parse().ok())
}

fn charset(content_type: &mime::Mime) -> Option<&'static Encoding> {
    content_type
        .get_param(mime::CHARSET)
        .and_then(|label| Encoding::for_label_no_replacement(label.as_str().as_bytes()))
}

fn meta_charset(body: &[u8]) -> Option<&'static Encoding> {
    let end = body.len().min(META_SCAN_LIMIT);
    let prefix = String::from_utf8_lossy(&body[..end]);
    let soup = scrape_core::Soup::parse(&prefix);
    let nodes = soup.select("meta").ok()?;

    for node in nodes {
        let label = if let Some(label) = node.get("charset") {
            Some(label.to_string())
        } else {
            node.get("http-equiv")
                .filter(|value| value.eq_ignore_ascii_case("content-type"))
                .and_then(|_| node.get("content"))
                .and_then(|value| value.parse::<mime::Mime>().ok())
                .and_then(|content_type| {
                    content_type
                        .get_param(mime::CHARSET)
                        .map(|value| value.as_str().to_string())
                })
        };
        if let Some(encoding) = label.as_deref().and_then(html_encoding) {
            return Some(encoding);
        }
    }
    None
}

fn html_encoding(label: &str) -> Option<&'static Encoding> {
    let encoding = Encoding::for_label_no_replacement(label.as_bytes())?;
    Some(if encoding == UTF_16BE || encoding == UTF_16LE {
        UTF_8
    } else if encoding == X_USER_DEFINED {
        WINDOWS_1252
    } else {
        encoding
    })
}

fn decode_with(encoding: &'static Encoding, body: &[u8]) -> String {
    encoding.decode_without_bom_handling(body).0.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(content_type: &str) -> Headers {
        Headers::from([("Content-Type".to_string(), content_type.to_string())])
    }

    #[test]
    fn recognized_boms_have_priority_and_are_removed() {
        let cases: &[(&[u8], &str)] = &[
            (
                b"\xEF\xBB\xBF<meta charset='gbk'>utf-8",
                "<meta charset='gbk'>utf-8",
            ),
            (b"\xFF\xFE\x2D\x4E\x87\x65", "中文"),
            (b"\xFE\xFF\x4E\x2D\x65\x87", "中文"),
        ];

        for (body, expected) in cases {
            assert_eq!(decode(body, &headers("text/html; charset=gbk")), *expected);
        }
    }

    #[test]
    fn content_type_accepts_quoted_labels_and_has_priority_over_meta() {
        let body = b"<meta charset='utf-8'><h1>\xB9\xF0\xC1\xD6\xC3\xD7\xB7\xDB</h1>";
        let decoded = decode(body, &headers("Text/Html; Charset=\"GBK\""));

        assert!(decoded.contains("桂林米粉"));
    }

    #[test]
    fn content_type_accepts_standard_charset_aliases() {
        let body = b"<h1>\xB9\xF0\xC1\xD6</h1>";

        assert!(decode(body, &headers("text/html; charset=gb2312")).contains("桂林"));
    }

    #[test]
    fn html_meta_supports_charset_and_http_equiv() {
        let direct = b"<meta charset='gbk'><h1>\xB9\xF0\xC1\xD6</h1>";
        let http_equiv = b"<meta http-equiv='Content-Type' content='text/html; charset=gbk'><h1>\xB9\xF0\xC1\xD6</h1>";

        assert!(decode(direct, &Headers::new()).contains("桂林"));
        assert!(decode(http_equiv, &headers("text/html")).contains("桂林"));
    }

    #[test]
    fn invalid_header_falls_back_to_html_meta() {
        let body = b"<meta charset='gbk'><h1>\xB9\xF0\xC1\xD6</h1>";
        let decoded = decode(body, &headers("text/html; charset=not-real"));

        assert!(decoded.contains("桂林"));
    }

    #[test]
    fn meta_must_end_within_the_scan_limit() {
        const META: &[u8] = b"<meta charset='gbk'>";
        let mut inside = vec![b' '; META_SCAN_LIMIT - META.len()];
        inside.extend_from_slice(META);
        inside.extend_from_slice(b"\xB9\xF0");
        let mut crossed = vec![b' '; META_SCAN_LIMIT - META.len() + 1];
        crossed.extend_from_slice(META);
        crossed.extend_from_slice(b"\xB9\xF0");

        assert!(decode(&inside, &Headers::new()).ends_with("桂"));
        assert!(decode(&crossed, &Headers::new()).ends_with("��"));
    }

    #[test]
    fn non_html_content_type_does_not_use_meta() {
        let body = b"<meta charset='gbk'>\xB9\xF0";

        assert!(decode(body, &headers("text/plain")).ends_with("��"));
    }

    #[test]
    fn invalid_utf8_uses_replacement_characters() {
        assert_eq!(decode(b"ok\xFF", &Headers::new()), "ok�");
    }

    #[test]
    fn malformed_bytes_do_not_change_a_selected_legacy_encoding() {
        let body = b"<meta charset='windows-1252'>\xFF";

        assert!(decode(body, &headers("text/html; charset=gbk")).ends_with('�'));
    }

    #[test]
    fn html_meta_applies_web_encoding_label_adjustments() {
        let utf16_label = b"<meta charset='utf-16le'><h1>utf-8 body</h1>";
        let user_defined = b"<meta charset='x-user-defined'>\x80";

        assert!(decode(utf16_label, &Headers::new()).contains("utf-8 body"));
        assert!(decode(user_defined, &Headers::new()).ends_with('€'));
    }
}
