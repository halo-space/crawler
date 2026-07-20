use std::net::IpAddr;

use crate::{config, graph, net, spider};

pub(super) fn check(config: &spider::Config, graph: &graph::Config) -> Result<(), config::Error> {
    if config.name.trim().is_empty() {
        return Err(config::Error::Message(
            "spider.name is required".to_string(),
        ));
    }
    if config
        .version
        .as_deref()
        .is_some_and(|version| version.trim().is_empty())
    {
        return Err(config::Error::Message(
            "spider.version must not be empty when provided".to_string(),
        ));
    }
    if let Some(timezone) = config.timezone.as_deref() {
        if timezone.trim().is_empty() {
            return Err(config::Error::Message(
                "spider.timezone must not be empty when provided".to_string(),
            ));
        }
        timezone.parse::<chrono_tz::Tz>().map_err(|_| {
            config::Error::Message("spider.timezone must be a valid IANA time zone".to_string())
        })?;
    }
    if config.start.is_empty() {
        return Err(config::Error::Message(
            "spider.start must not be empty".to_string(),
        ));
    }
    check_allowed_domains(&config.allowed_domains, "spider.allowed_domains")?;
    for spec in &config.start {
        if spec.node.trim().is_empty() {
            return Err(config::Error::Message(
                "spider.start node must not be empty".to_string(),
            ));
        }
        if !graph.nodes.contains_key(&spec.node) {
            return Err(config::Error::Message(format!(
                "spider.start node does not exist in graph.nodes: {}",
                spec.node
            )));
        }
        check_start_url(&spec.url)?;
        if spec.vals.contains_key("idx") {
            return Err(config::Error::Message(
                "spider.start vals must not define reserved key: idx".to_string(),
            ));
        }
        for value in spec.vals.values() {
            check_start_value(value)?;
        }
        spec.transport.validate_with(
            &format!("spider.start node {}", spec.node),
            valid_start_template,
        )?;
    }
    Ok(())
}

pub(super) fn check_allowed_domains(domains: &[String], field: &str) -> Result<(), config::Error> {
    if let Some(domain) = domains.iter().find(|domain| !is_host_only(domain)) {
        return Err(config::Error::Message(format!(
            "{field} must contain only host names or IP addresses without a scheme, port, path, query, or credentials: {domain}"
        )));
    }
    Ok(())
}

fn is_host_only(value: &str) -> bool {
    if value.is_empty() || value.trim() != value {
        return false;
    }
    if value.parse::<IpAddr>().is_ok() {
        return true;
    }

    match url::Host::parse(value) {
        Ok(url::Host::Domain(domain)) => {
            !value.contains('%') && is_domain_name(domain.strip_suffix('.').unwrap_or(&domain))
        }
        Ok(url::Host::Ipv4(_)) | Ok(url::Host::Ipv6(_)) => true,
        Err(_) => false,
    }
}

fn is_domain_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn valid_start_template(reference: &str) -> bool {
    matches!(reference, "request.url" | "request.node")
        || reference
            .strip_prefix("vals.")
            .is_some_and(|name| !name.is_empty())
        || !reference.contains('.') && !reference.is_empty()
}

fn check_start_url(reference: &graph::rules::ValueRef) -> Result<(), config::Error> {
    match (&reference.from, &reference.value) {
        (Some(path), None) => check_vals_path(path),
        (None, Some(serde_json::Value::String(url))) if !url.is_empty() => {
            net::Request::follow(url).map_err(|error| {
                config::Error::Message(format!("invalid spider.start URL: {error}"))
            })?;
            Ok(())
        }
        (None, Some(_)) => Err(config::Error::Message(
            "spider.start url must be a non-empty string or $vals reference".to_string(),
        )),
        (Some(_), Some(_)) => Err(config::Error::Message(
            "spider.start url cannot define both from and value".to_string(),
        )),
        (None, None) => Err(config::Error::Message(
            "spider.start url is required".to_string(),
        )),
    }
}

fn check_start_value(reference: &graph::rules::ValueRef) -> Result<(), config::Error> {
    match (&reference.from, &reference.value) {
        (Some(path), None) => check_vals_path(path),
        (None, Some(_)) => Ok(()),
        (Some(_), Some(_)) => Err(config::Error::Message(
            "spider.start value cannot define both from and value".to_string(),
        )),
        (None, None) => Err(config::Error::Message(
            "spider.start value requires from or value".to_string(),
        )),
    }
}

fn check_vals_path(path: &str) -> Result<(), config::Error> {
    if path
        .strip_prefix("$vals.")
        .is_some_and(|name| !name.is_empty())
    {
        Ok(())
    } else {
        Err(config::Error::Message(format!(
            "spider.start only supports $vals references: {path}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::check_allowed_domains;
    use crate::config::Config;

    #[test]
    fn start_transport_only_accepts_start_context_references() {
        let error = Config::from_yaml(
            r#"
spider:
  name: start
  start:
    - node: index
      url: https://example.com
      headers:
        X-Source: "{response.url}"
graph:
  nodes:
    index: {}
  edges: []
"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("response.url"));
    }

    #[test]
    fn request_header_and_cookie_templates_must_be_scalar() {
        let error = Config::from_yaml(
            r#"
spider:
  name: start
  start:
    - node: index
      url: https://example.com
      headers:
        X-Invalid: [one, two]
graph:
  nodes:
    index: {}
  edges: []
"#,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must be a string, number, or boolean")
        );
    }

    #[test]
    fn spider_metadata_must_not_be_blank() {
        let base = r#"
spider:
  name: metadata
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges: []
"#;

        let error =
            Config::from_yaml(&base.replace("name: metadata", "name: metadata\n  version: '  '"))
                .unwrap_err();
        assert!(error.to_string().contains("spider.version"));

        let error =
            Config::from_yaml(&base.replace("name: metadata", "name: metadata\n  timezone: '  '"))
                .unwrap_err();
        assert!(error.to_string().contains("spider.timezone"));
    }

    #[test]
    fn spider_timezone_must_be_a_valid_iana_name() {
        let rules = r#"
spider:
  name: metadata
  timezone: Mars/Olympus
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges: []
"#;

        let error = Config::from_yaml(rules).unwrap_err();
        assert!(error.to_string().contains("valid IANA time zone"));

        assert!(Config::from_yaml(&rules.replace("Mars/Olympus", "Asia/Shanghai")).is_ok());
    }

    #[test]
    fn allowed_domains_require_host_only_domain_names_or_ip_addresses() {
        let valid = [
            "example.com",
            "example.com.",
            "Example.COM",
            "localhost",
            "\u{4f8b}\u{5b50}.\u{6d4b}\u{8bd5}",
            "127.0.0.1",
            "::1",
            "[2001:db8::1]",
        ]
        .map(str::to_string);
        assert!(check_allowed_domains(&valid, "spider.allowed_domains").is_ok());

        for invalid in [
            "",
            " example.com",
            "example.com ",
            "example .com",
            "https://example.com",
            "example.com:443",
            "example.com/path",
            "example.com?source=test",
            "user@example.com",
            "*.example.com",
            "bad_domain.example",
            "-example.com",
            "example..com",
            "example.com..",
        ] {
            let error = check_allowed_domains(&[invalid.to_string()], "spider.allowed_domains")
                .unwrap_err();
            assert!(error.to_string().contains("without a scheme, port, path"));
        }
    }
}
