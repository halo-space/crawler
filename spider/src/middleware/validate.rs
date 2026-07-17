use std::sync::Arc;

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::{Protocol, Request, Response};

pub struct Validate {
    schemas: Arc<crate::item::schema::Store>,
}

impl Default for Validate {
    fn default() -> Self {
        Self::new(Arc::new(crate::item::schema::Store::new()))
    }
}

impl Validate {
    pub(crate) fn new(schemas: Arc<crate::item::schema::Store>) -> Self {
        Self { schemas }
    }
}

impl Middleware for Validate {
    fn order(&self, hook: &str) -> i32 {
        if hook == "before_item" { 200 } else { 100 }
    }

    fn before_scheduler<'a>(
        &'a self,
        request: Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            if request_url_is_valid(&request) {
                Ok(Next::Continue(request))
            } else {
                Ok(Next::Skip)
            }
        })
    }

    fn before_download<'a>(
        &'a self,
        request: Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            if !request_url_is_valid(&request) {
                return Err(crate::middleware::Error::Message(format!(
                    "request is not executable: {}",
                    request.url
                )));
            }
            if request.protocol != Protocol::Http {
                return Err(crate::middleware::Error::Message(format!(
                    "request protocol is not enabled: {:?}",
                    request.protocol
                )));
            }

            Ok(Next::Continue(request))
        })
    }

    fn after_download<'a>(
        &'a self,
        response: Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Response>> {
        Box::pin(async move {
            if url::Url::parse(&response.url).is_err() || response.status.0 == 0 {
                return Err(crate::middleware::Error::Message(
                    "downloader returned an invalid response".to_string(),
                ));
            }

            Ok(Next::Continue(response))
        })
    }

    fn before_parse<'a>(
        &'a self,
        response: Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Response>> {
        Box::pin(async move {
            if (200..400).contains(&response.status.0) {
                Ok(Next::Continue(response))
            } else {
                Ok(Next::Skip)
            }
        })
    }

    fn before_item<'a>(
        &'a self,
        item: Box<dyn crate::item::Item>,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Box<dyn crate::item::Item>>> {
        Box::pin(async move {
            let value = serde_json::to_value(item.as_ref()).map_err(|error| {
                crate::middleware::Error::Message(format!("item cannot be serialized: {error}"))
            })?;

            if let Some(key) = item.state().schema() {
                match self
                    .schemas
                    .validate(key, &value)
                    .map_err(|error| crate::middleware::Error::Message(error.to_string()))?
                {
                    crate::item::schema::Output::Valid => {}
                    crate::item::schema::Output::Invalid(_) => {
                        return Ok(Next::Skip);
                    }
                }
            } else if required_field_is_missing(&value, &spec.args) {
                return Ok(Next::Skip);
            }

            Ok(Next::Continue(item))
        })
    }
}

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    let Some(args) = spec.args.as_object() else {
        if spec.args.is_null() {
            return Ok(());
        }
        return Err(invalid_config("args must be an object"));
    };
    if args.keys().any(|name| name != "required") {
        return Err(invalid_config("only required is supported"));
    }
    if let Some(required) = args.get("required") {
        let values = required
            .as_array()
            .ok_or_else(|| invalid_config("required must be an array of field names"))?;
        if values
            .iter()
            .any(|value| value.as_str().is_none_or(|value| value.trim().is_empty()))
        {
            return Err(invalid_config(
                "required must contain non-empty field names",
            ));
        }
    }
    Ok(())
}

fn invalid_config(message: &str) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "validate".to_string(),
        message: message.to_string(),
    }
}

fn required_field_is_missing(value: &serde_json::Value, args: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return true;
    };
    args.get("required")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .any(|name| object.get(name).is_none_or(value_is_empty))
}

fn value_is_empty(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::String(value) => value.is_empty(),
        serde_json::Value::Array(value) => value.is_empty(),
        serde_json::Value::Object(value) => value.is_empty(),
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => false,
    }
}

fn request_url_is_valid(request: &Request) -> bool {
    url::Url::parse(&request.url)
        .ok()
        .is_some_and(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use serde_json::Value;

    use super::*;

    #[derive(serde::Serialize)]
    struct TestItem {
        #[serde(skip)]
        state: crate::item::State,
        value: Option<String>,
    }

    impl crate::item::Item for TestItem {
        fn from_values(mut values: crate::item::Values) -> Result<Self, crate::item::Error> {
            Ok(Self {
                state: crate::item::State::default(),
                value: values
                    .shift_remove("value")
                    .and_then(|value| value.as_str().map(str::to_string)),
            })
        }

        fn state(&self) -> &crate::item::State {
            &self.state
        }

        fn state_mut(&mut self) -> &mut crate::item::State {
            &mut self.state
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn skips_item_when_required_field_is_missing() {
        let item = Box::new(TestItem {
            state: crate::item::State::default(),
            value: None,
        });
        let spec = Spec::new("validate").args(serde_json::json!({"required": ["value"]}));

        let next = Validate::default().before_item(item, &spec).await.unwrap();

        assert!(matches!(next, Next::Skip));
    }

    #[tokio::test]
    async fn skips_rules_item_when_field_config_does_not_match() {
        let schema = serde_json::json!({"fields": {
            "title": {"type": "string", "rules": ["required", {"min": 2}]},
            "url": {"type": "string", "rules": ["required", "url"]}
        }});
        let store = Arc::new(crate::item::schema::Store::new());
        let key = store.register(&schema).unwrap();
        let mut item = crate::item::Map::new(indexmap::IndexMap::from([
            ("title".to_string(), Value::from("x")),
            ("url".to_string(), Value::from("not-a-url")),
        ]));
        crate::item::Item::state_mut(&mut item).set_schema(Some(key));
        let item = Box::new(item);
        let spec = Spec::new("validate");

        let next = Validate::new(store).before_item(item, &spec).await.unwrap();

        assert!(matches!(next, Next::Skip));
    }

    #[tokio::test]
    async fn accepts_rules_item_when_all_field_rules_match() {
        let schema = serde_json::json!({"fields": {
            "title": {"type": "string", "rules": ["required", {"min": 2}]},
            "url": {"type": "string", "rules": ["required", "url"]}
        }});
        let store = Arc::new(crate::item::schema::Store::new());
        let key = store.register(&schema).unwrap();
        let mut item = crate::item::Map::new(indexmap::IndexMap::from([
            ("title".to_string(), Value::from("Rust")),
            ("url".to_string(), Value::from("https://example.com/rust")),
        ]));
        crate::item::Item::state_mut(&mut item).set_schema(Some(key));
        let item = Box::new(item);
        let spec = Spec::new("validate");

        let next = Validate::new(store).before_item(item, &spec).await.unwrap();

        assert!(matches!(next, Next::Continue(_)));
    }
}
