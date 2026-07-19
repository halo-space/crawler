use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::item::Item;
use crate::middleware::{Error, Middleware, Next, Spec};
use crate::net::{Request, Response};

pub struct Registry {
    middlewares: RwLock<HashMap<String, Arc<dyn Middleware>>>,
    defaults: Vec<Spec>,
    schemas: Arc<crate::item::schema::Store>,
}

#[derive(Debug)]
pub(crate) enum Output<T> {
    Continue(T),
    Skip { middleware: String },
}

struct Bind {
    spec: Spec,
    middleware: Arc<dyn Middleware>,
    order: i32,
    sequence: usize,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<M>(&self, name: impl Into<String>, middleware: M)
    where
        M: Middleware + 'static,
    {
        self.write().insert(name.into(), Arc::new(middleware));
    }

    pub(crate) fn schemas(&self) -> Arc<crate::item::schema::Store> {
        self.schemas.clone()
    }

    pub(crate) async fn before_scheduler(
        &self,
        mut request: Request,
    ) -> Result<Output<Request>, Error> {
        for bind in self.resolve(&request.middlewares, "before_scheduler", true)? {
            request = match bind
                .middleware
                .before_scheduler(request, &bind.spec)
                .await?
            {
                Next::Continue(request) => request,
                Next::Skip => {
                    return Ok(Output::Skip {
                        middleware: bind.spec.name,
                    });
                }
            };
        }

        Ok(Output::Continue(request))
    }

    pub(crate) async fn before_download(
        &self,
        mut request: Request,
    ) -> Result<Output<Request>, Error> {
        for bind in self.resolve(&request.middlewares, "before_download", true)? {
            request = match bind.middleware.before_download(request, &bind.spec).await? {
                Next::Continue(request) => request,
                Next::Skip => {
                    return Ok(Output::Skip {
                        middleware: bind.spec.name,
                    });
                }
            };
        }

        Ok(Output::Continue(request))
    }

    pub(crate) async fn after_download(
        &self,
        mut response: Response,
    ) -> Result<Output<Response>, Error> {
        for bind in self.resolve(&response.middlewares, "after_download", true)? {
            response = match bind.middleware.after_download(response, &bind.spec).await? {
                Next::Continue(response) => response,
                Next::Skip => {
                    return Ok(Output::Skip {
                        middleware: bind.spec.name,
                    });
                }
            };
        }

        Ok(Output::Continue(response))
    }

    pub(crate) async fn before_parse(
        &self,
        mut response: Response,
    ) -> Result<Output<Response>, Error> {
        for bind in self.resolve(&response.middlewares, "before_parse", true)? {
            response = match bind.middleware.before_parse(response, &bind.spec).await? {
                Next::Continue(response) => response,
                Next::Skip => {
                    return Ok(Output::Skip {
                        middleware: bind.spec.name,
                    });
                }
            };
        }

        Ok(Output::Continue(response))
    }

    pub(crate) async fn before_item(
        &self,
        mut item: Box<dyn Item>,
    ) -> Result<Output<Box<dyn Item>>, Error> {
        for bind in self.resolve(item.middlewares(), "before_item", true)? {
            item = match bind.middleware.before_item(item, &bind.spec).await? {
                Next::Continue(item) => item,
                Next::Skip => {
                    return Ok(Output::Skip {
                        middleware: bind.spec.name,
                    });
                }
            };
        }

        Ok(Output::Continue(item))
    }

    pub async fn before_spider(&self, specs: &[Spec]) -> Result<(), Error> {
        for bind in self.resolve(specs, "before_spider", false)? {
            bind.middleware.before_spider(&bind.spec).await?;
        }
        Ok(())
    }

    pub async fn after_spider(&self, specs: &[Spec]) -> Result<(), Error> {
        for bind in self.resolve(specs, "after_spider", false)? {
            bind.middleware.after_spider(&bind.spec).await?;
        }
        Ok(())
    }

    pub async fn error_download(&self, request: &Request, error: &str) -> Result<(), Error> {
        for bind in self.resolve(&request.middlewares, "error_download", false)? {
            bind.middleware
                .error_download(request, error, &bind.spec)
                .await?;
        }
        Ok(())
    }

    pub async fn error_parse(&self, response: &Response, error: &str) -> Result<(), Error> {
        for bind in self.resolve(&response.middlewares, "error_parse", false)? {
            bind.middleware
                .error_parse(response, error, &bind.spec)
                .await?;
        }
        Ok(())
    }

    pub async fn error_item(&self, item: &dyn Item, error: &str) -> Result<(), Error> {
        for bind in self.resolve(item.middlewares(), "error_item", false)? {
            bind.middleware.error_item(item, error, &bind.spec).await?;
        }
        Ok(())
    }

    pub(crate) fn retry_policy(
        &self,
        specs: &[Spec],
        hook: &str,
    ) -> Result<crate::middleware::retry::Policy, Error> {
        let mut effective = HashMap::<Option<String>, (usize, Spec)>::new();
        for (sequence, spec) in specs.iter().enumerate() {
            if spec.name != "retry" || spec.hook.as_deref().is_some_and(|value| value != hook) {
                continue;
            }
            crate::middleware::check(spec)?;
            if spec.skip {
                effective.remove(&spec.key);
            } else {
                effective.insert(spec.key.clone(), (sequence, spec.clone()));
            }
        }
        let mut effective = effective.into_values().collect::<Vec<_>>();
        effective.sort_by_key(|(sequence, spec)| (spec.order.unwrap_or(100), *sequence));

        let mut policy = crate::middleware::retry::Policy::default();
        for (_, spec) in effective {
            policy.extend(crate::middleware::retry::Policy::from_spec(&spec)?);
        }
        Ok(policy)
    }

    fn resolve(
        &self,
        specs: &[Spec],
        hook: &str,
        include_defaults: bool,
    ) -> Result<Vec<Bind>, Error> {
        let middlewares = self.read();
        let mut effective = HashMap::<(String, Option<String>), (usize, Spec)>::new();

        let defaults = if include_defaults {
            self.defaults.as_slice()
        } else {
            &[]
        };
        for (sequence, spec) in defaults.iter().chain(specs).enumerate() {
            if spec.hook.as_deref().is_some_and(|value| value != hook) {
                continue;
            }

            crate::middleware::check(spec)?;

            let key = (spec.name.clone(), spec.key.clone());
            if spec.skip {
                effective.remove(&key);
            } else {
                effective.insert(key, (sequence, spec.clone()));
            }
        }

        let mut resolved = effective
            .into_values()
            .map(|(sequence, spec)| {
                let middleware = middlewares
                    .get(&spec.name)
                    .cloned()
                    .ok_or_else(|| Error::NotRegistered(spec.name.clone()))?;
                let order = spec.order.unwrap_or_else(|| middleware.order(hook));
                Ok(Bind {
                    spec,
                    middleware,
                    order,
                    sequence,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        resolved.sort_by_key(|bind| (bind.order, bind.sequence));

        Ok(resolved)
    }

    fn read(&self) -> RwLockReadGuard<'_, HashMap<String, Arc<dyn Middleware>>> {
        self.middlewares
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, HashMap<String, Arc<dyn Middleware>>> {
        self.middlewares
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for Registry {
    fn default() -> Self {
        let registry = Self {
            middlewares: RwLock::new(HashMap::new()),
            defaults: [
                "before_scheduler",
                "before_download",
                "after_download",
                "before_parse",
                "before_item",
            ]
            .into_iter()
            .map(|hook| Spec::new("validate").hook(hook))
            .collect(),
            schemas: Arc::new(crate::item::schema::Store::new()),
        };
        registry.register(
            "validate",
            crate::middleware::validate::Validate::new(registry.schemas()),
        );
        registry.register("dedup", crate::middleware::dedup::Dedup::default());
        registry.register(
            "rate_limit",
            crate::middleware::rate_limit::RateLimit::default(),
        );
        registry.register("retry", crate::middleware::retry::Retry);
        registry
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    use super::*;

    struct Recorder {
        name: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[derive(serde::Serialize)]
    struct TestItem {
        #[serde(skip)]
        state: crate::item::State,
        #[serde(skip)]
        middlewares: Vec<Spec>,
    }

    impl Item for TestItem {
        fn from_values(_values: crate::item::Values) -> Result<Self, crate::item::Error> {
            Ok(Self {
                state: crate::item::State::default(),
                middlewares: Vec::new(),
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

        fn middlewares(&self) -> &[Spec] {
            &self.middlewares
        }
    }

    impl Middleware for Recorder {
        fn before_scheduler<'a>(
            &'a self,
            mut request: Request,
            spec: &'a Spec,
        ) -> crate::middleware::BoxFuture<'a, Next<Request>> {
            Box::pin(async move {
                let name = spec
                    .args
                    .get("label")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(self.name);
                self.calls.lock().unwrap().push(name.to_string());
                request
                    .headers
                    .insert(self.name.to_string(), "called".to_string());
                Ok(Next::Continue(request))
            })
        }

        fn error_download<'a>(
            &'a self,
            _request: &'a Request,
            error: &'a str,
            _spec: &'a Spec,
        ) -> crate::middleware::BoxFuture<'a, ()> {
            Box::pin(async move {
                self.calls.lock().unwrap().push(error.to_string());
                Ok(())
            })
        }
    }

    fn spec(name: &str, hook: &str) -> Spec {
        Spec {
            name: name.to_string(),
            hook: Some(hook.to_string()),
            ..Spec::default()
        }
    }

    #[tokio::test]
    async fn resolves_in_spec_order_and_applies_skip() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new();
        registry.register(
            "first",
            Recorder {
                name: "first",
                calls: calls.clone(),
            },
        );
        registry.register(
            "second",
            Recorder {
                name: "second",
                calls: calls.clone(),
            },
        );

        let mut skipped = spec("unused", "before_scheduler");
        skipped.skip = true;
        let mut request = Request::follow("https://example.com").unwrap();
        let mut second = spec("second", "before_scheduler");
        second.order = Some(20);
        second.args = serde_json::json!({"label": "second"});
        let mut first = spec("first", "before_scheduler");
        first.order = Some(10);
        first.args = serde_json::json!({"label": "first"});
        request.middlewares = vec![second, skipped, first, spec("first", "before_download")];

        let Output::Continue(request) = registry.before_scheduler(request).await.unwrap() else {
            panic!("request should continue");
        };

        assert_eq!(calls.lock().unwrap().as_slice(), ["first", "second"]);
        assert_eq!(
            request.headers.get("first").map(String::as_str),
            Some("called")
        );
        assert_eq!(
            request.headers.get("second").map(String::as_str),
            Some("called")
        );
    }

    #[tokio::test]
    async fn reports_missing_registration_for_active_spec() {
        let registry = Registry::new();
        let mut request = Request::follow("https://example.com").unwrap();
        request.middlewares = vec![spec("missing", "before_scheduler")];

        let error = registry.before_scheduler(request).await.unwrap_err();

        assert!(matches!(error, Error::NotRegistered(name) if name == "missing"));
    }

    #[tokio::test]
    async fn rejects_invalid_item_middleware_before_execution() {
        let registry = Registry::new();
        let item = TestItem {
            state: crate::item::State::default(),
            middlewares: vec![
                Spec::new("validate")
                    .hook("before_item")
                    .args(serde_json::json!({"required": "title"})),
            ],
        };

        let error = match registry.before_item(Box::new(item)).await {
            Ok(_) => panic!("invalid Item middleware must be rejected"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::InvalidConfig { name, .. } if name == "validate"));
    }

    #[tokio::test]
    async fn runs_error_hook_for_matching_specs() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new();
        registry.register(
            "errors",
            Recorder {
                name: "errors",
                calls: calls.clone(),
            },
        );
        let mut request = Request::follow("https://example.com").unwrap();
        request.middlewares = vec![spec("errors", "error_download")];

        registry
            .error_download(&request, "network down")
            .await
            .unwrap();

        assert_eq!(calls.lock().unwrap().as_slice(), ["network down"]);
    }

    #[tokio::test]
    async fn default_validate_can_be_skipped_for_one_hook() {
        let registry = Registry::new();
        let mut request = Request::follow("https://example.com").unwrap();
        request.url = "invalid".to_string();

        assert!(matches!(
            registry.before_scheduler(request.clone()).await.unwrap(),
            Output::Skip { .. }
        ));

        request.middlewares = vec![Spec {
            hook: Some("before_scheduler".to_string()),
            name: "validate".to_string(),
            skip: true,
            ..Spec::default()
        }];
        assert!(matches!(
            registry.before_scheduler(request).await.unwrap(),
            Output::Continue(_)
        ));
    }

    #[test]
    fn builtins_keep_the_documented_default_order() {
        assert_eq!(
            crate::middleware::validate::Validate::default().order("before_scheduler"),
            100
        );
        assert_eq!(
            crate::middleware::dedup::Dedup::default().order("before_scheduler"),
            400
        );
        assert_eq!(
            crate::middleware::rate_limit::RateLimit::default().order("before_download"),
            200
        );
        assert_eq!(crate::middleware::retry::Retry.order("error_download"), 100);
        assert_eq!(
            crate::middleware::validate::Validate::default().order("before_item"),
            200
        );
    }
}
