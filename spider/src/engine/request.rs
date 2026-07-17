use std::sync::Arc;

use crate::engine::contract::Execute;
use crate::spider::tx;
use crate::{downloader, middleware, net, scheduler, spider, stats};

/// 执行一条已领取 Request 的完整下载与解析生命周期。
pub(crate) async fn execute<R, D>(
    claimed: &net::Request,
    downloader: Arc<D>,
    executor: Arc<R>,
    registry: Arc<middleware::Registry>,
    stats: Arc<stats::Delta>,
) -> Result<(), crate::Error>
where
    D: downloader::Download + Sync,
    R: Execute + 'static,
{
    let mut request = match registry
        .before_download(claimed.clone())
        .await
        .map_err(crate::Error::Middleware)?
    {
        middleware::registry::Output::Continue(request) => request,
        middleware::registry::Output::Skip { .. } => {
            stats.filter(claimed.node_key(), 1);
            return Ok(());
        }
    };

    let allowed_domains = executor.allowed_domains(&request).await;
    request.set_allowed_domains(allowed_domains);
    if !is_allowed(&request) {
        stats.filter(claimed.node_key(), 1);
        return Ok(());
    }

    executor.validate(&request)?;

    let download_retry = registry
        .retry_policy(&request.middlewares, "error_download")
        .map_err(crate::Error::Middleware)?;
    let mut download_attempt = 0;
    let response = loop {
        match downloader.fetch(request.clone()).await {
            Ok(response) => break response,
            Err(downloader::Error::DisallowedRedirect(_)) => {
                stats.filter(claimed.node_key(), 1);
                return Ok(());
            }
            Err(error) => {
                if let Some(delay) = download_retry.delay(download_attempt) {
                    download_attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                registry
                    .error_download(&request, &error.to_string())
                    .await
                    .map_err(crate::Error::Middleware)?;
                stats.download(claimed.node_key(), 1);
                return Err(crate::Error::Download(error));
            }
        }
    };

    let response = match registry
        .after_download(response)
        .await
        .map_err(crate::Error::Middleware)?
    {
        middleware::registry::Output::Continue(response) => response,
        middleware::registry::Output::Skip { .. } => {
            stats.filter(claimed.node_key(), 1);
            return Ok(());
        }
    };

    let response = match registry
        .before_parse(response)
        .await
        .map_err(crate::Error::Middleware)?
    {
        middleware::registry::Output::Continue(response) => response,
        middleware::registry::Output::Skip { .. } => {
            stats.filter(claimed.node_key(), 1);
            return Ok(());
        }
    };

    let parse_retry = registry
        .retry_policy(&response.middlewares, "error_parse")
        .map_err(crate::Error::Middleware)?;
    let mut parse_attempt = 0;
    loop {
        let error_response = response.clone();
        let parsing = tx::scope(claimed, stats.clone(), async {
            executor.parse(request.clone(), response.clone()).await
        })
        .await;

        match parsing {
            Ok(()) => return Ok(()),
            Err(crate::Error::Spider(spider::Error::RequestRejected(message))) => {
                return Err(crate::Error::Scheduler(scheduler::Error::Message(message)));
            }
            Err(crate::Error::Spider(spider::Error::ItemRejected(message))) => {
                return Err(crate::Error::Item(crate::item::Error::Message(message)));
            }
            Err(crate::Error::Spider(spider::Error::ChannelClosed)) => {
                return Err(crate::Error::message(
                    spider::Error::ChannelClosed.to_string(),
                ));
            }
            Err(error) if is_parse_error(&error) => {
                if let Some(delay) = parse_retry.delay(parse_attempt) {
                    parse_attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                registry
                    .error_parse(&error_response, &error.to_string())
                    .await
                    .map_err(crate::Error::Middleware)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_parse_error(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Selector(_)
            | crate::Error::Graph(_)
            | crate::Error::Item(_)
            | crate::Error::Spider(spider::Error::Message(_))
            | crate::Error::Message(_)
    )
}

fn is_allowed(request: &net::Request) -> bool {
    let Ok(url) = url::Url::parse(&request.url) else {
        return false;
    };
    request.allows(&url)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct DisallowedDownload {
        calls: AtomicUsize,
    }

    impl downloader::Download for DisallowedDownload {
        async fn open(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn fetch(&self, _request: net::Request) -> Result<net::Response, downloader::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(downloader::Error::DisallowedRedirect(
                "https://outside.example".to_string(),
            ))
        }
    }

    struct Executor {
        parses: AtomicUsize,
    }

    struct ResponseDownload {
        calls: AtomicUsize,
    }

    impl downloader::Download for ResponseDownload {
        async fn open(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn close(&self) -> Result<(), downloader::Error> {
            Ok(())
        }

        async fn fetch(&self, request: net::Request) -> Result<net::Response, downloader::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let url = request.url.clone();
            Ok(net::Response {
                vals: request.vals.clone(),
                kwargs: request.kwargs.clone(),
                middlewares: request.middlewares.clone(),
                request,
                url,
                status: net::StatusCode(200),
                reason: Some("OK".to_string()),
                version: net::HttpVersion::Http11,
                redirects: Vec::new(),
                headers: net::Headers::new(),
                cookies: net::Cookies::new(),
                body: bytes::Bytes::from_static(b"<h1>book</h1>"),
            })
        }
    }

    struct TypedErrorExecutor {
        parses: AtomicUsize,
    }

    impl super::Execute for TypedErrorExecutor {
        async fn allowed_domains(&self, _request: &net::Request) -> Vec<String> {
            Vec::new()
        }

        fn validate(&self, _request: &net::Request) -> Result<(), crate::Error> {
            Ok(())
        }

        async fn parse(
            &self,
            _request: net::Request,
            _response: net::Response,
        ) -> Result<(), crate::Error> {
            let attempt = self.parses.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(crate::selector::Error::Css("temporary".to_string()).into())
            } else {
                Ok(())
            }
        }
    }

    impl super::Execute for Executor {
        async fn allowed_domains(&self, _request: &net::Request) -> Vec<String> {
            vec!["example.com".to_string()]
        }

        fn validate(&self, _request: &net::Request) -> Result<(), crate::Error> {
            Ok(())
        }

        async fn parse(
            &self,
            _request: net::Request,
            _response: net::Response,
        ) -> Result<(), crate::Error> {
            self.parses.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn allowed_domains_are_ascii_case_insensitive() {
        let request = net::Request::follow("https://News.Example.com/article").unwrap();
        let mut request = request;
        request.set_allowed_domains(vec!["Example.COM".to_string()]);

        assert!(is_allowed(&request));
    }

    #[tokio::test]
    async fn disallowed_redirect_is_filtered_without_download_retry() {
        let request = net::Request::follow("https://example.com")
            .unwrap()
            .with_retry(3, [0]);
        let download = Arc::new(DisallowedDownload {
            calls: AtomicUsize::new(0),
        });
        let executor = Arc::new(Executor {
            parses: AtomicUsize::new(0),
        });
        let delta = Arc::new(stats::Delta::default());

        execute(
            &request,
            download.clone(),
            executor.clone(),
            Arc::new(middleware::Registry::new()),
            delta.clone(),
        )
        .await
        .unwrap();

        assert_eq!(download.calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor.parses.load(Ordering::SeqCst), 0);
        assert_eq!(delta.snapshot()["index"]["filter"], 1);
        assert_eq!(delta.snapshot()["index"]["download"], 0);
    }

    #[tokio::test]
    async fn typed_parse_errors_retry_the_same_downloaded_response() {
        let request = net::Request::follow("https://example.com")
            .unwrap()
            .with_retry(2, [0]);
        let download = Arc::new(ResponseDownload {
            calls: AtomicUsize::new(0),
        });
        let executor = Arc::new(TypedErrorExecutor {
            parses: AtomicUsize::new(0),
        });

        execute(
            &request,
            download.clone(),
            executor.clone(),
            Arc::new(middleware::Registry::new()),
            Arc::new(stats::Delta::default()),
        )
        .await
        .unwrap();

        assert_eq!(download.calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor.parses.load(Ordering::SeqCst), 3);
    }
}
