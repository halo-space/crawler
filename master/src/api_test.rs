#[allow(dead_code)]
#[path = "../../contrib/tests/support/scheduler/conformance.rs"]
mod conformance;

use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use contrib::scheduler::api::Api;
use spider::scheduler::{Init as _, Lease};
use spider::{Scheduler as _, net, payload, scheduler, trace};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql as SqlxMySql, Pool};
use tokio::sync::oneshot;

use crate::{Config, Policy, Server};

const WORKER_TOKEN: &str = "api-conformance-worker-token";
const CONTROL_TOKEN: &str = "api-conformance-control-token";
const CONTRACT_LEASE_TIMEOUT: Duration = Duration::from_secs(60);
const CONTRACT_LEASE_INTERVAL: Duration = Duration::from_secs(2);
const CONTRACT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_LEASE_TIMEOUT: Duration = Duration::from_secs(2);
const RECOVERY_LEASE_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);

type TestResult<T> = std::result::Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn api_scheduler_conforms_through_master() -> TestResult<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping API Scheduler conformance test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let running = Running::start(
        &database_url,
        CONTRACT_LEASE_TIMEOUT,
        CONTRACT_LEASE_INTERVAL,
        CONTRACT_HEARTBEAT_INTERVAL,
    )
    .await?;

    let timing = conformance::Timing::new(
        CONTRACT_LEASE_TIMEOUT,
        CONTRACT_LEASE_INTERVAL,
        Duration::from_millis(250),
        Duration::from_millis(200),
        Duration::from_millis(100),
    );
    let lease = Lease::new(CONTRACT_LEASE_TIMEOUT, CONTRACT_LEASE_INTERVAL)?;

    conformance::run(
        || {
            fixture(
                &running.base_url,
                &running.namespace,
                running.pool.clone(),
                lease,
            )
        },
        false,
        Some(fixture(
            &running.base_url,
            &running.namespace,
            running.pool.clone(),
            lease,
        )),
        timing,
    )
    .await;
    verify_transport(&running.base_url, &running.namespace, lease).await?;
    running.close().await
}

#[tokio::test]
async fn api_scheduler_lease_conforms_through_master() -> TestResult<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping API Scheduler lease test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let running = Running::start(
        &database_url,
        RECOVERY_LEASE_TIMEOUT,
        RECOVERY_LEASE_INTERVAL,
        RECOVERY_HEARTBEAT_INTERVAL,
    )
    .await?;
    let timing = conformance::Timing::new(
        RECOVERY_LEASE_TIMEOUT,
        RECOVERY_LEASE_INTERVAL,
        Duration::from_millis(250),
        Duration::from_millis(200),
        Duration::from_millis(100),
    );
    let lease = Lease::new(RECOVERY_LEASE_TIMEOUT, RECOVERY_LEASE_INTERVAL)?;

    conformance::lease(
        fixture(
            &running.base_url,
            &running.namespace,
            running.pool.clone(),
            lease,
        ),
        timing,
    )
    .await;
    running.close().await
}

struct Running {
    base_url: String,
    namespace: String,
    pool: Pool<SqlxMySql>,
    shutdown: oneshot::Sender<()>,
    serving: tokio::task::JoinHandle<Result<(), crate::Error>>,
}

impl Running {
    async fn start(
        database_url: &str,
        lease_timeout: Duration,
        lease_interval: Duration,
        heartbeat_interval: Duration,
    ) -> TestResult<Self> {
        let namespace = format!("api-conformance-{}", uuid::Uuid::now_v7().simple());
        let policy = Policy {
            lease_timeout_ms: lease_timeout.as_millis().try_into()?,
            lease_interval_ms: lease_interval.as_millis().try_into()?,
            heartbeat_interval_ms: heartbeat_interval.as_millis().try_into()?,
        };
        let config = Config::new(
            "127.0.0.1:0".parse()?,
            database_url,
            &namespace,
            WORKER_TOKEN,
            CONTROL_TOKEN,
        )?
        .with_policy(policy)?
        .with_cron_interval(Duration::from_secs(60))?;
        let server = Server::from_config(config).await?;
        let pool = MySqlPoolOptions::new().connect(database_url).await?;
        clear(&pool, &namespace).await?;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let (shutdown, stopped) = oneshot::channel();
        let serving = tokio::spawn(server.serve_listener(listener, async move {
            let _ = stopped.await;
        }));
        Ok(Self {
            base_url,
            namespace,
            pool,
            shutdown,
            serving,
        })
    }

    async fn close(self) -> TestResult<()> {
        let _ = self.shutdown.send(());
        tokio::time::timeout(Duration::from_secs(5), self.serving)
            .await
            .map_err(|_| std::io::Error::other("Master server did not stop"))???;
        clear(&self.pool, &self.namespace).await?;
        self.pool.close().await;
        Ok(())
    }
}

async fn verify_transport(base_url: &str, namespace: &str, lease: Lease) -> TestResult<()> {
    let api = Api::new(base_url, WORKER_TOKEN)?
        .with_namespace(namespace)?
        .with_lease(lease)?;
    api.open().await?;

    let result = async {
        let trace_id = "trace/part name?query".to_string();
        api.init(
            trace_id.clone(),
            trace::Snapshot::code("transport-task"),
            Vec::new(),
        )
        .await?;
        let error = api
            .init(
                trace_id.clone(),
                trace::Snapshot::code("transport-task"),
                Vec::new(),
            )
            .await
            .unwrap_err();
        if !matches!(&error, scheduler::Error::Message(message) if message.contains("Trace already exists"))
        {
            return Err(std::io::Error::other(format!(
                "Master accepted a second init for the same Trace: {error}"
            ))
            .into());
        }
        let snapshot = api
            .trace(&trace_id)
            .await?
            .ok_or_else(|| std::io::Error::other("Master did not return the initialized Trace"))?;
        if snapshot.task_id != "transport-task" {
            return Err(std::io::Error::other("Master returned the wrong Trace Snapshot").into());
        }

        let mut request = net::Request::follow("https://example.com/completion").unwrap();
        request.id = "transport-completion".to_string();
        request.task_id = "transport-task".to_string();
        request.trace_id = trace_id;
        api.push(payload::Payload::new().requests(vec![request]))
            .await?;
        let request = api
            .next_requests(1, "transport-worker", &[net::Mode::Http])
            .await?
            .pop()
            .ok_or_else(|| std::io::Error::other("Master did not claim the test Request"))?;

        let mut release = payload::Payload::for_request(&request, "transport-worker");
        release.state = net::State::Processing;
        let (left, right) = tokio::join!(api.release(&release), api.release(&release));
        let error = match (left, right) {
            (Ok(()), Err(error)) | (Err(error), Ok(())) => error,
            (Ok(()), Ok(())) => {
                return Err(std::io::Error::other(
                    "Master accepted two independent releases for one execution",
                )
                .into());
            }
            (Err(left), Err(right)) => {
                return Err(std::io::Error::other(format!(
                    "Master rejected both independent releases: {left}; {right}"
                ))
                .into());
            }
        };
        if !error.is_ownership_loss() {
            return Err(std::io::Error::other(format!(
                "Master returned the wrong error for the second independent release: {error}"
            ))
            .into());
        }

        let request = api
            .next_requests(1, "transport-worker", &[net::Mode::Http])
            .await?
            .pop()
            .ok_or_else(|| std::io::Error::other("Master did not reclaim the released Request"))?;

        let mut ack = payload::Payload::for_request(&request, "transport-worker");
        ack.state = net::State::Processing;
        api.ack(&ack).await?;

        let mut settled = payload::Payload::for_request(&request, "transport-worker");
        settled.start_time = Some(1);
        settled.end_time = Some(2);
        let (left, right) = tokio::join!(api.success(&settled), api.success(&settled));
        left?;
        right?;

        let mut changed = payload::Payload::for_request(&request, "transport-worker");
        changed.start_time = Some(1);
        changed.end_time = Some(3);
        let error = api.success(&changed).await.unwrap_err();
        if !matches!(&error, scheduler::Error::Message(message) if message.contains("completion body conflicts"))
        {
            return Err(std::io::Error::other(format!(
                "Master did not reject a changed completion replay: {error}"
            ))
            .into());
        }
        Ok(())
    }
    .await;

    api.close().await?;
    result
}

fn fixture(base_url: &str, namespace: &str, pool: Pool<SqlxMySql>, lease: Lease) -> Reset {
    let api = Api::new(base_url, WORKER_TOKEN)
        .unwrap()
        .with_namespace(namespace)
        .unwrap()
        .with_lease(lease)
        .unwrap();
    Reset {
        api,
        pool,
        namespace: namespace.to_string(),
        cleared: AtomicBool::new(false),
    }
}

struct Reset {
    api: Api,
    pool: Pool<SqlxMySql>,
    namespace: String,
    cleared: AtomicBool,
}

impl scheduler::Scheduler for Reset {
    fn lease(&self) -> Option<Lease> {
        self.api.lease()
    }

    async fn open(&self) -> std::result::Result<(), scheduler::Error> {
        if !self.cleared.swap(true, Ordering::AcqRel) {
            clear(&self.pool, &self.namespace)
                .await
                .map_err(|error| scheduler::Error::Unavailable(error.to_string()))?;
        }
        self.api.open().await
    }

    async fn close(&self) -> std::result::Result<(), scheduler::Error> {
        self.api.close().await
    }

    async fn push(&self, value: payload::Payload) -> std::result::Result<(), scheduler::Error> {
        self.api.push(value).await
    }

    async fn push_items(
        &self,
        value: &payload::Payload,
    ) -> std::result::Result<(), scheduler::Error> {
        self.api.push_items(value).await
    }

    async fn trace(
        &self,
        trace_id: &str,
    ) -> std::result::Result<Option<trace::Snapshot>, scheduler::Error> {
        self.api.trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> std::result::Result<Vec<net::Request>, scheduler::Error> {
        self.api.next_requests(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> std::result::Result<bool, scheduler::Error> {
        self.api.has_pending_requests(worker_id, modes).await
    }

    async fn ack(&self, value: &payload::Payload) -> std::result::Result<(), scheduler::Error> {
        self.api.ack(value).await
    }

    async fn release(&self, value: &payload::Payload) -> std::result::Result<(), scheduler::Error> {
        self.api.release(value).await
    }

    async fn refresh_lease(
        &self,
        value: &payload::Payload,
    ) -> std::result::Result<(), scheduler::Error> {
        self.api.refresh_lease(value).await
    }

    async fn success(&self, value: &payload::Payload) -> std::result::Result<(), scheduler::Error> {
        self.api.success(value).await
    }

    async fn failure(&self, value: &payload::Payload) -> std::result::Result<(), scheduler::Error> {
        self.api.failure(value).await
    }
}

impl scheduler::Init for Reset {
    fn initializes_run(&self) -> bool {
        self.api.initializes_run()
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> std::result::Result<(), scheduler::Error> {
        self.api.init(trace_id, snapshot, requests).await
    }
}

fn database_url() -> Option<String> {
    std::env::var("CRAWLER_MYSQL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

async fn clear(pool: &Pool<SqlxMySql>, namespace: &str) -> TestResult<()> {
    for statement in [
        "DELETE FROM trace_stats WHERE namespace = ?",
        "DELETE FROM items WHERE namespace = ?",
        "DELETE FROM request_completions WHERE namespace = ?",
        "DELETE FROM requests WHERE namespace = ?",
        "DELETE FROM traces WHERE namespace = ?",
        "DELETE FROM workers WHERE namespace = ?",
        "DELETE FROM operations WHERE namespace = ?",
        "DELETE FROM tasks WHERE namespace = ?",
    ] {
        sqlx::query(statement).bind(namespace).execute(pool).await?;
    }
    Ok(())
}
