use spider::{net, payload, scheduler, trace};
use tokio::sync::Mutex;

use super::error::{message, redis as redis_error, status};
use super::key::Keys;
use super::script::Scripts;

/// A Redis 7 standalone Scheduler.
///
/// The Scheduler owns no Worker-local files. Its queue, leases, Trace Snapshots, completions,
/// statistics, and Item output are scoped by its namespace in Redis.
pub struct Redis {
    pub(super) client: redis::Client,
    pub(super) connection: Mutex<Option<redis::aio::ConnectionManager>>,
    pub(super) keys: Keys,
    pub(super) lease: scheduler::Lease,
    pub(super) scripts: Scripts,
}

impl Redis {
    /// Creates a Redis Scheduler with the default `crawler` namespace.
    ///
    /// The URL is parsed here, while [`scheduler::Scheduler::open`] establishes the connection.
    pub fn new(url: impl Into<String>) -> Result<Self, scheduler::Error> {
        let client = redis::Client::open(url.into()).map_err(message)?;
        Ok(Self {
            client,
            connection: Mutex::new(None),
            keys: Keys::new("crawler")?,
            lease: scheduler::Lease::default(),
            scripts: Scripts::new(),
        })
    }

    /// Selects the namespace used for all Redis keys owned by this Scheduler.
    pub fn with_namespace(
        mut self,
        namespace: impl Into<String>,
    ) -> Result<Self, scheduler::Error> {
        self.keys = Keys::new(namespace)?;
        Ok(self)
    }

    /// Replaces the default lease policy.
    pub fn with_lease(mut self, lease: scheduler::Lease) -> Self {
        self.lease = lease;
        self
    }

    /// Returns the namespace selected for this Scheduler.
    pub fn namespace(&self) -> &str {
        self.keys.namespace()
    }

    pub(super) async fn connection(
        &self,
    ) -> Result<redis::aio::ConnectionManager, scheduler::Error> {
        self.connection
            .lock()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| scheduler::Error::Message("Redis Scheduler is not open".to_string()))
    }

    async fn connect(&self) -> Result<redis::aio::ConnectionManager, scheduler::Error> {
        let mut connection = redis::aio::ConnectionManager::new(self.client.clone())
            .await
            .map_err(redis_error)?;
        let _: String = redis::cmd("PING")
            .query_async(&mut connection)
            .await
            .map_err(redis_error)?;
        self.scripts
            .load(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(connection)
    }

    pub(super) fn encode<T: serde::Serialize>(value: &T) -> Result<String, scheduler::Error> {
        serde_json::to_string(value).map_err(message)
    }

    pub(super) fn result(value: String, id: &str) -> Result<(), scheduler::Error> {
        if value == "OK" {
            Ok(())
        } else {
            Err(status(&value, id))
        }
    }
}

impl scheduler::Scheduler for Redis {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self) -> Result<(), scheduler::Error> {
        // Do not let close finish while this open is still establishing its connection.
        let mut active = self.connection.lock().await;
        if let Some(connection) = active.as_mut() {
            let _: String = redis::cmd("PING")
                .query_async(connection)
                .await
                .map_err(redis_error)?;
            self.scripts.load(connection).await.map_err(redis_error)?;
            return Ok(());
        }

        let connection = self.connect().await?;
        *active = Some(connection);
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        self.connection.lock().await.take();
        Ok(())
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        self.enqueue(payload).await
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.write_items(payload).await
    }

    async fn trace(&self, trace_id: &str) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        self.load_trace(trace_id).await
    }

    async fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        self.claim(limit, worker_id, modes).await
    }

    async fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        self.pending(worker_id, modes).await
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.acknowledge(payload).await
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.return_to_queue(payload).await
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.refresh(payload).await
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.succeed(payload).await
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        self.fail(payload).await
    }
}

impl scheduler::Init for Redis {
    fn initializes_run(&self) -> bool {
        false
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        self.initialize(trace_id, snapshot, requests).await
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read as _, Write as _};
    use std::net::{TcpListener, TcpStream};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use spider::Scheduler as _;
    use tokio::sync::oneshot;

    use super::Redis;

    #[tokio::test]
    async fn close_waiting_behind_open_leaves_the_scheduler_closed() {
        let (url, mut ping_started, resume, server_task) = fake_server();
        let scheduler = Redis::new(url).unwrap();
        {
            let active = scheduler.connection.lock().await;

            let open = scheduler.open();
            tokio::pin!(open);
            tokio::select! {
                biased;
                result = &mut open => panic!("open unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }

            let close = scheduler.close();
            tokio::pin!(close);
            tokio::select! {
                biased;
                result = &mut close => panic!("close unexpectedly completed: {result:?}"),
                _ = tokio::task::yield_now() => {}
            }

            drop(active);
            let settled = async { tokio::join!(&mut open, &mut close) };
            tokio::pin!(settled);
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    _ = &mut ping_started => {}
                    result = &mut settled => panic!("open and close completed before PING was blocked: {result:?}"),
                }
            })
            .await
            .expect("open did not reach the controlled PING");
            resume.send(()).unwrap();
            let (opened, closed) = settled.await;
            opened.unwrap();
            closed.unwrap();

            assert!(scheduler.connection.lock().await.is_none());
        }
        drop(scheduler);
        tokio::task::spawn_blocking(move || server_task.join().unwrap())
            .await
            .unwrap();
    }

    fn fake_server() -> (
        String,
        oneshot::Receiver<()>,
        mpsc::Sender<()>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (ping_sender, ping_started) = oneshot::channel();
        let (resume, resume_receiver) = mpsc::channel();
        let server_task =
            thread::spawn(move || serve_redis(listener, ping_sender, resume_receiver));
        (
            format!("redis://{address}"),
            ping_started,
            resume,
            server_task,
        )
    }

    fn serve_redis(
        listener: TcpListener,
        ping_sender: oneshot::Sender<()>,
        resume: mpsc::Receiver<()>,
    ) {
        let (mut connection, _) = listener.accept().unwrap();
        connection
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut ping_sender = Some(ping_sender);
        loop {
            let command = match read_command(&mut connection) {
                Ok(command) => command,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return;
                }
                Err(error) => panic!("fake Redis server failed to read a command: {error}"),
            };
            match command.first().map(String::as_str) {
                Some(command) if command.eq_ignore_ascii_case("PING") => {
                    ping_sender
                        .take()
                        .expect("Redis Scheduler must only issue one setup PING")
                        .send(())
                        .unwrap();
                    resume.recv().unwrap();
                    connection.write_all(b"+PONG\r\n").unwrap();
                }
                Some(name)
                    if name.eq_ignore_ascii_case("SCRIPT")
                        && command
                            .get(1)
                            .is_some_and(|action| action.eq_ignore_ascii_case("LOAD")) =>
                {
                    let hash = redis::Script::new(&command[2]).get_hash().to_string();
                    write_bulk(&mut connection, &hash);
                }
                _ => connection.write_all(b"+OK\r\n").unwrap(),
            }
            connection.flush().unwrap();
        }
    }

    fn read_command(connection: &mut TcpStream) -> io::Result<Vec<String>> {
        let count = read_line(connection)?;
        assert_eq!(count.first(), Some(&b'*'));
        let count = std::str::from_utf8(&count[1..])
            .unwrap()
            .parse::<usize>()
            .unwrap();
        (0..count)
            .map(|_| {
                let length = read_line(connection)?;
                assert_eq!(length.first(), Some(&b'$'));
                let length = std::str::from_utf8(&length[1..])
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                let mut value = vec![0; length];
                connection.read_exact(&mut value)?;
                let mut ending = [0; 2];
                connection.read_exact(&mut ending)?;
                assert_eq!(ending, *b"\r\n");
                Ok(String::from_utf8(value).unwrap())
            })
            .collect()
    }

    fn read_line(connection: &mut TcpStream) -> io::Result<Vec<u8>> {
        let mut value = Vec::new();
        loop {
            let mut byte = [0; 1];
            connection.read_exact(&mut byte)?;
            if byte[0] == b'\r' {
                let mut newline = [0; 1];
                connection.read_exact(&mut newline)?;
                assert_eq!(newline, [b'\n']);
                return Ok(value);
            }
            value.push(byte[0]);
        }
    }

    fn write_bulk(connection: &mut TcpStream, value: &str) {
        write!(connection, "${}\r\n{value}\r\n", value.len()).unwrap();
    }
}
