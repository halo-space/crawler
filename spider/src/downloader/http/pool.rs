use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::{downloader, net};

pub(super) const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub(super) struct Clients {
    pool: Mutex<Pool>,
}

impl Clients {
    pub(super) fn new() -> Self {
        Self {
            pool: Mutex::new(Pool::new(Instant::now())),
        }
    }

    pub(super) fn get(&self, request: &net::Request) -> Result<Handle, downloader::Error> {
        self.get_at(request, Instant::now())
    }

    fn get_at(&self, request: &net::Request, now: Instant) -> Result<Handle, downloader::Error> {
        let key = Key::from(request);
        let generation = {
            let mut pool = self.pool();
            if let Some(client) = pool.checkout(&key, now) {
                return Ok(client);
            }
            pool.generation
        };

        // Building outside the lock keeps unrelated proxy configurations independent.
        let client = Arc::new(Client::new(build(&key)?));
        Ok(self.pool().checkout_or_insert(key, client, generation, now))
    }

    pub(super) fn clear(&self) {
        self.pool().clear(Instant::now());
    }

    fn pool(&self) -> MutexGuard<'_, Pool> {
        self.pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for Clients {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct Key {
    proxy: Option<String>,
    accept_invalid_certs: bool,
}

impl From<&net::Request> for Key {
    fn from(request: &net::Request) -> Self {
        Self {
            proxy: request.proxy.as_ref().map(|proxy| proxy.url.clone()),
            accept_invalid_certs: request
                .tls
                .as_ref()
                .is_some_and(|tls| tls.accept_invalid_certs),
        }
    }
}

pub(super) struct Client {
    inner: reqwest::Client,
    state: Mutex<State>,
}

impl Client {
    pub(super) fn new(inner: reqwest::Client) -> Self {
        Self {
            inner,
            state: Mutex::new(State::default()),
        }
    }

    fn expired(&self, now: Instant) -> bool {
        let state = self.state();
        state.active == 0
            && state
                .idle_since
                .and_then(|idle_since| now.checked_duration_since(idle_since))
                .is_some_and(|idle| idle >= IDLE_TIMEOUT)
    }

    fn checkout(self: &Arc<Self>) -> Handle {
        let mut state = self.state();
        state.active = state
            .active
            .checked_add(1)
            .expect("active HTTP client count overflow");
        state.idle_since = None;
        drop(state);
        Handle {
            client: Arc::clone(self),
        }
    }

    fn release(&self) {
        let mut state = self.state();
        debug_assert!(state.active > 0, "released an inactive HTTP client");
        if state.active == 0 {
            return;
        }
        state.active -= 1;
        if state.active == 0 {
            state.idle_since = Some(Instant::now());
        }
    }

    pub(super) fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Default)]
pub(super) struct State {
    pub(super) active: usize,
    pub(super) idle_since: Option<Instant>,
}

pub(super) struct Handle {
    pub(super) client: Arc<Client>,
}

impl Deref for Handle {
    type Target = reqwest::Client;

    fn deref(&self) -> &Self::Target {
        &self.client.inner
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.client.release();
    }
}

struct Pool {
    clients: HashMap<Key, Arc<Client>>,
    next_cleanup: Instant,
    generation: u64,
}

impl Pool {
    fn new(now: Instant) -> Self {
        Self {
            clients: HashMap::new(),
            next_cleanup: next_cleanup(now),
            generation: 0,
        }
    }

    fn checkout(&mut self, key: &Key, now: Instant) -> Option<Handle> {
        self.cleanup(now);
        self.clients.get(key).map(Client::checkout)
    }

    fn checkout_or_insert(
        &mut self,
        key: Key,
        client: Arc<Client>,
        generation: u64,
        now: Instant,
    ) -> Handle {
        self.cleanup(now);
        if self.generation != generation {
            return client.checkout();
        }
        self.clients.entry(key).or_insert(client).checkout()
    }

    fn cleanup(&mut self, now: Instant) {
        if now < self.next_cleanup {
            return;
        }
        self.clients.retain(|_, client| !client.expired(now));
        self.next_cleanup = next_cleanup(now);
    }

    fn clear(&mut self, now: Instant) {
        self.clients.clear();
        self.next_cleanup = next_cleanup(now);
        self.generation = self.generation.wrapping_add(1);
    }
}

fn next_cleanup(now: Instant) -> Instant {
    now.checked_add(IDLE_TIMEOUT).unwrap_or(now)
}

pub(super) fn build(key: &Key) -> Result<reqwest::Client, downloader::Error> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(IDLE_TIMEOUT);

    if let Some(proxy) = &key.proxy {
        builder = builder.proxy(reqwest::Proxy::all(proxy)?);
    }
    builder = builder.danger_accept_invalid_certs(key.accept_invalid_certs);

    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn proxied(url: &str) -> net::Request {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.proxy = Some(net::ProxyConfig {
            url: url.to_string(),
        });
        request
    }

    #[test]
    fn reuses_the_same_key_and_isolates_different_proxies() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let other = proxied("http://127.0.0.1:8081");

        let first = clients.get(&request).unwrap();
        let second = clients.get(&request).unwrap();
        let other = clients.get(&other).unwrap();

        assert!(Arc::ptr_eq(&first.client, &second.client));
        assert!(!Arc::ptr_eq(&first.client, &other.client));
        assert_eq!(clients.pool().clients.len(), 2);
    }

    #[test]
    fn active_client_survives_cleanup_after_the_idle_timeout() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let now = Instant::now();

        let first = clients.get_at(&request, now).unwrap();
        let second = clients.get_at(&request, now + IDLE_TIMEOUT).unwrap();

        assert!(Arc::ptr_eq(&first.client, &second.client));
        assert_eq!(clients.pool().clients.len(), 1);
        assert_eq!(first.client.state().active, 2);
    }

    #[test]
    fn released_client_is_replaced_after_it_expires() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let trigger = proxied("http://127.0.0.1:8081");
        let now = Instant::now();
        let handle = clients.get_at(&request, now).unwrap();
        let expired = Arc::clone(&handle.client);

        drop(handle);
        assert_eq!(expired.state().active, 0);
        let idle_since = expired.state().idle_since.unwrap();

        let cleanup = idle_since + IDLE_TIMEOUT;
        let _trigger = clients.get_at(&trigger, cleanup).unwrap();
        assert!(!clients.pool().clients.contains_key(&Key::from(&request)));

        let replacement = clients.get_at(&request, cleanup).unwrap();
        assert!(!Arc::ptr_eq(&expired, &replacement.client));
    }

    #[test]
    fn clear_does_not_invalidate_checked_out_clients() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let client = clients.get(&request).unwrap();
        assert_eq!(clients.pool().clients.len(), 1);

        clients.clear();

        assert!(clients.pool().clients.is_empty());
        assert!(client.get("https://example.com").build().is_ok());

        let replacement = clients.get(&request).unwrap();
        assert!(!Arc::ptr_eq(&client.client, &replacement.client));
        drop(client);
        assert_eq!(replacement.client.state().active, 1);
    }

    #[test]
    fn stale_build_after_clear_is_not_inserted_into_the_new_generation() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let key = Key::from(&request);
        let now = Instant::now();
        let generation = {
            let mut pool = clients.pool();
            assert!(pool.checkout(&key, now).is_none());
            pool.generation
        };
        let stale = Arc::new(Client::new(build(&key).unwrap()));

        clients.clear();
        let handle =
            clients
                .pool()
                .checkout_or_insert(key.clone(), Arc::clone(&stale), generation, now);

        assert!(Arc::ptr_eq(&handle.client, &stale));
        assert!(clients.pool().clients.is_empty());
        assert!(handle.get("https://example.com").build().is_ok());

        let current = clients.get(&request).unwrap();
        assert!(!Arc::ptr_eq(&handle.client, &current.client));
        drop(handle);
        assert_eq!(current.client.state().active, 1);
        assert!(Arc::ptr_eq(
            clients.pool().clients.get(&key).unwrap(),
            &current.client
        ));
    }

    #[test]
    fn proxy_and_tls_are_both_part_of_the_key() {
        let clients = Clients::new();
        let request = proxied("http://127.0.0.1:8080");
        let other_proxy = proxied("http://127.0.0.1:8081");
        let mut insecure = request.clone();
        insecure.tls = Some(net::TlsConfig {
            accept_invalid_certs: true,
        });

        let client = clients.get(&request).unwrap();
        let proxy = clients.get(&other_proxy).unwrap();
        let tls = clients.get(&insecure).unwrap();

        assert!(!Arc::ptr_eq(&client.client, &proxy.client));
        assert!(!Arc::ptr_eq(&client.client, &tls.client));
        assert!(!Arc::ptr_eq(&proxy.client, &tls.client));
        assert_eq!(clients.pool().clients.len(), 3);
    }

    #[test]
    fn concurrent_checkout_of_the_same_key_uses_one_client() {
        const WORKERS: usize = 16;

        let clients = Arc::new(Clients::new());
        let barrier = Arc::new(Barrier::new(WORKERS));
        let threads = (0..WORKERS)
            .map(|_| {
                let clients = Arc::clone(&clients);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let request = proxied("http://127.0.0.1:8080");
                    barrier.wait();
                    clients.get(&request).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let handles = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(clients.pool().clients.len(), 1);
        assert!(
            handles
                .iter()
                .skip(1)
                .all(|handle| Arc::ptr_eq(&handles[0].client, &handle.client))
        );
    }
}
