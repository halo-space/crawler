use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::{downloader, net};

pub(super) const MAX_IDLE_CLIENTS: usize = 64;
pub(super) const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub(super) struct Clients {
    pool: Arc<Mutex<Pool>>,
}

impl Clients {
    pub(super) fn new() -> Self {
        Self {
            pool: Arc::new(Mutex::new(Pool::new(MAX_IDLE_CLIENTS))),
        }
    }

    pub(super) fn set_max_idle_clients(
        &self,
        max_idle_clients: usize,
    ) -> Result<(), downloader::Error> {
        if max_idle_clients == 0 {
            return Err(downloader::Error::InvalidConfig(
                "max idle HTTP client count must be positive".to_string(),
            ));
        }
        self.pool()
            .set_max_idle_clients(max_idle_clients, Instant::now());
        Ok(())
    }

    pub(super) fn get(&self, request: &net::Request) -> Result<Handle, downloader::Error> {
        self.get_at(request, Instant::now())
    }

    fn get_at(&self, request: &net::Request, now: Instant) -> Result<Handle, downloader::Error> {
        let key = Key::from(request);
        let generation = {
            let mut pool = self.pool();
            if let Some(client) = pool.checkout(&key, now) {
                return Ok(Handle::new(client, key, Arc::downgrade(&self.pool)));
            }
            pool.generation
        };

        // Building outside the lock keeps unrelated proxy configurations independent.
        let client = Arc::new(Client::new(build(&key)?));
        let client = self
            .pool()
            .checkout_or_insert(key.clone(), client, generation, now);
        Ok(Handle::new(client, key, Arc::downgrade(&self.pool)))
    }

    pub(super) fn clear(&self) {
        self.pool().clear();
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

    fn expires_at(&self) -> Option<Instant> {
        let state = self.state();
        if state.active > 0 {
            return None;
        }
        state.idle_since.and_then(idle_expiry)
    }

    fn expired(&self, now: Instant) -> bool {
        self.expires_at()
            .is_some_and(|expires_at| expires_at <= now)
    }

    fn checkout(&self) -> (bool, Option<Instant>) {
        let mut state = self.state();
        let was_idle = state.active == 0 && state.idle_since.is_some();
        let expires_at = state.idle_since.and_then(idle_expiry);
        state.active = state
            .active
            .checked_add(1)
            .expect("active HTTP client count overflow");
        state.idle_since = None;
        (was_idle, expires_at)
    }

    fn release(&self, now: Instant) -> bool {
        let mut state = self.state();
        debug_assert!(state.active > 0, "released an inactive HTTP client");
        if state.active == 0 {
            return false;
        }
        state.active -= 1;
        if state.active == 0 {
            state.idle_since = Some(now);
            return true;
        }
        false
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
    key: Key,
    pool: Option<std::sync::Weak<Mutex<Pool>>>,
}

impl Handle {
    fn new(client: Arc<Client>, key: Key, pool: std::sync::Weak<Mutex<Pool>>) -> Self {
        Self {
            client,
            key,
            pool: Some(pool),
        }
    }

    fn release(&mut self, now: Instant) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        let Some(pool) = pool.upgrade() else {
            self.client.release(now);
            return;
        };
        let mut pool = pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pool.release(&self.key, &self.client, now);
    }

    #[cfg(test)]
    fn release_at(mut self, now: Instant) {
        self.release(now);
    }
}

impl Deref for Handle {
    type Target = reqwest::Client;

    fn deref(&self) -> &Self::Target {
        &self.client.inner
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.release(Instant::now());
    }
}

struct Pool {
    clients: HashMap<Key, Arc<Client>>,
    idle_clients: usize,
    max_idle_clients: usize,
    next_expiry: Option<Instant>,
    generation: u64,
}

impl Pool {
    fn new(max_idle_clients: usize) -> Self {
        Self {
            clients: HashMap::new(),
            idle_clients: 0,
            max_idle_clients,
            next_expiry: None,
            generation: 0,
        }
    }

    fn checkout(&mut self, key: &Key, now: Instant) -> Option<Arc<Client>> {
        self.cleanup(now);
        let client = Arc::clone(self.clients.get(key)?);
        let (was_idle, expires_at) = client.checkout();
        if was_idle {
            debug_assert!(
                self.idle_clients > 0,
                "idle HTTP client count is inconsistent"
            );
            self.idle_clients -= 1;
            if self.next_expiry == expires_at {
                self.refresh_expiry();
            }
        }
        Some(client)
    }

    fn checkout_or_insert(
        &mut self,
        key: Key,
        client: Arc<Client>,
        generation: u64,
        now: Instant,
    ) -> Arc<Client> {
        self.cleanup(now);
        if self.generation != generation {
            let _ = client.checkout();
            return client;
        }
        let client = Arc::clone(self.clients.entry(key).or_insert(client));
        let (was_idle, expires_at) = client.checkout();
        if was_idle {
            debug_assert!(
                self.idle_clients > 0,
                "idle HTTP client count is inconsistent"
            );
            self.idle_clients -= 1;
            if self.next_expiry == expires_at {
                self.refresh_expiry();
            }
        }
        client
    }

    fn cleanup(&mut self, now: Instant) {
        if self.next_expiry.is_none_or(|next| now < next) {
            return;
        }
        let mut removed = 0;
        self.clients.retain(|_, client| {
            let retain = !client.expired(now);
            removed += usize::from(!retain);
            retain
        });
        debug_assert!(
            removed <= self.idle_clients,
            "idle HTTP client count is inconsistent"
        );
        self.idle_clients -= removed;
        self.refresh_expiry();
    }

    fn release(&mut self, key: &Key, client: &Arc<Client>, now: Instant) {
        let pooled = self
            .clients
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, client));
        let became_idle = client.release(now);
        if !pooled || !became_idle {
            return;
        }
        self.idle_clients = self
            .idle_clients
            .checked_add(1)
            .expect("idle HTTP client count overflow");
        self.next_expiry = earliest(self.next_expiry, idle_expiry(now));
        self.cleanup(now);
        self.enforce_max_idle();
    }

    fn set_max_idle_clients(&mut self, max_idle_clients: usize, now: Instant) {
        self.max_idle_clients = max_idle_clients;
        self.cleanup(now);
        self.enforce_max_idle();
    }

    fn enforce_max_idle(&mut self) {
        let mut evicted = false;
        while self.idle_clients > self.max_idle_clients {
            let key = self
                .clients
                .iter()
                .filter_map(|(key, client)| {
                    client
                        .state()
                        .idle_since
                        .map(|idle_since| (key.clone(), idle_since))
                })
                .min_by_key(|(_, idle_since)| *idle_since)
                .map(|(key, _)| key)
                .expect("idle HTTP client count is inconsistent");
            self.clients.remove(&key);
            self.idle_clients -= 1;
            evicted = true;
        }
        if evicted {
            self.refresh_expiry();
        }
    }

    fn refresh_expiry(&mut self) {
        self.next_expiry = self
            .clients
            .values()
            .filter_map(|client| client.expires_at())
            .min();
    }

    fn clear(&mut self) {
        self.clients.clear();
        self.idle_clients = 0;
        self.next_expiry = None;
        self.generation = self.generation.wrapping_add(1);
    }
}

fn idle_expiry(now: Instant) -> Option<Instant> {
    now.checked_add(IDLE_TIMEOUT)
}

fn earliest(left: Option<Instant>, right: Option<Instant>) -> Option<Instant> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
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

        handle.release_at(now);
        assert_eq!(expired.state().active, 0);
        let cleanup = now + IDLE_TIMEOUT;
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
        let client =
            clients
                .pool()
                .checkout_or_insert(key.clone(), Arc::clone(&stale), generation, now);
        let handle = Handle::new(client, key.clone(), Arc::downgrade(&clients.pool));

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
    fn defaults_to_sixty_four_idle_clients() {
        let clients = Clients::new();

        assert_eq!(clients.pool().max_idle_clients, MAX_IDLE_CLIENTS);
    }

    #[test]
    fn http_builder_replaces_the_idle_limit() {
        let http = super::super::Http::new().with_max_idle_clients(3).unwrap();

        assert_eq!(http.clients.pool().max_idle_clients, 3);
    }

    #[test]
    fn rejects_a_zero_idle_limit() {
        assert!(matches!(
            Clients::new().set_max_idle_clients(0),
            Err(downloader::Error::InvalidConfig(_))
        ));
    }

    #[test]
    fn evicts_the_oldest_idle_client_when_the_limit_is_exceeded() {
        let clients = Clients::new();
        clients.set_max_idle_clients(2).unwrap();
        let first_request = proxied("http://127.0.0.1:8080");
        let second_request = proxied("http://127.0.0.1:8081");
        let third_request = proxied("http://127.0.0.1:8082");
        let now = Instant::now();
        let first = clients.get_at(&first_request, now).unwrap();
        let second = clients.get_at(&second_request, now).unwrap();
        let third = clients.get_at(&third_request, now).unwrap();

        first.release_at(now);
        second.release_at(now + Duration::from_secs(1));
        third.release_at(now + Duration::from_secs(2));

        let pool = clients.pool();
        assert_eq!(pool.idle_clients, 2);
        assert_eq!(pool.clients.len(), 2);
        assert!(!pool.clients.contains_key(&Key::from(&first_request)));
        assert!(pool.clients.contains_key(&Key::from(&second_request)));
        assert!(pool.clients.contains_key(&Key::from(&third_request)));
    }

    #[test]
    fn active_clients_survive_capacity_pressure_until_they_become_idle() {
        let clients = Clients::new();
        clients.set_max_idle_clients(1).unwrap();
        let first_request = proxied("http://127.0.0.1:8080");
        let second_request = proxied("http://127.0.0.1:8081");
        let now = Instant::now();
        let first = clients.get_at(&first_request, now).unwrap();
        let second = clients.get_at(&second_request, now).unwrap();

        first.release_at(now);
        {
            let pool = clients.pool();
            assert_eq!(pool.clients.len(), 2);
            assert!(pool.clients.contains_key(&Key::from(&second_request)));
        }

        second.release_at(now + Duration::from_secs(1));
        let pool = clients.pool();
        assert_eq!(pool.idle_clients, 1);
        assert_eq!(pool.clients.len(), 1);
        assert!(!pool.clients.contains_key(&Key::from(&first_request)));
        assert!(pool.clients.contains_key(&Key::from(&second_request)));
    }

    #[test]
    fn cleanup_tracks_the_earliest_idle_expiry() {
        let clients = Clients::new();
        let first_request = proxied("http://127.0.0.1:8080");
        let second_request = proxied("http://127.0.0.1:8081");
        let trigger = proxied("http://127.0.0.1:8082");
        let now = Instant::now();
        let first = clients.get_at(&first_request, now).unwrap();
        let second = clients.get_at(&second_request, now).unwrap();

        first.release_at(now);
        second.release_at(now + Duration::from_secs(10));
        assert_eq!(clients.pool().next_expiry, Some(now + IDLE_TIMEOUT));

        let first = clients
            .get_at(&first_request, now + Duration::from_secs(20))
            .unwrap();
        assert_eq!(
            clients.pool().next_expiry,
            Some(now + Duration::from_secs(10) + IDLE_TIMEOUT)
        );

        let _trigger = clients
            .get_at(&trigger, now + Duration::from_secs(10) + IDLE_TIMEOUT)
            .unwrap();
        let pool = clients.pool();
        assert!(pool.clients.contains_key(&Key::from(&first_request)));
        assert!(!pool.clients.contains_key(&Key::from(&second_request)));
        drop(pool);
        drop(first);
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
