//! The public client. `Options::addresses` may name either a single
//! nanocached-node or discovery server(s) fronting a cluster —
//! `connect()` finds out from the server's own handshake response
//! (doc/adr/0007-*.md), so calling code is identical either way.
//!
//! Cluster mode implements ADR-0011 client-side replication: writes fan
//! out to each key's top-R owners (the primary's result decides; a dead
//! replica never fails a write), reads ask the primary and fall over to
//! the next owner only when the holder is unreachable. Dead connections
//! are redialed lazily on use (with one transparent retry — a Rust
//! socket only learns of a peer FIN on I/O, and every operation is
//! idempotent), and an opt-in keep-alive can hold connections open
//! across the server's 60s idle timeout.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Semaphore};

use crate::compression::resolve_compression;
use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::hash_ring::HashRing;
use crate::identify::{
    connect_and_identify, resolve_tls, split_host_port, Identified, TlsConfig, CONNECT_DEADLINE,
};
use crate::open_targets;

// How long the node list may go without a re-fetch from discovery before
// get/set/delete refreshes it first (checked lazily on use).
const NODE_LIST_STALE_AFTER: Duration = Duration::from_secs(30);
// The keep-alive ping: the server rejects empty keys, so it needs at
// least one byte; a single NUL stays out of any real key space.
const KEEPALIVE_KEY: &[u8] = &[0];
/// Internal keep-alive interval in milliseconds — see the comment at its
/// use in `connect`. Public-but-hidden purely as a test hook.
#[doc(hidden)]
pub static KEEPALIVE_INTERVAL_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(30_000);

/// doc/adr/0013-*.md: values shorter than this (bytes) are never
/// compressed — the per-value overhead of attempting it outweighs the
/// savings. Only meaningful when `compress(true)`.
const DEFAULT_COMPRESSION_THRESHOLD: usize = 256;

/// doc/adr/0014-*.md: bounds how many replica writes a single client may
/// have running in the background at once when `fire_and_forget_replicas`
/// is enabled — once the cap is reached, further replica legs fall back
/// to running synchronously, the same as with the option off. Read once
/// per `connect`; public-but-hidden purely as a test hook, mirroring
/// `KEEPALIVE_INTERVAL_MS`.
#[doc(hidden)]
pub static MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES: AtomicUsize = AtomicUsize::new(32);

/// Options for [`NanocachedClient::connect`].
pub struct Options {
    addresses: Vec<(String, u16)>,
    auth_secret: Option<String>,
    tls: bool,
    ca: Option<std::path::PathBuf>,
    compress: bool,
    compression_threshold: usize,
    fire_and_forget_replicas: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            addresses: Vec::new(),
            auth_secret: None,
            tls: false,
            ca: None,
            compress: false,
            compression_threshold: DEFAULT_COMPRESSION_THRESHOLD,
            fire_and_forget_replicas: false,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// The connect targets, tried in order for connect and every
    /// refresh: a single-node deployment is a one-element list, a
    /// cluster's discovery replicas (ADR-0010) a longer one.
    ///
    /// ```no_run
    /// # use nanocached::Options;
    /// let single = Options::new().addresses([("127.0.0.1", 8357)]);
    /// let replicas = Options::new().addresses([("10.0.0.1", 8357), ("10.0.0.2", 8357)]);
    /// ```
    pub fn addresses<I, H>(mut self, addrs: I) -> Self
    where
        I: IntoIterator<Item = (H, u16)>,
        H: Into<String>,
    {
        self.addresses = addrs
            .into_iter()
            .map(|(host, port)| (host.into(), port))
            .collect();
        self
    }

    /// Shared secret matching NANOCACHED_AUTH_SECRET on the server.
    pub fn auth_secret(mut self, secret: impl Into<String>) -> Self {
        self.auth_secret = Some(secret.into());
        self
    }

    /// Connect over TLS. Requires the `tls` feature (a default feature —
    /// disable it with `default-features = false` to opt out); without
    /// it, `tls(true)` fails at `connect()` time instead of failing to
    /// compile.
    pub fn tls(mut self, enabled: bool) -> Self {
        self.tls = enabled;
        self
    }

    /// A PEM file of trusted root certificate(s), replacing the platform
    /// trust store `tls(true)` verifies against by default. Meaningful
    /// only when `tls(true)`; silently ignored otherwise. An
    /// unreadable/unparseable file is a `connect()`-time error.
    pub fn ca(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.ca = Some(path.into());
        self
    }

    /// Transparently compress values above [`Self::compression_threshold`]
    /// on `set` and decompress them on `get`/`get_bytes`
    /// (doc/adr/0013-*.md). Off by default. Requires the `compression`
    /// feature (a default feature — disable it with `default-features =
    /// false` to opt out); without it, `compress(true)` fails at
    /// `connect()` time instead of failing to compile. **Every client
    /// that reads or writes a given set of keys must agree on this
    /// setting** — it is a per-keyspace format decision, not a
    /// per-client preference; see the ADR's Consequences before enabling
    /// this against an existing keyspace another client may still touch
    /// with `compress` off.
    pub fn compress(mut self, enabled: bool) -> Self {
        self.compress = enabled;
        self
    }

    /// Values shorter than this (in bytes) are never compressed — the
    /// per-value overhead of attempting it outweighs the savings. Only
    /// meaningful when [`Self::compress`] is enabled. Default 256.
    pub fn compression_threshold(mut self, bytes: usize) -> Self {
        self.compression_threshold = bytes;
        self
    }

    /// Let `set`/`delete` return as soon as the primary owner acks,
    /// letting replica legs finish in the background instead of waiting
    /// for them too (doc/adr/0014-*.md). Off by default. Unlike
    /// [`Self::compress`], this is a pure latency/durability trade for
    /// this client's own writes — it carries no wire format and needs no
    /// agreement with other clients.
    pub fn fire_and_forget_replicas(mut self, enabled: bool) -> Self {
        self.fire_and_forget_replicas = enabled;
        self
    }
}

struct Member {
    address: String,
    connection: Arc<Connection>,
}

/// What `write` should replay for a replica leg that ends up running
/// detached (doc/adr/0014-*.md) — the synchronous path keeps using the
/// borrowed `op` closure unchanged; this only exists to let a background
/// `tokio::spawn` task own its own copy of the data, since `op` typically
/// borrows from the caller's stack frame (see `set`/`delete`).
enum WriteBody<'a> {
    Set { value: &'a [u8], ttl_seconds: u64 },
    Delete,
}

impl WriteBody<'_> {
    fn to_owned(&self) -> OwnedWriteBody {
        match self {
            WriteBody::Set { value, ttl_seconds } => OwnedWriteBody::Set {
                value: value.to_vec(),
                ttl_seconds: *ttl_seconds,
            },
            WriteBody::Delete => OwnedWriteBody::Delete,
        }
    }
}

enum OwnedWriteBody {
    Set { value: Vec<u8>, ttl_seconds: u64 },
    Delete,
}

enum Target {
    Single {
        address: String,
        connection: Arc<Connection>,
    },
    Cluster {
        ring: HashRing,
        members: HashMap<String, Member>,
        replication: usize,
    },
}

struct Inner {
    state: Mutex<State>,
    redials: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    addresses: Vec<(String, u16)>,
    auth_secret: Option<String>,
    tls: Option<TlsConfig>,
    /// The address that answered `connect()` ("host:port") — every socket
    /// this client ever opens is counted against this one open-targets
    /// key, whichever node it actually dials (mirrors the TypeScript
    /// SDK's `this.url`).
    tracking_key: String,
    closed: AtomicBool,
    compress: bool,
    compression_threshold: usize,
    fire_and_forget_replicas: bool,
    /// doc/adr/0014-*.md: bounds in-flight background replica writes.
    /// Also close()'s drain primitive — acquiring every permit blocks
    /// until every currently in-flight background write has released
    /// its own, i.e. finished.
    background_replica_permits: Arc<Semaphore>,
    background_replica_cap: usize,
}

impl Inner {
    fn auth_secret_bytes(&self) -> Option<&[u8]> {
        self.auth_secret.as_deref().map(str::as_bytes)
    }
}

struct State {
    target: Target,
    last_fetch: Instant,
}

fn close_all_connections(target: &Target) {
    match target {
        Target::Single { connection, .. } => connection.close(),
        Target::Cluster { members, .. } => {
            for member in members.values() {
                member.connection.close();
            }
        }
    }
}

/// A cheaply cloneable handle; all clones share one set of connections.
#[derive(Clone)]
pub struct NanocachedClient {
    inner: Arc<Inner>,
    keepalive: Option<Arc<tokio::task::JoinHandle<()>>>,
}

impl NanocachedClient {
    pub async fn connect(options: Options) -> Result<Self> {
        if options.addresses.is_empty() {
            return Err(Error::InvalidArgument(
                "nanocached: connect() needs a non-empty addresses list".to_string(),
            ));
        }

        let tls = resolve_tls(options.tls, options.ca.as_deref())?;
        let compress = resolve_compression(options.compress)?;
        let auth_secret = options.auth_secret.as_deref().map(str::as_bytes);

        // Walk the addresses until one yields a working target; an
        // address that is unreachable, warming up (`B`, ADR-0010), or
        // knows no live nodes is skipped — the next replica may do
        // better.
        let mut last_error: Option<Error> = None;
        let mut target: Option<Target> = None;
        let mut tracking_key = String::new();

        for (host, port) in &options.addresses {
            let key = format!("{host}:{port}");

            // Only meaningful for a single explicit target: with an
            // addresses list, another client instance legitimately
            // holding connections to the same address makes this
            // heuristic false-positive (issue #12).
            if options.addresses.len() == 1 && open_targets::has_open(&key) {
                eprintln!(
                    "nanocached: connect() called for {key} while a previous connection to it is \
                     still open — was close() forgotten?"
                );
            }

            match connect_and_identify(host, *port, auth_secret, tls.as_ref(), CONNECT_DEADLINE)
                .await
            {
                Err(error) => last_error = Some(error),
                Ok(Identified::Node(stream)) => {
                    if options.addresses.len() > 1 {
                        let remaining = options.addresses.len() - 1;
                        eprintln!(
                            "nanocached: {key} is a cache node, so this client is pinned to that \
                             single server — the {remaining} remaining address(es) will not be \
                             used. Point addresses at discovery servers for cluster routing and \
                             failover."
                        );
                    }
                    target = Some(Target::Single {
                        address: key.clone(),
                        connection: Arc::new(Connection::new(stream, key.clone())),
                    });
                    tracking_key = key;
                    break;
                }
                Ok(Identified::Cluster { nodes, replication }) => {
                    if nodes.is_empty() {
                        last_error = Some(Error::Protocol(format!(
                            "nanocached: no live nodes registered with the discovery server at {key}"
                        )));
                        continue;
                    }

                    let mut members = HashMap::new();
                    let mut dial_error = None;
                    for node in &nodes {
                        let outcome: Result<_> = async {
                            let (node_host, node_port) = split_host_port(&node.address)?;
                            let identified = connect_and_identify(
                                &node_host,
                                node_port,
                                auth_secret,
                                tls.as_ref(),
                                CONNECT_DEADLINE,
                            )
                            .await?;
                            match identified {
                                Identified::Node(stream) => Ok(stream),
                                Identified::Cluster { .. } => Err(Error::Protocol(format!(
                                    "nanocached: discovery server returned a non-node address: {}",
                                    node.address
                                ))),
                            }
                        }
                        .await;

                        match outcome {
                            Ok(stream) => {
                                members.insert(
                                    node.name.clone(),
                                    Member {
                                        address: node.address.clone(),
                                        connection: Arc::new(Connection::new(stream, key.clone())),
                                    },
                                );
                            }
                            Err(error) => {
                                dial_error = Some(error);
                                break;
                            }
                        }
                    }
                    if let Some(error) = dial_error {
                        // A node (not the discovery address) is the
                        // problem here; another address would hand back
                        // the same node list, so don't try one — but
                        // close whatever members already connected so
                        // they aren't leaked (and stay counted forever in
                        // open_targets).
                        for member in members.values() {
                            member.connection.close();
                        }
                        return Err(error);
                    }

                    target = Some(Target::Cluster {
                        ring: HashRing::new(nodes.iter().map(|node| node.name.clone()).collect()),
                        members,
                        replication,
                    });
                    tracking_key = key;
                    break;
                }
            }
        }

        let Some(target) = target else {
            return Err(last_error.unwrap_or_else(|| {
                Error::ConnectionLost("nanocached: could not connect to any address".to_string())
            }));
        };

        let background_replica_cap = MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES.load(Ordering::SeqCst);
        let inner = Arc::new(Inner {
            state: Mutex::new(State {
                target,
                last_fetch: Instant::now(),
            }),
            redials: Mutex::new(HashMap::new()),
            addresses: options.addresses,
            auth_secret: options.auth_secret,
            tls,
            tracking_key,
            closed: AtomicBool::new(false),
            compress,
            compression_threshold: options.compression_threshold,
            fire_and_forget_replicas: options.fire_and_forget_replicas,
            background_replica_permits: Arc::new(Semaphore::new(background_replica_cap)),
            background_replica_cap,
        });

        // Keep-alive is always on, with an internal interval (issue #27):
        // half the server's 60s idle timeout, so it never severs a healthy
        // client. Read once per connect; the static exists only so tests
        // can shorten it.
        let interval =
            Duration::from_millis(KEEPALIVE_INTERVAL_MS.load(std::sync::atomic::Ordering::SeqCst));
        let keepalive = Some({
            let inner = Arc::clone(&inner);
            Arc::new(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if inner.closed.load(Ordering::SeqCst) {
                        return;
                    }
                    let connections: Vec<Arc<Connection>> = {
                        let state = inner.state.lock().await;
                        match &state.target {
                            Target::Single { connection, .. } => vec![Arc::clone(connection)],
                            Target::Cluster { members, .. } => members
                                .values()
                                .map(|member| Arc::clone(&member.connection))
                                .collect(),
                        }
                    };
                    for connection in connections {
                        if connection.is_closed() || connection.idle() < interval {
                            continue; // dead ones stay lazy; busy ones don't need a ping
                        }
                        // Any parseable reply proves liveness — `N`, or `W`
                        // from a non-owner — and resets the idle timer.
                        let _ = connection.get(KEEPALIVE_KEY).await;
                    }
                }
            }))
        });

        Ok(Self { inner, keepalive })
    }

    /// How many nodes hold each key (ADR-0011) — 1 against a single node.
    pub async fn replication(&self) -> usize {
        match &self.inner.state.lock().await.target {
            Target::Single { .. } => 1,
            Target::Cluster { replication, .. } => *replication,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Idempotent — but a second call warns (stderr), since it's usually
    /// a sign the caller lost track of this instance's lifecycle.
    pub fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            eprintln!("nanocached: close() called again on an already-closed client");
            return;
        }
        if let Some(keepalive) = &self.keepalive {
            keepalive.abort();
        }

        // doc/adr/0014-*.md: give background replica writes a chance to
        // finish before their connections are torn out from under them.
        // Every in-flight background write holds one permit and releases
        // it on completion, so acquiring all of them blocks until every
        // one has finished — bounded by background_replica_cap, so this
        // is a short wait in practice. Skipped entirely when nothing is
        // in flight (the common case), which also keeps close() on its
        // existing fast, non-blocking path then.
        if self.inner.background_replica_permits.available_permits()
            < self.inner.background_replica_cap
        {
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let _ = Arc::clone(&inner.background_replica_permits)
                    .acquire_many_owned(inner.background_replica_cap as u32)
                    .await;
                let state = inner.state.lock().await;
                close_all_connections(&state.target);
            });
            return;
        }

        // Close every connection now rather than waiting for the last
        // `NanocachedClient` clone (and so `Inner`) to drop, both to
        // release the sockets promptly and to keep open_targets accurate
        // (see Connection::close). `state` is a tokio::sync::Mutex, so a
        // request that's mid-flight (holding it only long enough to clone
        // an `Arc<Connection>` out — see `slot_connection`) could very
        // briefly contend it; rather than block this synchronous method,
        // fall back to closing them once it's free, same as the native
        // socket "close" event the TypeScript SDK relies on landing on a
        // later tick.
        match self.inner.state.try_lock() {
            Ok(state) => close_all_connections(&state.target),
            Err(_) => {
                let inner = Arc::clone(&self.inner);
                tokio::spawn(async move {
                    let state = inner.state.lock().await;
                    close_all_connections(&state.target);
                });
            }
        }
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(Error::InvalidUtf8)?)),
            None => Ok(None),
        }
    }

    /// Transparently decompresses when `compress` is enabled
    /// (doc/adr/0013-*.md).
    pub async fn get_bytes(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        self.before_operation().await?;
        let value = self
            .with_cluster_retry(|| {
                self.read(key, |connection| async move { connection.get(key).await })
            })
            .await?;
        match value {
            Some(bytes) if self.inner.compress => {
                Ok(Some(crate::compression::decompress_value(&bytes)?))
            }
            other => Ok(other),
        }
    }

    /// `ttl_seconds == 0` means no expiry. Transparently compresses
    /// values at or above `compression_threshold` when `compress` is
    /// enabled (doc/adr/0013-*.md).
    pub async fn set(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<()> {
        let key = key.as_ref();
        let owned_compressed;
        let value: &[u8] = if self.inner.compress {
            owned_compressed = crate::compression::compress_value(
                value.as_ref(),
                self.inner.compression_threshold,
            );
            &owned_compressed
        } else {
            value.as_ref()
        };
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(
                key,
                WriteBody::Set { value, ttl_seconds },
                move |connection| async move { connection.set(key, value, ttl_seconds).await },
            )
        })
        .await
    }

    /// Returns whether the key existed before this call.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(key, WriteBody::Delete, |connection| async move {
                connection.delete(key).await
            })
        })
        .await
    }

    async fn before_operation(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::AlreadyClosed);
        }
        self.maybe_refresh(false).await;
        Ok(())
    }

    /// Runs the operation; on a `W` answer (stale routing) or a
    /// connection-level failure that exhausted the current ranking (e.g.
    /// the key's primary died), forces a node-list refresh and retries
    /// the whole operation once against the fresh ranking. The retry
    /// window for a dead node is therefore bounded by discovery's
    /// liveness timeout. A second failure after a fresh refresh
    /// propagates.
    async fn with_cluster_retry<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match operation().await {
            Ok(value) => Ok(value),
            Err(error @ (Error::WrongNode | Error::ConnectionLost(_))) => {
                let clustered =
                    matches!(self.inner.state.lock().await.target, Target::Cluster { .. });
                if !clustered {
                    return Err(error);
                }
                self.maybe_refresh(true).await;
                operation().await
            }
            Err(error) => Err(error),
        }
    }

    fn owner_names(state: &State, key: &[u8]) -> Vec<String> {
        match &state.target {
            Target::Single { .. } => Vec::new(),
            Target::Cluster {
                ring, replication, ..
            } => ring
                .owners(key, *replication)
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }

    async fn read<T, F, Fut>(&self, key: &[u8], op: F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                return self.apply_reconnecting(None, &op).await;
            }
            Self::owner_names(&state, key)
        };

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        let mut last_error: Option<Error> = None;
        for name in owners {
            match self.apply_reconnecting(Some(&name), &op).await {
                Ok(value) => return Ok(value),
                Err(Error::WrongNode) => return Err(Error::WrongNode),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            Error::ConnectionLost("nanocached: no owner is reachable for this key".to_string())
        }))
    }

    async fn write<T, F, Fut>(&self, key: &[u8], body: WriteBody<'_>, op: F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                return self.apply_reconnecting(None, &op).await;
            }
            Self::owner_names(&state, key)
        };

        let Some((primary, replicas)) = owners.split_first() else {
            return Err(Error::ConnectionLost(
                "nanocached: no owner is reachable for this key".to_string(),
            ));
        };

        // Fan out to the replicas concurrently with the primary write. The
        // primary's outcome decides; replica failures are swallowed by
        // design (ADR-0011) — a dead or disagreeing replica leaves the key
        // under-replicated until the next node-list refresh, never fails
        // the write. doc/adr/0014-*.md: with fire_and_forget_replicas, up
        // to background_replica_cap legs run detached on their own tokio
        // task instead of being awaited below — past that cap, further
        // legs fall back to the synchronous path exactly as with the
        // option off.
        let replica_writes = async {
            for name in replicas {
                if self.inner.fire_and_forget_replicas {
                    if let Ok(permit) =
                        Arc::clone(&self.inner.background_replica_permits).try_acquire_owned()
                    {
                        let client = self.clone();
                        let name = name.clone();
                        let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
                        let owned_body = body.to_owned();
                        tokio::spawn(async move {
                            let _permit = permit; // held until this task finishes
                            match owned_body {
                                OwnedWriteBody::Set { value, ttl_seconds } => {
                                    let value: Arc<[u8]> = Arc::from(value);
                                    let op = move |connection: Arc<Connection>| {
                                        let key = Arc::clone(&owned_key);
                                        let value = Arc::clone(&value);
                                        async move { connection.set(&key, &value, ttl_seconds).await }
                                    };
                                    let _ = client.apply_reconnecting(Some(&name), &op).await;
                                }
                                OwnedWriteBody::Delete => {
                                    let op = move |connection: Arc<Connection>| {
                                        let key = Arc::clone(&owned_key);
                                        async move { connection.delete(&key).await }
                                    };
                                    let _ = client.apply_reconnecting(Some(&name), &op).await;
                                }
                            }
                        });
                        continue;
                    }
                }
                let _ = self.apply_reconnecting(Some(name), &op).await;
            }
        };
        let primary_write = self.apply_reconnecting(Some(primary), &op);

        let (primary_result, ()) = tokio::join!(primary_write, replica_writes);
        primary_result
    }

    /// Runs `op` against the slot's connection, retrying once on a
    /// connection-level failure: a Rust socket only learns of a peer FIN
    /// (e.g. the server's 60s idle timeout) on I/O, so lazy
    /// reconnect-on-use means the failed request poisons the connection,
    /// the redial replaces it, and the operation runs again. Safe because
    /// get/set/delete are all idempotent. `slot` is `None` in single mode.
    async fn apply_reconnecting<T, F, Fut>(&self, slot: Option<&str>, op: &F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match op(self.slot_connection(slot).await?).await {
            Err(Error::ConnectionLost(_)) => op(self.slot_connection(slot).await?).await,
            outcome => outcome,
        }
    }

    async fn slot_connection(&self, slot: Option<&str>) -> Result<Arc<Connection>> {
        let (slot_key, address, current) = {
            let state = self.inner.state.lock().await;
            match (&state.target, slot) {
                (
                    Target::Single {
                        address,
                        connection,
                    },
                    None,
                ) => (String::new(), address.clone(), Arc::clone(connection)),
                (Target::Cluster { members, .. }, Some(name)) => {
                    let Some(member) = members.get(name) else {
                        return Err(Error::ConnectionLost(format!(
                            "nanocached: {name} has no open connection"
                        )));
                    };
                    (
                        name.to_string(),
                        member.address.clone(),
                        Arc::clone(&member.connection),
                    )
                }
                _ => {
                    return Err(Error::Protocol(
                        "nanocached: internal error — slot/target mismatch".to_string(),
                    ))
                }
            }
        };

        if !current.is_closed() {
            return Ok(current);
        }

        // Concurrent requests finding the same dead connection share one
        // dial: the first task in redials, the rest wait then reuse.
        let slot_lock = {
            let mut redials = self.inner.redials.lock().await;
            Arc::clone(redials.entry(slot_key.clone()).or_default())
        };
        let _guard = slot_lock.lock().await;

        // Re-check under the slot lock — another task may have redialed.
        {
            let state = self.inner.state.lock().await;
            let existing = match (&state.target, slot) {
                (Target::Single { connection, .. }, None) => Some(Arc::clone(connection)),
                (Target::Cluster { members, .. }, Some(name)) => members
                    .get(name)
                    .map(|member| Arc::clone(&member.connection)),
                _ => None,
            };
            if let Some(existing) = existing {
                if !existing.is_closed() {
                    return Ok(existing);
                }
            }
        }

        let connection = Arc::new(Connection::new(
            self.open_node_stream(&address).await?,
            self.inner.tracking_key.clone(),
        ));

        let mut state = self.inner.state.lock().await;
        if self.inner.closed.load(Ordering::SeqCst) {
            // close() ran while we were dialing (issue #10): installing
            // this connection now would leak it past teardown.
            connection.close();
            return Err(Error::AlreadyClosed);
        }
        match (&mut state.target, slot) {
            (
                Target::Single {
                    connection: current,
                    ..
                },
                None,
            ) => {
                *current = Arc::clone(&connection);
            }
            (Target::Cluster { members, .. }, Some(name)) => {
                if let Some(member) = members.get_mut(name) {
                    member.connection = Arc::clone(&connection);
                } else {
                    // The refresh that dropped this member from the
                    // cluster already reconciled without this dial, so
                    // installing it now would leak the socket (and leave
                    // it counted forever in open_targets).
                    connection.close();
                    return Err(Error::ConnectionLost(format!(
                        "nanocached: {name} left the cluster while reconnecting"
                    )));
                }
            }
            _ => {}
        }
        Ok(connection)
    }

    async fn open_node_stream(&self, address: &str) -> Result<crate::identify::Stream> {
        let (host, port) = split_host_port(address)?;
        let identified = connect_and_identify(
            &host,
            port,
            self.inner.auth_secret_bytes(),
            self.inner.tls.as_ref(),
            CONNECT_DEADLINE,
        )
        .await?;
        match identified {
            Identified::Node(stream) => {
                if self.is_closed() {
                    return Err(Error::AlreadyClosed);
                }
                Ok(stream)
            }
            Identified::Cluster { .. } => Err(Error::Protocol(format!(
                "nanocached: {address} no longer identifies as a cache node"
            ))),
        }
    }

    async fn maybe_refresh(&self, force: bool) {
        {
            let state = self.inner.state.lock().await;
            if matches!(state.target, Target::Single { .. }) {
                return;
            }
            if !force && state.last_fetch.elapsed() < NODE_LIST_STALE_AFTER {
                return;
            }
        }
        self.refresh_node_list().await;
    }

    async fn refresh_node_list(&self) {
        let fetched = self.fetch_node_list().await;

        let mut state = self.inner.state.lock().await;
        state.last_fetch = Instant::now();
        let Some((nodes, replication)) = fetched else {
            return;
        };
        let Target::Cluster { members, .. } = &mut state.target else {
            return;
        };

        let mut fresh: HashMap<String, Member> = HashMap::new();
        for node in &nodes {
            if let Some(existing) = members.remove(&node.name) {
                fresh.insert(
                    node.name.clone(),
                    Member {
                        address: node.address.clone(),
                        connection: existing.connection,
                    },
                );
            }
        }
        // Nodes no longer listed: close their connections now — both to
        // release the sockets immediately and to keep open_targets
        // accurate (see Connection::close) — rather than waiting for
        // `members` to drop here. Newly listed nodes are dialed lazily on
        // first use (slot_connection), which keeps this refresh free of
        // network I/O under the lock.
        for member in members.values() {
            member.connection.close();
        }
        for node in &nodes {
            fresh.entry(node.name.clone()).or_insert_with(|| Member {
                address: node.address.clone(),
                connection: Arc::new(Connection::dead()),
            });
        }

        state.target = Target::Cluster {
            ring: HashRing::new(fresh.keys().cloned().collect()),
            members: fresh,
            replication,
        };
        drop(state);

        // Node names are per-process UUIDs; departed nodes' redial gates
        // would otherwise accumulate forever (issue #12).
        let live: std::collections::HashSet<String> =
            nodes.iter().map(|node| node.name.clone()).collect();
        let mut redials = self.inner.redials.lock().await;
        redials.retain(|slot, _| slot.is_empty() || live.contains(slot));
    }

    /// Walks every address (ADR-0010). Returns `None` — keep the
    /// last-known list — when none can provide one: unreachable, still
    /// inside its startup grace (`B`), no longer a discovery server, or
    /// knowing no live nodes. Silent by design (issue #12's noisy
    /// refresh-failure logging was removed): none of this changes
    /// behavior, so it isn't worth a warning on every stale check.
    async fn fetch_node_list(&self) -> Option<(Vec<crate::identify::DiscoveredNode>, usize)> {
        for (host, port) in &self.inner.addresses {
            if let Ok(Identified::Cluster { nodes, replication }) = connect_and_identify(
                host,
                *port,
                self.inner.auth_secret_bytes(),
                self.inner.tls.as_ref(),
                CONNECT_DEADLINE,
            )
            .await
            {
                if !nodes.is_empty() {
                    return Some((nodes, replication));
                }
            }
        }
        None
    }
}
