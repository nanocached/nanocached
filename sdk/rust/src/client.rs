//! The public client. A host/port (or a seeds list) may name either a
//! single nanocached-node or discovery server(s) fronting a cluster —
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
//! across the server's 30s idle timeout.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::hash_ring::HashRing;
use crate::identify::{connect_and_identify, split_host_port, Identified, TlsConfig};

// How long the node list may go without a re-fetch from discovery before
// get/set/delete refreshes it first (checked lazily on use).
const NODE_LIST_STALE_AFTER: Duration = Duration::from_secs(30);
// The keep-alive ping: the server rejects empty keys, so it needs at
// least one byte; a single NUL stays out of any real key space.
const KEEPALIVE_KEY: &[u8] = &[0];

/// Options for [`NanocachedClient::connect`].
#[derive(Default)]
pub struct Options {
    seeds: Vec<(String, u16)>,
    auth_secret: Option<Vec<u8>>,
    tls: Option<TlsConfig>,
    keep_alive_interval: Option<Duration>,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a target; call repeatedly to list discovery replicas
    /// (ADR-0010), tried in order for connect and every refresh.
    pub fn host(mut self, host: impl Into<String>, port: u16) -> Self {
        self.seeds.push((host.into(), port));
        self
    }

    /// Shared secret matching NANOCACHED_AUTH_SECRET on the server.
    pub fn auth_secret(mut self, secret: impl Into<Vec<u8>>) -> Self {
        self.auth_secret = Some(secret.into());
        self
    }

    /// Connect over TLS with this rustls client config (built by the
    /// caller: system roots, a private CA — their choice). Requires the
    /// `tls` feature.
    pub fn tls(mut self, config: TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    /// Opt-in keep-alive; pick something below the server's 30s idle timeout.
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self
    }
}

struct Member {
    address: String,
    connection: Arc<Connection>,
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
    seeds: Vec<(String, u16)>,
    auth_secret: Option<Vec<u8>>,
    tls: Option<TlsConfig>,
    closed: AtomicBool,
}

struct State {
    target: Target,
    last_fetch: Instant,
}

/// A cheaply cloneable handle; all clones share one set of connections.
#[derive(Clone)]
pub struct NanocachedClient {
    inner: Arc<Inner>,
    keepalive: Option<Arc<tokio::task::JoinHandle<()>>>,
}

impl NanocachedClient {
    pub async fn connect(options: Options) -> Result<Self> {
        if options.seeds.is_empty() {
            return Err(Error::InvalidArgument(
                "nanocached: connect() needs at least one host/port".to_string(),
            ));
        }
        if let Some(interval) = options.keep_alive_interval {
            if interval.is_zero() {
                return Err(Error::InvalidArgument(
                    "nanocached: keep_alive_interval must be positive".to_string(),
                ));
            }
        }

        // Walk the seeds until one yields a working target; a seed that is
        // unreachable, warming up (`B`, ADR-0010), or knows no live nodes
        // is skipped — the next replica may do better.
        let mut last_error: Option<Error> = None;
        let mut target: Option<Target> = None;

        for (host, port) in &options.seeds {
            match connect_and_identify(
                host,
                *port,
                options.auth_secret.as_deref(),
                options.tls.as_ref(),
            )
            .await
            {
                Err(error) => last_error = Some(error),
                Ok(Identified::Node(stream)) => {
                    if options.seeds.len() > 1 {
                        eprintln!(
                            "nanocached: {host}:{port} is a cache node, so this client is pinned to \
                             that single server — the remaining seed(s) will not be used. Point \
                             seeds at discovery servers for cluster routing and failover."
                        );
                    }
                    target = Some(Target::Single {
                        address: format!("{host}:{port}"),
                        connection: Arc::new(Connection::new(stream)),
                    });
                    break;
                }
                Ok(Identified::Cluster { nodes, replication }) => {
                    if nodes.is_empty() {
                        last_error = Some(Error::Protocol(format!(
                            "nanocached: no live nodes registered with the discovery server at {host}:{port}"
                        )));
                        continue;
                    }

                    let mut members = HashMap::new();
                    for node in &nodes {
                        let (node_host, node_port) = split_host_port(&node.address)?;
                        let identified = connect_and_identify(
                            &node_host,
                            node_port,
                            options.auth_secret.as_deref(),
                            options.tls.as_ref(),
                        )
                        .await?;
                        let Identified::Node(stream) = identified else {
                            return Err(Error::Protocol(format!(
                                "nanocached: discovery server returned a non-node address: {}",
                                node.address
                            )));
                        };
                        members.insert(
                            node.name.clone(),
                            Member {
                                address: node.address.clone(),
                                connection: Arc::new(Connection::new(stream)),
                            },
                        );
                    }

                    target = Some(Target::Cluster {
                        ring: HashRing::new(nodes.iter().map(|node| node.name.clone()).collect()),
                        members,
                        replication,
                    });
                    break;
                }
            }
        }

        let Some(target) = target else {
            return Err(last_error.unwrap_or_else(|| {
                Error::ConnectionLost("nanocached: could not connect to any seed".to_string())
            }));
        };

        let inner = Arc::new(Inner {
            state: Mutex::new(State {
                target,
                last_fetch: Instant::now(),
            }),
            redials: Mutex::new(HashMap::new()),
            seeds: options.seeds,
            auth_secret: options.auth_secret,
            tls: options.tls,
            closed: AtomicBool::new(false),
        });

        let keepalive = options.keep_alive_interval.map(|interval| {
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

    /// Idempotent; later get/set/delete return `Error::AlreadyClosed`.
    pub fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(keepalive) = &self.keepalive {
            keepalive.abort();
        }
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.read(key, |connection| async move { connection.get(key).await })
        })
        .await
    }

    pub async fn set(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let (key, value) = (key.as_ref(), value.as_ref());
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(key, move |connection| async move {
                connection.set(key, value, ttl_seconds).await
            })
        })
        .await
    }

    /// Returns whether the key existed before this call.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(
                key,
                |connection| async move { connection.delete(key).await },
            )
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

    async fn write<T, F, Fut>(&self, key: &[u8], op: F) -> Result<T>
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
        // the write.
        let replica_writes = async {
            for name in replicas {
                let _ = self.apply_reconnecting(Some(name), &op).await;
            }
        };
        let primary_write = self.apply_reconnecting(Some(primary), &op);

        let (primary_result, ()) = tokio::join!(primary_write, replica_writes);
        primary_result
    }

    /// Runs `op` against the slot's connection, retrying once on a
    /// connection-level failure: a Rust socket only learns of a peer FIN
    /// (e.g. the server's 30s idle timeout) on I/O, so lazy
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

        let connection = Arc::new(Connection::new(self.open_node_stream(&address).await?));

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
            self.inner.auth_secret.as_deref(),
            self.inner.tls.as_ref(),
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
        // Nodes no longer listed: their connections drop here. Newly
        // listed nodes are dialed lazily on first use (slot_connection),
        // which keeps this refresh free of network I/O under the lock.
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

    /// Walks every seed (ADR-0010); `None` means keep the last-known list.
    async fn fetch_node_list(&self) -> Option<(Vec<crate::identify::DiscoveredNode>, usize)> {
        for (host, port) in &self.inner.seeds {
            match connect_and_identify(
                host,
                *port,
                self.inner.auth_secret.as_deref(),
                self.inner.tls.as_ref(),
            )
            .await
            {
                Ok(Identified::Cluster { nodes, replication }) if !nodes.is_empty() => {
                    return Some((nodes, replication));
                }
                Ok(Identified::Cluster { .. }) => {
                    eprintln!(
                        "nanocached: discovery at {host}:{port} returned no live nodes, skipping"
                    );
                }
                Ok(Identified::Node(_)) => {
                    eprintln!(
                        "nanocached: {host}:{port} no longer identifies as a discovery server"
                    );
                }
                Err(error) => {
                    eprintln!(
                        "nanocached: could not refresh the node list from {host}:{port}: {error}"
                    );
                }
            }
        }
        eprintln!(
            "nanocached: no discovery seed could provide a node list, keeping the last-known list"
        );
        None
    }
}
