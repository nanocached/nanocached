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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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

/// How long, in milliseconds, the node list may go without a re-fetch from
/// discovery before get/set/delete refreshes it first (checked lazily on
/// use). Read fresh on every `maybe_refresh` call rather than once at
/// connect, mirroring `connection::REQUEST_TIMEOUT_MS` —
/// `#[doc(hidden)]` purely as a test hook so a single-flight-coalescing
/// test can shrink the staleness window instead of waiting out the real
/// 30s default; a test that lowers it should restore it immediately after
/// the one check it means to affect.
#[doc(hidden)]
pub static NODE_LIST_STALE_AFTER_MS: AtomicU64 = AtomicU64::new(30_000);
// The keep-alive ping key is reserved by the SDKs precisely so a real
// application key can never collide with it: a leading 0x00 already
// keeps it out of any UTF-8 key space, and "nanocached-keepalive" makes
// an accidental binary-key collision vanishingly unlikely too. Collision
// would matter because a `get` does refresh the server-side LRU recency
// of whatever key it names — colliding with a real key would silently
// keep that key artificially "hot" on every keep-alive tick.
const KEEPALIVE_KEY: &[u8] = b"\x00nanocached-keepalive";
/// The TTL a read-repair write applies to the primary (doc/adr/0015-*.md).
/// `get`'s response carries no TTL, so the key's original expiry is
/// unrecoverable; repairing with `ttl_seconds` 0 (no expiry) would make
/// an expiring key immortal, permanently resurrecting data the primary
/// had correctly let expire. 60s bounds the overshoot instead — a key
/// repaired past its true expiry simply gets re-repaired (or genuinely
/// found missing) on a later miss.
const READ_REPAIR_TTL: u64 = 60;
/// Internal keep-alive interval in milliseconds — see the comment at its
/// use in `connect`. Public-but-hidden purely as a test hook.
#[doc(hidden)]
pub static KEEPALIVE_INTERVAL_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(30_000);

/// doc/adr/0013-*.md: values shorter than this (bytes) are never
/// compressed — the per-value overhead of attempting it outweighs the
/// savings. Only meaningful when `compress(true)`.
const DEFAULT_COMPRESSION_THRESHOLD: usize = 256;

/// See [`Options::reconnect_cooldown`].
const DEFAULT_RECONNECT_COOLDOWN: Duration = Duration::from_secs(1);

/// The server's own request cap (src/server.rs's `MAX_REQUEST_SIZE`) is 1
/// MiB for the *entire* frame — header line plus key plus value; a
/// request over that limit is rejected by simply closing the connection
/// without a response (poisoning whatever else is pipelined behind it on
/// that same connection). This reserves 256 bytes of headroom for the
/// header itself (marker byte, decimal lengths, an optional TTL, ADR-0019's
/// tag field, spaces, the trailing newline — always comfortably under
/// this even for the largest fields), so a key+value that clears
/// `MAX_REQUEST_BYTES` is guaranteed to fit under the server's own cap and
/// never trips that connection-poisoning rejection (issue #47 audit item
/// R1; see README's "Errors" section).
const MAX_REQUEST_BYTES: usize = 1024 * 1024 - 256;

/// Rejects an empty key, or one that alone already exceeds
/// `MAX_REQUEST_BYTES`, before any network I/O: the server's protocol has
/// no way to represent a zero-length key request that doesn't collide
/// with other framing, and a key past the server's own 1 MiB request cap
/// can never be stored either way — both cases get exactly one reply from
/// the server: closing the connection outright, silently poisoning every
/// other request already pipelined on that connection (see
/// src/command.rs's `rejects_empty_key_for_get` et al., and this module's
/// `MAX_REQUEST_BYTES` doc comment). `get`/`delete` call this directly (no
/// value to bound), so without the size check here an oversized key on
/// either of those paths would sail straight past client-side validation
/// and only be caught by the server slamming the connection shut (issue
/// #47 audit item R1 follow-up). Catching both cases here client-side, as
/// `Error::InvalidArgument`, gives the caller a clear synchronous error
/// and avoids that blast radius entirely.
fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument(
            "nanocached: key must not be empty".to_string(),
        ));
    }
    if key.len() > MAX_REQUEST_BYTES {
        return Err(Error::InvalidArgument(format!(
            "nanocached: key exceeds MAX_REQUEST_BYTES ({MAX_REQUEST_BYTES} bytes), got {} bytes",
            key.len()
        )));
    }
    Ok(())
}

/// `validate_key` plus a `MAX_REQUEST_BYTES` bound on `key.len() +
/// value.len()` — anything past it can never fit the server's own 1 MiB
/// request cap, so failing fast here is strictly better than sending a
/// frame the server can only reject by silently closing the connection.
/// The combined check below is redundant whenever `validate_key` alone
/// already rejects an oversized key, but stays as its own check since a
/// key comfortably under the bound can still push the combined total over
/// it once `value` is added.
fn validate_key_and_value(key: &[u8], value: &[u8]) -> Result<()> {
    validate_key(key)?;
    if key.len() + value.len() > MAX_REQUEST_BYTES {
        return Err(Error::InvalidArgument(format!(
            "nanocached: key ({} bytes) + value ({} bytes) exceeds MAX_REQUEST_BYTES ({} bytes)",
            key.len(),
            value.len(),
            MAX_REQUEST_BYTES
        )));
    }
    Ok(())
}

/// doc/adr/0014-*.md: bounds how many replica writes a single client may
/// have running in the background at once when `fire_and_forget_replicas`
/// is enabled — once the cap is reached, further replica legs fall back
/// to running synchronously, the same as with the option off. Read once
/// per `connect`; public-but-hidden purely as a test hook, mirroring
/// `KEEPALIVE_INTERVAL_MS`.
#[doc(hidden)]
pub static MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES: AtomicUsize = AtomicUsize::new(32);

/// Monotonic counters for failures this SDK deliberately swallows
/// (ADR-0011/0014/0015) — observability for silently degrading
/// replication or a stuck node-list refresh that would otherwise have no
/// visible symptom until reads start missing more often than expected.
/// Returned by [`NanocachedClient::stats`]; never reset.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub replica_write_failures: u64,
    pub read_repair_failures: u64,
    pub refresh_failures: u64,
}

/// The live, atomically-updated counters [`NanocachedClient::stats`]
/// snapshots into a [`Stats`]; kept separate so the atomic types stay an
/// implementation detail of `Inner`.
#[derive(Default)]
struct StatsCounters {
    replica_write_failures: AtomicU64,
    read_repair_failures: AtomicU64,
    refresh_failures: AtomicU64,
}

/// Options for [`NanocachedClient::connect`].
pub struct Options {
    addresses: Vec<(String, u16)>,
    auth_secret: Option<String>,
    tls: bool,
    ca: Option<std::path::PathBuf>,
    compress: bool,
    compression_threshold: usize,
    fire_and_forget_replicas: bool,
    read_repair: bool,
    reconnect_cooldown: ReconnectCooldown,
}

/// `Options::reconnect_cooldown`'s intent, kept distinct from the
/// resolved [`Duration`] until [`ReconnectCooldown::resolve`]: unlike the
/// Go SDK, whose zero-value `Config` can't tell "not specified" apart
/// from "explicitly zero", this crate's builder can, so it uses a
/// three-way choice instead of overloading `Duration` (where zero would
/// otherwise be ambiguous between "use the default" and "disable it").
#[derive(Clone, Copy)]
enum ReconnectCooldown {
    Default,
    Explicit(Duration),
    Disabled,
}

impl ReconnectCooldown {
    /// `None` means disabled; `Some` is the cooldown to use.
    /// [`Duration::ZERO`] resolves to the default, matching the Go SDK's
    /// zero-value `Config.ReconnectCooldown`.
    fn resolve(self) -> Option<Duration> {
        match self {
            ReconnectCooldown::Default => Some(DEFAULT_RECONNECT_COOLDOWN),
            ReconnectCooldown::Explicit(duration) if duration.is_zero() => {
                Some(DEFAULT_RECONNECT_COOLDOWN)
            }
            ReconnectCooldown::Explicit(duration) => Some(duration),
            ReconnectCooldown::Disabled => None,
        }
    }
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
            read_repair: false,
            reconnect_cooldown: ReconnectCooldown::Default,
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

    /// Shared secret matching NANOCACHED_AUTH_SECRET on the server. An
    /// empty secret is the same as none, matching the other SDKs: sent
    /// literally, an empty string would reach the wire as an explicit
    /// zero-length secret, which the server rejects as EmptySecret and
    /// closes without replying — turning what should be "no auth
    /// configured" into an opaque `ConnectionLost`.
    pub fn auth_secret(mut self, secret: impl Into<String>) -> Self {
        let secret = secret.into();
        self.auth_secret = if secret.is_empty() {
            None
        } else {
            Some(secret)
        };
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

    /// On a clean miss (the key's first-reached owner reports it
    /// missing), probe the remaining owners before accepting that, and
    /// repair the primary in the background if one still has the value
    /// (doc/adr/0015-*.md). Off by default. Costs extra reads only on
    /// the misses this actually applies to.
    pub fn read_repair(mut self, enabled: bool) -> Self {
        self.read_repair = enabled;
        self
    }

    /// How long, after a reconnect dial to an address fails, that address
    /// is treated as still down — a request routed to it during this
    /// window fails immediately with the original dial error instead of
    /// paying another full `CONNECT_DEADLINE` (5s) redialing an address
    /// that just proved unreachable. Default 1 second. Keep it well under
    /// the 30-second node-list refresh interval so a node that genuinely
    /// recovers isn't shut out for long.
    ///
    /// [`Duration::ZERO`] means "use the default", not "disable it" —
    /// this matches the Go SDK, where a zero-value `Config` (the
    /// `ReconnectCooldown` field simply left unset) can't distinguish
    /// "not specified" from "explicitly zero", so zero has to mean
    /// "default" there. To disable the cooldown entirely — every request
    /// that finds the address's connection dead pays its own full dial
    /// attempt instead of reusing a cached failure — call
    /// [`Self::disable_reconnect_cooldown`] instead (the Go SDK's
    /// equivalent is a negative `Config.ReconnectCooldown`).
    pub fn reconnect_cooldown(mut self, duration: Duration) -> Self {
        self.reconnect_cooldown = ReconnectCooldown::Explicit(duration);
        self
    }

    /// Disables the per-address reconnect cooldown entirely: every
    /// request that finds an address's connection dead pays its own full
    /// dial attempt instead of reusing a cached failure. See
    /// [`Self::reconnect_cooldown`] for what the cooldown is; the Go
    /// SDK's equivalent of this method is a negative
    /// `Config.ReconnectCooldown`.
    pub fn disable_reconnect_cooldown(mut self) -> Self {
        self.reconnect_cooldown = ReconnectCooldown::Disabled;
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
    /// Single-flight gate for `maybe_refresh`/`refresh_node_list`: without
    /// it, every concurrent caller that observes a stale (or forced)
    /// node list independently redials discovery, so a burst of
    /// concurrent `WrongNode` replies (or requests that all land right as
    /// the list goes stale) can fan out into many redundant discovery
    /// round trips at once. Held only across the re-check-then-refresh
    /// sequence in `maybe_refresh` — never across `state`, and never
    /// across other I/O — mirroring Go's `Client.refreshMu` (sdk/go's
    /// `client.go`) and this struct's own `redials` gate for dialing.
    refresh_gate: Mutex<()>,
    /// Per-address reconnect cooldown (see [`Options::reconnect_cooldown`]):
    /// the address of the most recently failed dial, and how long it
    /// stays "down" before another dial to it is attempted. Keyed by
    /// address, not slot — a cluster refresh can reassign a slot (node
    /// name) to a different address, but the address itself is what's
    /// actually unreachable.
    reconnect_cooldowns: Mutex<HashMap<String, (Instant, Error)>>,
    /// Resolved from `Options::reconnect_cooldown`: `None` means
    /// disabled.
    reconnect_cooldown: Option<Duration>,
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
    read_repair: bool,
    stats: StatsCounters,
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

        let tls = resolve_tls(options.tls, options.ca.as_deref()).await?;
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
                Ok(Identified::Node { stream, tagged }) => {
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
                        connection: Arc::new(Connection::new(stream, key.clone(), tagged)),
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
                                Identified::Node { stream, tagged } => Ok((stream, tagged)),
                                Identified::Cluster { .. } => Err(Error::Protocol(format!(
                                    "nanocached: discovery server returned a non-node address: {}",
                                    node.address
                                ))),
                            }
                        }
                        .await;

                        match outcome {
                            Ok((stream, tagged)) => {
                                members.insert(
                                    node.name.clone(),
                                    Member {
                                        address: node.address.clone(),
                                        connection: Arc::new(Connection::new(
                                            stream,
                                            key.clone(),
                                            tagged,
                                        )),
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
            refresh_gate: Mutex::new(()),
            reconnect_cooldowns: Mutex::new(HashMap::new()),
            reconnect_cooldown: options.reconnect_cooldown.resolve(),
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
            read_repair: options.read_repair,
            stats: StatsCounters::default(),
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

    /// A snapshot of counters for failures this SDK swallows by design
    /// (ADR-0011/0014/0015) — lets operators detect silently degrading
    /// replication or a stuck node-list refresh.
    pub fn stats(&self) -> Stats {
        Stats {
            replica_write_failures: self
                .inner
                .stats
                .replica_write_failures
                .load(Ordering::Relaxed),
            read_repair_failures: self
                .inner
                .stats
                .read_repair_failures
                .load(Ordering::Relaxed),
            refresh_failures: self.inner.stats.refresh_failures.load(Ordering::Relaxed),
        }
    }

    /// Idempotent — but a second call warns (stderr), since it's usually
    /// a sign the caller lost track of this instance's lifecycle.
    ///
    /// Returns only after every in-flight background replica write has
    /// finished and the connections are torn down (doc/adr/0014-*.md as
    /// amended by issue #47 item 3 — the drain contract every SDK now
    /// shares); async since then, which is what lets it actually await
    /// that drain instead of handing teardown to a detached task.
    pub async fn close(&self) {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            eprintln!("nanocached: close() called again on an already-closed client");
            return;
        }
        if let Some(keepalive) = &self.keepalive {
            keepalive.abort();
        }

        // Every in-flight background write holds one permit and releases
        // it on completion, so acquiring all of them waits until every
        // one has finished — bounded by background_replica_cap, so this
        // is a short wait in practice. Skipped entirely when nothing is
        // in flight (the common case).
        if self.inner.background_replica_permits.available_permits()
            < self.inner.background_replica_cap
        {
            let _ = Arc::clone(&self.inner.background_replica_permits)
                .acquire_many_owned(self.inner.background_replica_cap as u32)
                .await;
        }

        // Close every connection now rather than waiting for the last
        // `NanocachedClient` clone (and so `Inner`) to drop, both to
        // release the sockets promptly and to keep open_targets accurate
        // (see Connection::close).
        let state = self.inner.state.lock().await;
        close_all_connections(&state.target);
    }

    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(String::from_utf8(bytes).map_err(Error::InvalidUtf8)?)),
            None => Ok(None),
        }
    }

    /// Transparently decompresses when `compress` is enabled
    /// (doc/adr/0013-*.md). With `read_repair`, a clean miss probes the
    /// remaining owners before being accepted as final (doc/adr/0015-*.md).
    pub async fn get_bytes(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        validate_key(key)?;
        self.before_operation().await?;
        let mut value = self
            .with_cluster_retry(|| {
                self.read(key, |connection| async move { connection.get(key).await })
            })
            .await?;
        if value.is_none() && self.inner.read_repair {
            let clustered = matches!(self.inner.state.lock().await.target, Target::Cluster { .. });
            if clustered {
                value = self.try_read_repair(key).await;
            }
        }
        match value {
            Some(bytes) if self.inner.compress => {
                Ok(Some(crate::compression::decompress_value(&bytes)?))
            }
            other => Ok(other),
        }
    }

    /// doc/adr/0015-*.md: probes the remaining owners of `key` — every
    /// owner but the primary, which the normal read path already probed
    /// and got a clean miss from — in rank order, for a value. The first
    /// one that has it wins: its value is returned, and — detached, not
    /// awaited, no tracking — that same value repairs the true primary in
    /// the background with `READ_REPAIR_TTL`. Every failure along the way
    /// (connection lost, WrongNode, another miss) is swallowed; nothing
    /// here may turn an already-accepted miss into an error. A failed
    /// repair write is counted in `stats().read_repair_failures`.
    async fn try_read_repair(&self, key: &[u8]) -> Option<Vec<u8>> {
        let owners = {
            let state = self.inner.state.lock().await;
            Self::owner_names(&state, key)
        };

        for name in owners.iter().skip(1) {
            let probe = |connection: Arc<Connection>| async move { connection.get(key).await };
            let Ok(Some(value)) = self.apply_reconnecting(Some(name), &probe).await else {
                continue;
            };

            if let Some(primary) = owners.first() {
                // Bounded and tracked exactly like a fire-and-forget replica
                // write (see `write`): the background repair holds one
                // `background_replica_permits` permit until it finishes, so
                // `close()`'s drain waits for it and no more than
                // `background_replica_cap` run at once. Past the cap the
                // repair for this miss is simply skipped — it's opportunistic
                // (ADR-0015), so a later miss repairs the key instead, and it
                // must never add latency or unbounded task growth to the read
                // path it rides on. The `closed` re-check after acquiring the
                // permit closes the same teardown race the replica path guards
                // against (issue #47 item 3).
                if let Ok(permit) =
                    Arc::clone(&self.inner.background_replica_permits).try_acquire_owned()
                {
                    if !self.inner.closed.load(Ordering::SeqCst) {
                        let client = self.clone();
                        let primary = primary.clone();
                        let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
                        let owned_value: Arc<[u8]> = Arc::from(value.clone());
                        tokio::spawn(async move {
                            let _permit = permit; // held until this task finishes
                            let op = move |connection: Arc<Connection>| {
                                let key = Arc::clone(&owned_key);
                                let value = Arc::clone(&owned_value);
                                async move { connection.set(&key, &value, READ_REPAIR_TTL).await }
                            };
                            if client
                                .apply_reconnecting(Some(&primary), &op)
                                .await
                                .is_err()
                            {
                                client
                                    .inner
                                    .stats
                                    .read_repair_failures
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        });
                    }
                }
            }
            return Some(value);
        }
        None
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
        validate_key(key)?;
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
        // Sized against what actually goes on the wire — the compressed
        // form when compression is on — like the other SDKs, so a large
        // but compressible value isn't refused for its raw size.
        validate_key_and_value(key, value)?;
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
        validate_key(key)?;
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
        // the write. Counted in `stats().replica_write_failures` so
        // operators can spot silently degrading replication.
        // doc/adr/0014-*.md: with fire_and_forget_replicas, up to
        // background_replica_cap legs run detached on their own tokio
        // task instead of being awaited below — past that cap, further
        // legs fall back to the synchronous path exactly as with the
        // option off.
        let replica_writes = async {
            for name in replicas {
                if self.inner.fire_and_forget_replicas {
                    if let Ok(permit) =
                        Arc::clone(&self.inner.background_replica_permits).try_acquire_owned()
                    {
                        // Re-check `closed` *after* taking the permit, the
                        // same ordering Go's SDK gets from re-checking under
                        // the lock `Close()` holds: `close()` sets `closed`
                        // before draining permits, so if we still see it
                        // clear here, `close()`'s drain is guaranteed to wait
                        // for this permit; if it's already set, `close()` may
                        // have passed its drain, so we must not spawn a
                        // detached task it won't await — fall back to the
                        // synchronous path (issue #47 item 3). SeqCst on both
                        // sides makes the permit acquisition and this load
                        // totally ordered against `close()`'s swap+drain.
                        if self.inner.closed.load(Ordering::SeqCst) {
                            drop(permit);
                        } else {
                            let client = self.clone();
                            let name = name.clone();
                            let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
                            let owned_body = body.to_owned();
                            tokio::spawn(async move {
                                let _permit = permit; // held until this task finishes
                                let failed = match owned_body {
                                    OwnedWriteBody::Set { value, ttl_seconds } => {
                                        let value: Arc<[u8]> = Arc::from(value);
                                        let op = move |connection: Arc<Connection>| {
                                            let key = Arc::clone(&owned_key);
                                            let value = Arc::clone(&value);
                                            async move {
                                                connection.set(&key, &value, ttl_seconds).await
                                            }
                                        };
                                        client.apply_reconnecting(Some(&name), &op).await.is_err()
                                    }
                                    OwnedWriteBody::Delete => {
                                        let op = move |connection: Arc<Connection>| {
                                            let key = Arc::clone(&owned_key);
                                            async move { connection.delete(&key).await }
                                        };
                                        client.apply_reconnecting(Some(&name), &op).await.is_err()
                                    }
                                };
                                if failed {
                                    client
                                        .inner
                                        .stats
                                        .replica_write_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                            });
                            continue;
                        }
                    }
                }
                if self.apply_reconnecting(Some(name), &op).await.is_err() {
                    self.inner
                        .stats
                        .replica_write_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
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

        // Per-address reconnect cooldown (see `Inner::reconnect_cooldowns`'
        // own doc comment): an address whose dial just failed stays "down"
        // for `reconnect_cooldown`, so a burst of requests routed to it —
        // or one request every keep-alive tick — fails immediately with
        // the same error the dial itself produced, instead of each paying
        // another full `CONNECT_DEADLINE` in turn.
        {
            let cooldowns = self.inner.reconnect_cooldowns.lock().await;
            if let Some((until, error)) = cooldowns.get(&address) {
                if Instant::now() < *until {
                    return Err(error.clone());
                }
            }
        }

        let dial_result = self.open_node_stream(&address).await;
        let (stream, tagged) = match dial_result {
            Ok(v) => {
                let mut cooldowns = self.inner.reconnect_cooldowns.lock().await;
                cooldowns.remove(&address);
                v
            }
            Err(error) => {
                if let Some(cooldown) = self.inner.reconnect_cooldown {
                    let mut cooldowns = self.inner.reconnect_cooldowns.lock().await;
                    cooldowns.insert(address.clone(), (Instant::now() + cooldown, error.clone()));
                }
                return Err(error);
            }
        };
        let connection = Arc::new(Connection::new(
            stream,
            self.inner.tracking_key.clone(),
            tagged,
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

    async fn open_node_stream(&self, address: &str) -> Result<(crate::identify::Stream, bool)> {
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
            Identified::Node { stream, tagged } => {
                if self.is_closed() {
                    return Err(Error::AlreadyClosed);
                }
                Ok((stream, tagged))
            }
            Identified::Cluster { .. } => Err(Error::Protocol(format!(
                "nanocached: {address} no longer identifies as a cache node"
            ))),
        }
    }

    /// Two-phase check-then-refresh, mirroring Go's `Client.maybeRefresh`:
    /// the first check (under `state` alone) cheaply short-circuits the
    /// common case of a fresh list without ever touching `refresh_gate`.
    /// Once a caller decides a refresh is needed, it queues on
    /// `refresh_gate` rather than dialing immediately — and, critically,
    /// re-checks staleness under `state` again *after* acquiring the gate,
    /// since a concurrent caller may have already refreshed while this one
    /// was waiting. Without that re-check, N callers that all observed
    /// staleness at once would simply serialize N redundant discovery
    /// round trips instead of coalescing into one. Only one lock is ever
    /// held at a time — `state` is always dropped before awaiting
    /// `refresh_gate` or any I/O, matching every other lock in this file.
    async fn maybe_refresh(&self, force: bool) {
        {
            let state = self.inner.state.lock().await;
            if matches!(state.target, Target::Single { .. }) {
                return;
            }
            if !force
                && state.last_fetch.elapsed()
                    < Duration::from_millis(NODE_LIST_STALE_AFTER_MS.load(Ordering::SeqCst))
            {
                return;
            }
        }

        let _gate = self.inner.refresh_gate.lock().await;
        {
            let state = self.inner.state.lock().await;
            if !force
                && state.last_fetch.elapsed()
                    < Duration::from_millis(NODE_LIST_STALE_AFTER_MS.load(Ordering::SeqCst))
            {
                // Someone else refreshed while we were waiting for the gate.
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
            self.inner
                .stats
                .refresh_failures
                .fetch_add(1, Ordering::Relaxed);
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
    /// knowing no live nodes. Silent by design: this path's noisy
    /// refresh-failure logging was removed by the #25/#27 API-unification
    /// work (unlike the redial-gate pruning below, which is issue #12's),
    /// since none of this changes behavior and isn't worth a warning on
    /// every stale check — the caller counts it in
    /// `stats().refresh_failures` instead.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_key_rejects_a_key_over_max_request_bytes() {
        // A key alone past MAX_REQUEST_BYTES can never fit the server's
        // own request cap, so `validate_key` — called directly by both
        // `get_bytes` and `delete`, not just via `validate_key_and_value`
        // — must reject it on its own, not just an empty key (issue #47
        // audit item R1 follow-up).
        let oversized = vec![0u8; MAX_REQUEST_BYTES + 1];
        assert!(matches!(
            validate_key(&oversized),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn validate_key_accepts_a_key_right_at_max_request_bytes() {
        let boundary = vec![0u8; MAX_REQUEST_BYTES];
        assert!(validate_key(&boundary).is_ok());
    }

    #[test]
    fn validate_key_rejects_an_empty_key() {
        assert!(matches!(validate_key(b""), Err(Error::InvalidArgument(_))));
    }
}
