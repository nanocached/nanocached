//! The public client. `Options::addresses` may name either a single
//! nanocached-node or discovery server(s) fronting a cluster —
//! `connect()` finds out from the server's own handshake response
//! (the server type in the auth response), so calling code is identical either way.
//!
//! Cluster mode implements client-side replication client-side replication: writes fan
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

use tokio::sync::{mpsc, Mutex, Semaphore};

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
/// The TTL a read-repair write applies to the primary (read repair).
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

/// Value compression: values shorter than this (bytes) are never
/// compressed — the per-value overhead of attempting it outweighs the
/// savings. Only meaningful when `compress(true)`.
const DEFAULT_COMPRESSION_THRESHOLD: usize = 256;

/// See [`Options::reconnect_cooldown`].
const DEFAULT_RECONNECT_COOLDOWN: Duration = Duration::from_secs(1);

/// The server's own request cap (src/server.rs's `MAX_REQUEST_SIZE`) is 1
/// MiB for the *entire* frame — header line plus namespace plus key plus
/// value; a request over that limit is rejected by simply closing the
/// connection without a response (poisoning whatever else is pipelined
/// behind it on that same connection). This reserves 256 bytes of
/// headroom for the header itself (marker byte, decimal lengths —
/// including a namespaced frame's extra `<namespace-length>` field
/// (Namespaces, issue #105) — an optional TTL, echoed response tags's tag
/// field, spaces, the trailing newline — always comfortably under this
/// even for the largest fields), so a namespace+key+value that clears
/// `MAX_REQUEST_BYTES` is guaranteed to fit under the server's own cap and
/// never trips that connection-poisoning rejection (issue #47 audit item
/// R1; see README's "Errors" section).
const MAX_REQUEST_BYTES: usize = 1024 * 1024 - 256;

/// The default namespace — always the empty byte string. Every
/// namespace-less `get`/`set`/`delete` call on this client passes this,
/// which is what keeps them on the legacy `G`/`S`/`D` wire forms
/// byte-for-byte (Namespaces, issue #105's SDK rule): an unchanged client
/// talking to a pre-namespace server keeps working.
const DEFAULT_NAMESPACE: &[u8] = b"";

/// Rejects an empty key, or a namespace+key that alone already exceeds
/// `MAX_REQUEST_BYTES`, before any network I/O: the server's protocol has
/// no way to represent a zero-length key request that doesn't collide
/// with other framing, and a namespace+key past the server's own 1 MiB
/// request cap can never be stored either way — both cases get exactly
/// one reply from the server: closing the connection outright, silently
/// poisoning every other request already pipelined on that connection
/// (see src/command.rs's `rejects_empty_key_for_get` et al., and this
/// module's `MAX_REQUEST_BYTES` doc comment). `get`/`delete` call this
/// directly (no value to bound), so without the size check here an
/// oversized namespace/key on either of those paths would sail straight
/// past client-side validation and only be caught by the server slamming
/// the connection shut (issue #47 audit item R1 follow-up). Catching both
/// cases here client-side, as `Error::InvalidArgument`, gives the caller
/// a clear synchronous error and avoids that blast radius entirely. The
/// namespace itself has no length limit of its own (Namespaces, issue
/// #105) beyond this shared request-size bound.
fn validate_key(namespace: &[u8], key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidArgument(
            "nanocached: key must not be empty".to_string(),
        ));
    }
    if namespace.len() + key.len() > MAX_REQUEST_BYTES {
        return Err(Error::InvalidArgument(if namespace.is_empty() {
            // Keeps the pre-namespace message unchanged for the common,
            // namespace-less case.
            format!(
                "nanocached: key exceeds MAX_REQUEST_BYTES ({MAX_REQUEST_BYTES} bytes), got {} bytes",
                key.len()
            )
        } else {
            format!(
                "nanocached: namespace ({} bytes) + key ({} bytes) exceeds MAX_REQUEST_BYTES ({MAX_REQUEST_BYTES} bytes)",
                namespace.len(),
                key.len()
            )
        }));
    }
    Ok(())
}

/// `validate_key` plus a `MAX_REQUEST_BYTES` bound on `namespace.len() +
/// key.len() + value.len()` — anything past it can never fit the
/// server's own 1 MiB request cap, so failing fast here is strictly
/// better than sending a frame the server can only reject by silently
/// closing the connection. The combined check below is redundant
/// whenever `validate_key` alone already rejects an oversized
/// namespace+key, but stays as its own check since a namespace+key
/// comfortably under the bound can still push the combined total over it
/// once `value` is added.
fn validate_key_and_value(namespace: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
    validate_key(namespace, key)?;
    if namespace.len() + key.len() + value.len() > MAX_REQUEST_BYTES {
        return Err(Error::InvalidArgument(if namespace.is_empty() {
            format!(
                "nanocached: key ({} bytes) + value ({} bytes) exceeds MAX_REQUEST_BYTES ({} bytes)",
                key.len(),
                value.len(),
                MAX_REQUEST_BYTES
            )
        } else {
            format!(
                "nanocached: namespace ({} bytes) + key ({} bytes) + value ({} bytes) exceeds MAX_REQUEST_BYTES ({} bytes)",
                namespace.len(),
                key.len(),
                value.len(),
                MAX_REQUEST_BYTES
            )
        }));
    }
    Ok(())
}

/// `get`'s strict UTF-8 decode — shared by [`NanocachedClient::get`] and
/// [`Namespace::get`] so the two stay identical instead of drifting.
fn decode_utf8_value(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(Error::InvalidUtf8)
}

/// `clear`'s own bound (issue #106): a clear frame carries no key or
/// value, only the namespace, so unlike `validate_key`/
/// `validate_key_and_value` there is nothing to sum it against — the
/// namespace alone just needs to fit under the server's own request cap,
/// same rationale as those two (issue #47 audit item R1 follow-up):
/// failing fast here, as `Error::InvalidArgument`, beats sending a frame
/// the server can only reject by closing the connection outright.
fn validate_namespace_for_clear(namespace: &[u8]) -> Result<()> {
    if namespace.len() > MAX_REQUEST_BYTES {
        return Err(Error::InvalidArgument(format!(
            "nanocached: namespace exceeds MAX_REQUEST_BYTES ({MAX_REQUEST_BYTES} bytes), got {} bytes",
            namespace.len()
        )));
    }
    Ok(())
}

/// Fire-and-forget replica writes: bounds how many replica writes a single client may
/// have running in the background at once when `fire_and_forget_replicas`
/// is enabled — once the cap is reached, further replica legs fall back
/// to running synchronously, the same as with the option off. Read once
/// per `connect`; public-but-hidden purely as a test hook, mirroring
/// `KEEPALIVE_INTERVAL_MS`.
#[doc(hidden)]
pub static MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES: AtomicUsize = AtomicUsize::new(32);

/// Monotonic counters for failures this SDK deliberately swallows
/// (client-side replication / fire-and-forget replica writes / read repair) — observability for silently degrading
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
    read_hedge_after: Option<Duration>,
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
            read_hedge_after: None,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// The connect targets, tried in order for connect and every
    /// refresh: a single-node deployment is a one-element list, a
    /// cluster's discovery replicas (discovery HA) a longer one.
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
    /// (value compression). Off by default. Requires the `compression`
    /// feature (a default feature — disable it with `default-features =
    /// false` to opt out); without it, `compress(true)` fails at
    /// `connect()` time instead of failing to compile. **Every client
    /// that reads or writes a given set of keys must agree on this
    /// setting** — it is a per-keyspace format decision, not a
    /// per-client preference; take care before enabling
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
    /// for them too (fire-and-forget replica writes). Off by default. Unlike
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
    /// (read repair). Off by default. Costs extra reads only on
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

    /// Hedged reads (issue #64). Off by default (`None`). A read normally
    /// starts at the key's primary owner and only moves on to the next
    /// owner when the primary *fails* — so one slow-but-alive node (a
    /// saturated host, a bad link) bounds every read that touches it at
    /// its own full round trip, and with `R` copies on `N` nodes that is
    /// roughly `R/N` of all reads. Setting this sends the same read to the
    /// next owner as well once the primary has gone silent for `duration`
    /// (and, if that owner is also silent for another `duration`, the one
    /// after it, and so on), and takes the first answer:
    ///
    /// - a hit from any owner is final;
    /// - a miss is final only from the primary — a replica's miss is
    ///   provisional (it may simply lack the copy), so the primary is
    ///   still waited for and hedging never turns a hit into a miss; it
    ///   is accepted only once every owner has answered or failed;
    /// - a connection-level failure (or any error but [`Error::WrongNode`])
    ///   hedges onward immediately, no wait;
    /// - [`Error::WrongNode`] propagates exactly as the normal read path's
    ///   does.
    ///
    /// Only takes effect once a ring is known and the key has at least two
    /// owners (`R >= 2`); otherwise the sequential path runs unchanged —
    /// with a single copy there is nobody to hedge to. Writes are
    /// unaffected: every copy must still be written, so a slow owner
    /// bounds writes to it regardless ([`Self::fire_and_forget_replicas`]
    /// moves only the replica legs off the caller's path). The losing leg
    /// of a hedge is never cancelled — dropping a request mid-write could
    /// leave the connection desynced for whatever is queued behind it —
    /// so it runs to completion detached, and [`NanocachedClient::close`]
    /// drains it exactly like a fire-and-forget replica write.
    ///
    /// `duration` must be positive; a zero duration is rejected at
    /// [`NanocachedClient::connect`] time, the same as this crate's other
    /// invalid-argument checks.
    pub fn read_hedge_after(mut self, duration: Duration) -> Self {
        self.read_hedge_after = Some(duration);
        self
    }
}

struct Member {
    address: String,
    connection: Arc<Connection>,
}

/// What `write` should replay for a replica leg that ends up running
/// detached (fire-and-forget replica writes) — the synchronous path keeps using the
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

/// One hedged-read leg's outcome (hedged reads, issue #64): tagged with
/// `index` — its position in the owners list, 0 being the primary — so
/// `read_hedged` can tell a provisional replica miss (`index != 0`) apart
/// from the primary's own, final, miss.
struct HedgeOutcome {
    index: usize,
    result: Result<Option<Vec<u8>>>,
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
    /// Fire-and-forget replica writes: bounds in-flight background replica writes.
    /// Also close()'s drain primitive — acquiring every permit blocks
    /// until every currently in-flight background write has released
    /// its own, i.e. finished.
    background_replica_permits: Arc<Semaphore>,
    background_replica_cap: usize,
    read_repair: bool,
    /// See [`Options::read_hedge_after`]. `None` means hedging is off.
    read_hedge_after: Option<Duration>,
    /// Hedged reads (issue #64): the losing leg of a hedge is left
    /// running to completion, detached — tracked here exactly like
    /// `background_replica_permits` tracks a fire-and-forget replica
    /// write, so `close()` can drain it the same way before teardown
    /// instead of leaving it dangling past the client's own lifetime.
    hedged_reads: Mutex<tokio::task::JoinSet<()>>,
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
        if matches!(options.read_hedge_after, Some(duration) if duration.is_zero()) {
            return Err(Error::InvalidArgument(
                "nanocached: read_hedge_after must be a positive duration".to_string(),
            ));
        }

        let tls = resolve_tls(options.tls, options.ca.as_deref()).await?;
        let compress = resolve_compression(options.compress)?;
        let auth_secret = options.auth_secret.as_deref().map(str::as_bytes);
        let reconnect_cooldown = options.reconnect_cooldown.resolve();

        // Walk the addresses until one yields a working target; an
        // address that is unreachable, warming up (`B`, discovery HA), or
        // knows no live nodes is skipped — the next replica may do
        // better.
        let mut last_error: Option<Error> = None;
        let mut target: Option<Target> = None;
        let mut tracking_key = String::new();
        // Bootstrap tolerance (issue #67): a member installed without a
        // live connection (see below) gets its reconnect cooldown armed
        // here, at the same instant it's installed, so a request routed
        // to it right after connect() returns fails immediately with the
        // dial error instead of re-paying a doomed `CONNECT_DEADLINE` —
        // exactly the cooldown a mid-life redial failure would leave
        // behind. Folded into `Inner::reconnect_cooldowns` once it exists.
        let mut initial_cooldowns: HashMap<String, (Instant, Error)> = HashMap::new();

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

                    // Dials every listed node concurrently (issue #67):
                    // `join_all` polls every dial together instead of one
                    // after another, so bootstrap's worst-case latency
                    // stays one `CONNECT_DEADLINE` regardless of cluster
                    // size, not `nodes.len()` of them in sequence.
                    let outcomes = futures_util::future::join_all(nodes.iter().map(|node| async {
                        let (node_host, node_port) = split_host_port(&node.address)?;
                        connect_and_identify(
                            &node_host,
                            node_port,
                            auth_secret,
                            tls.as_ref(),
                            CONNECT_DEADLINE,
                        )
                        .await
                    }))
                    .await;

                    let mut members = HashMap::new();
                    let mut reachable = 0usize;
                    let mut dial_last_error: Option<Error> = None;
                    let mut hard_error: Option<Error> = None;

                    for (node, outcome) in nodes.iter().zip(outcomes) {
                        match outcome {
                            Ok(Identified::Node { stream, tagged }) => {
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
                                reachable += 1;
                            }
                            Ok(Identified::Cluster { .. }) => {
                                // The same wrong answer would come back
                                // from every replica of this address, so
                                // there's no point tolerating it or trying
                                // another discovery address — a hard
                                // error, same as before issue #67.
                                hard_error = Some(Error::Protocol(format!(
                                    "nanocached: discovery server returned a non-node address: {}",
                                    node.address
                                )));
                                break;
                            }
                            Err(error) => {
                                // Issue #67: a node discovery still lists
                                // but that can't be reached — typically
                                // one that just died and hasn't been
                                // evicted yet (a window of seconds) — is
                                // installed as a member without a live
                                // connection (the same `Connection::dead()`
                                // placeholder a newly-discovered node gets
                                // in `refresh_node_list`) rather than
                                // failing `connect()` outright. It stays in
                                // the ring, so a request for one of its
                                // keys fails over per request exactly as
                                // it would after a mid-life death, and the
                                // reconnect cooldown armed here means the
                                // very first such request doesn't even pay
                                // for a doomed redial.
                                if let Some(cooldown) = reconnect_cooldown {
                                    initial_cooldowns.insert(
                                        node.address.clone(),
                                        (Instant::now() + cooldown, error.clone()),
                                    );
                                }
                                members.insert(
                                    node.name.clone(),
                                    Member {
                                        address: node.address.clone(),
                                        connection: Arc::new(Connection::dead()),
                                    },
                                );
                                dial_last_error = Some(error);
                            }
                        }
                    }

                    if let Some(error) = hard_error {
                        // Close whatever real connections already opened
                        // so they aren't leaked (and stay counted forever
                        // in open_targets); a dead placeholder has none to
                        // close, but close() on it is a harmless no-op.
                        for member in members.values() {
                            member.connection.close();
                        }
                        return Err(error);
                    }

                    if reachable == 0 {
                        // No listed node was reachable at all: nothing to
                        // route to, so connect() itself fails, with the
                        // last dial error — matching steady-state
                        // behavior, where a node that never comes back is
                        // eventually indistinguishable from one that was
                        // never there.
                        return Err(dial_last_error.unwrap_or_else(|| {
                            Error::ConnectionLost(
                                "nanocached: could not connect to any address".to_string(),
                            )
                        }));
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
            reconnect_cooldowns: Mutex::new(initial_cooldowns),
            reconnect_cooldown,
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
            read_hedge_after: options.read_hedge_after,
            hedged_reads: Mutex::new(tokio::task::JoinSet::new()),
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
                        // Always the default namespace: the keep-alive key
                        // is reserved wire-wide, not per-namespace.
                        let _ = connection.get(DEFAULT_NAMESPACE, KEEPALIVE_KEY).await;
                    }
                }
            }))
        });

        Ok(Self { inner, keepalive })
    }

    /// How many nodes hold each key (client-side replication) — 1 against a single node.
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
    /// (client-side replication / fire-and-forget replica writes / read repair) — lets operators detect silently degrading
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
    /// finished and the connections are torn down (fire-and-forget replica writes as
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

        // Hedged reads (issue #64): the losing leg of a hedge is left
        // running to completion rather than cancelled (see
        // Options::read_hedge_after), so close() must not return while
        // one is still in flight — drained here exactly like the
        // fire-and-forget replica writes above.
        {
            let mut hedged = self.inner.hedged_reads.lock().await;
            while hedged.join_next().await.is_some() {}
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
            Some(bytes) => Ok(Some(decode_utf8_value(bytes)?)),
            None => Ok(None),
        }
    }

    /// Transparently decompresses when `compress` is enabled
    /// (value compression). With `read_repair`, a clean miss probes the
    /// remaining owners before being accepted as final (read repair).
    pub async fn get_bytes(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.get_bytes_in(DEFAULT_NAMESPACE, key).await
    }

    /// The shared implementation behind [`Self::get_bytes`] and
    /// [`Namespace::get_bytes`] (Namespaces, issue #105) — the latter is
    /// nothing but this, called with its own namespace instead of
    /// [`DEFAULT_NAMESPACE`]; no networking is duplicated between them.
    async fn get_bytes_in(
        &self,
        namespace: &[u8],
        key: impl AsRef<[u8]>,
    ) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        validate_key(namespace, key)?;
        self.before_operation().await?;
        let mut value = self
            .with_cluster_retry(|| self.read(namespace, key))
            .await?;
        if value.is_none() && self.inner.read_repair {
            let clustered = matches!(self.inner.state.lock().await.target, Target::Cluster { .. });
            if clustered {
                value = self.try_read_repair(namespace, key).await;
            }
        }
        match value {
            Some(bytes) if self.inner.compress => {
                Ok(Some(crate::compression::decompress_value(&bytes)?))
            }
            other => Ok(other),
        }
    }

    /// Read repair: probes the remaining owners of `(namespace, key)` —
    /// every owner but the primary, which the normal read path already
    /// probed and got a clean miss from — in rank order, for a value. The
    /// first one that has it wins: its value is returned, and — detached,
    /// not awaited, no tracking — that same value repairs the true primary
    /// in the background with `READ_REPAIR_TTL`. Every failure along the
    /// way (connection lost, WrongNode, another miss) is swallowed;
    /// nothing here may turn an already-accepted miss into an error. A
    /// failed repair write is counted in `stats().read_repair_failures`.
    async fn try_read_repair(&self, namespace: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let owners = {
            let state = self.inner.state.lock().await;
            Self::owner_names(&state, namespace, key)
        };

        for name in owners.iter().skip(1) {
            let probe =
                |connection: Arc<Connection>| async move { connection.get(namespace, key).await };
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
                // (read repair), so a later miss repairs the key instead, and it
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
                        let owned_namespace: Arc<[u8]> = Arc::from(namespace.to_vec());
                        let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
                        let owned_value: Arc<[u8]> = Arc::from(value.clone());
                        tokio::spawn(async move {
                            let _permit = permit; // held until this task finishes
                            let op = move |connection: Arc<Connection>| {
                                let namespace = Arc::clone(&owned_namespace);
                                let key = Arc::clone(&owned_key);
                                let value = Arc::clone(&owned_value);
                                async move {
                                    connection
                                        .set(&namespace, &key, &value, READ_REPAIR_TTL)
                                        .await
                                }
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
    /// enabled (value compression).
    pub async fn set(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<()> {
        self.set_in(DEFAULT_NAMESPACE, key, value, ttl_seconds)
            .await
    }

    /// The shared implementation behind [`Self::set`] and
    /// [`Namespace::set`] (Namespaces, issue #105).
    async fn set_in(
        &self,
        namespace: &[u8],
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
        // Sized against what actually goes on the wire — the compressed
        // form when compression is on — like the other SDKs, so a large
        // but compressible value isn't refused for its raw size.
        validate_key_and_value(namespace, key, value)?;
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(
                namespace,
                key,
                WriteBody::Set { value, ttl_seconds },
                move |connection| async move {
                    connection.set(namespace, key, value, ttl_seconds).await
                },
            )
        })
        .await
    }

    /// Returns whether the key existed before this call.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.delete_in(DEFAULT_NAMESPACE, key).await
    }

    /// The shared implementation behind [`Self::delete`] and
    /// [`Namespace::delete`] (Namespaces, issue #105).
    async fn delete_in(&self, namespace: &[u8], key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();
        validate_key(namespace, key)?;
        self.before_operation().await?;
        self.with_cluster_retry(|| {
            self.write(namespace, key, WriteBody::Delete, |connection| async move {
                connection.delete(namespace, key).await
            })
        })
        .await
    }

    /// Flushes every namespace, the default one included, across every
    /// node (issue #106's `F`) — the whole store starts empty again.
    /// Deliberately not named `flush*` (the issue's own naming
    /// guidance): from the caller's side this is exactly "clear
    /// everything" the same way [`Namespace::clear`] is "clear this one
    /// namespace". Fans out to every node and requires all of them to
    /// ack, refreshing the node list once and retrying if any fail
    /// (`clear_fanout`, shared with [`Namespace::clear`]) — success here
    /// never means a partial clear.
    pub async fn clear_all(&self) -> Result<()> {
        self.before_operation().await?;
        self.clear_fanout(|connection: Arc<Connection>| async move { connection.clear_all().await })
            .await
    }

    /// The shared implementation behind [`Namespace::clear`] (issue
    /// #106's `c`) — drops every entry in `namespace` across every node.
    /// `namespace` empty clears the default namespace; this is
    /// deliberately not rejected, matching `namespace("")` itself being a
    /// valid (if trivial) handle.
    async fn clear_in(&self, namespace: &[u8]) -> Result<()> {
        validate_namespace_for_clear(namespace)?;
        self.before_operation().await?;
        self.clear_fanout(move |connection: Arc<Connection>| async move {
            connection.clear(namespace).await
        })
        .await
    }

    /// A namespaced view onto this client (Namespaces, issue #105):
    /// `get`/`get_bytes`/`set`/`delete` on the returned [`Namespace`]
    /// behave exactly like this client's own, except every key is scoped
    /// to `ns` — the same key name under two different namespaces (or
    /// under no namespace at all) names two, or three, wholly independent
    /// entries. `ns` accepts the same key-ish types this crate accepts
    /// for keys; a `&str` is UTF-8 encoded. There is no length limit on
    /// `ns` beyond the request-size rules this crate already applies to
    /// key+value.
    ///
    /// `namespace("")` returns a handle equivalent to this client itself
    /// — the empty namespace is the default one every namespace-less call
    /// already uses, so it is not rejected. The handle is cheap (it
    /// shares this client's connections and routing, and opens no sockets
    /// of its own) and stays valid only as long as this client does:
    /// using it after [`Self::close`] fails with [`Error::AlreadyClosed`],
    /// exactly like calling this client's own methods after close.
    pub fn namespace(&self, ns: impl AsRef<[u8]>) -> Namespace {
        Namespace {
            client: self.clone(),
            namespace: Arc::from(ns.as_ref()),
        }
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

    fn owner_names(state: &State, namespace: &[u8], key: &[u8]) -> Vec<String> {
        match &state.target {
            Target::Single { .. } => Vec::new(),
            Target::Cluster {
                ring, replication, ..
            } => ring
                .owners(namespace, key, *replication)
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }

    async fn read(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                let op = |connection: Arc<Connection>| async move {
                    connection.get(namespace, key).await
                };
                return self.apply_reconnecting(None, &op).await;
            }
            Self::owner_names(&state, namespace, key)
        };

        // Hedged reads (issue #64): only once the key actually has a
        // second owner to hedge to — with a single owner (or in single-node
        // mode, already handled above) there is nobody to hedge to, so the
        // sequential path below runs exactly as before.
        if let Some(hedge_after) = self.inner.read_hedge_after {
            if owners.len() >= 2 {
                return self.read_hedged(namespace, key, owners, hedge_after).await;
            }
        }

        // Owners in rank order; fall through only on connection-level
        // failure — a replica hedges against a dead holder, not a miss.
        let mut last_error: Option<Error> = None;
        for name in owners {
            let op =
                |connection: Arc<Connection>| async move { connection.get(namespace, key).await };
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

    /// Hedged reads (issue #64): the read starts at `owners[0]` (the
    /// primary); if nothing has answered within `hedge_after`, the same
    /// read is also sent to `owners[1]`, and so on — one more owner every
    /// further `hedge_after` — until every owner is in flight or one
    /// settles the read. The first answer decides:
    ///
    /// - a hit (`Ok(Some(_))`) from any owner is final;
    /// - a miss (`Ok(None)`) is final only from the primary (`index ==
    ///   0`) — a replica's miss is provisional (it may simply lack the
    ///   copy) and does not by itself end the read; it is accepted only
    ///   once every owner has answered or failed (`replica_missed` with no
    ///   owner left to try);
    /// - `Error::WrongNode` propagates immediately, exactly as the
    ///   sequential path's does;
    /// - any other error hedges onward immediately (the next owner, if
    ///   any, starts right away rather than waiting out the rest of the
    ///   interval) and is remembered as `last_error`.
    ///
    /// Every leg — including the ones still in flight once this method
    /// returns — is spawned via `spawn_hedge_leg` onto `hedged_reads`,
    /// never cancelled, and drained by `close()`.
    async fn read_hedged(
        &self,
        namespace: &[u8],
        key: &[u8],
        owners: Vec<String>,
        hedge_after: Duration,
    ) -> Result<Option<Vec<u8>>> {
        let owned_namespace: Arc<[u8]> = Arc::from(namespace.to_vec());
        let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
        let (tx, mut rx) = mpsc::unbounded_channel::<HedgeOutcome>();

        self.spawn_hedge_leg(
            Arc::clone(&owned_namespace),
            Arc::clone(&owned_key),
            owners[0].clone(),
            0,
            tx.clone(),
        )
        .await?;
        let mut next_index = 1usize;
        // How many legs are currently in flight (spawned, no outcome
        // received yet) — once this hits zero with owners left to try,
        // the next one starts immediately rather than waiting out the
        // rest of the current hedge interval.
        let mut in_flight = 1usize;
        let mut last_error: Option<Error> = None;
        let mut replica_missed = false;

        loop {
            let outcome = if next_index < owners.len() {
                match tokio::time::timeout(hedge_after, rx.recv()).await {
                    Ok(Some(outcome)) => outcome,
                    // Every sender is a leg still counted in `in_flight`;
                    // this can't happen while any are outstanding. Kept as
                    // a defensive fallback rather than a panic.
                    Ok(None) => break,
                    Err(_elapsed) => {
                        // The hedge interval elapsed with no answer yet:
                        // one more owner, right away.
                        self.spawn_hedge_leg(
                            Arc::clone(&owned_namespace),
                            Arc::clone(&owned_key),
                            owners[next_index].clone(),
                            next_index,
                            tx.clone(),
                        )
                        .await?;
                        next_index += 1;
                        in_flight += 1;
                        continue;
                    }
                }
            } else {
                match rx.recv().await {
                    Some(outcome) => outcome,
                    None => break,
                }
            };
            in_flight -= 1;

            match outcome.result {
                Ok(Some(value)) => return Ok(Some(value)),
                Ok(None) if outcome.index == 0 => return Ok(None),
                Ok(None) => replica_missed = true,
                Err(Error::WrongNode) => return Err(Error::WrongNode),
                Err(error) => last_error = Some(error),
            }

            // Everything currently in flight has now answered or failed
            // (a provisional replica miss or a swallowed failure): the
            // next owner, if any is left, gets its turn immediately
            // instead of waiting out the rest of the interval.
            if in_flight == 0 {
                if next_index < owners.len() {
                    self.spawn_hedge_leg(
                        Arc::clone(&owned_namespace),
                        Arc::clone(&owned_key),
                        owners[next_index].clone(),
                        next_index,
                        tx.clone(),
                    )
                    .await?;
                    next_index += 1;
                    in_flight += 1;
                } else {
                    break;
                }
            }
        }

        if replica_missed {
            return Ok(None);
        }
        Err(last_error.unwrap_or_else(|| {
            Error::ConnectionLost("nanocached: no owner is reachable for this key".to_string())
        }))
    }

    /// Starts one hedge leg against `owners[index]`. Spawned onto
    /// `hedged_reads` rather than a bare `tokio::spawn` so `close()` can
    /// find and drain it (exactly as it drains a fire-and-forget replica
    /// write) even if `read_hedged` has already returned by the time this
    /// leg finishes — the losing leg of a hedge is never cancelled,
    /// because dropping a request mid-write could desync the connection
    /// for whatever else is queued behind it (see
    /// `Connection::request_uncapped`'s `WriteGuard`). Its outcome is
    /// delivered back over `tx`, tagged with `index`; if the read has
    /// already decided and dropped its receiver, the send simply fails
    /// and is ignored — the result is discarded either way.
    async fn spawn_hedge_leg(
        &self,
        namespace: Arc<[u8]>,
        key: Arc<[u8]>,
        name: String,
        index: usize,
        tx: mpsc::UnboundedSender<HedgeOutcome>,
    ) -> Result<()> {
        let client = self.clone();
        let mut hedged = self.inner.hedged_reads.lock().await;
        // Re-check `closed` *after* taking the lock `close()` drains the
        // JoinSet under (issue #91): `close()` sets `closed` before it
        // acquires this lock (see `close()`), so a leg that gets the lock
        // while closed must not spawn — it would run against connections
        // teardown is about to close and never be awaited by the drain that
        // has already run (or is about to). Mirrors the background-replica
        // recheck. If `closed` is still false here, `close()` hasn't taken
        // this lock yet, so the task added below is in the JoinSet before
        // that drain and is awaited by it.
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(Error::AlreadyClosed);
        }
        hedged.spawn(async move {
            let op = move |connection: Arc<Connection>| {
                let namespace = Arc::clone(&namespace);
                let key = Arc::clone(&key);
                async move { connection.get(&namespace, &key).await }
            };
            let result = client.apply_reconnecting(Some(&name), &op).await;
            let _ = tx.send(HedgeOutcome { index, result });
        });
        Ok(())
    }

    async fn write<T, F, Fut>(
        &self,
        namespace: &[u8],
        key: &[u8],
        body: WriteBody<'_>,
        op: F,
    ) -> Result<T>
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
            Self::owner_names(&state, namespace, key)
        };

        let Some((primary, replicas)) = owners.split_first() else {
            return Err(Error::ConnectionLost(
                "nanocached: no owner is reachable for this key".to_string(),
            ));
        };

        // Fan out to the replicas concurrently with the primary write. The
        // primary's outcome decides; replica failures are swallowed by
        // design (client-side replication) — a dead or disagreeing replica leaves the key
        // under-replicated until the next node-list refresh, never fails
        // the write. Counted in `stats().replica_write_failures` so
        // operators can spot silently degrading replication.
        // Fire-and-forget replica writes: with fire_and_forget_replicas, up to
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
                            let owned_namespace: Arc<[u8]> = Arc::from(namespace.to_vec());
                            let owned_key: Arc<[u8]> = Arc::from(key.to_vec());
                            let owned_body = body.to_owned();
                            tokio::spawn(async move {
                                let _permit = permit; // held until this task finishes
                                let failed = match owned_body {
                                    OwnedWriteBody::Set { value, ttl_seconds } => {
                                        let value: Arc<[u8]> = Arc::from(value);
                                        let op = move |connection: Arc<Connection>| {
                                            let namespace = Arc::clone(&owned_namespace);
                                            let key = Arc::clone(&owned_key);
                                            let value = Arc::clone(&value);
                                            async move {
                                                connection
                                                    .set(&namespace, &key, &value, ttl_seconds)
                                                    .await
                                            }
                                        };
                                        client.apply_reconnecting(Some(&name), &op).await.is_err()
                                    }
                                    OwnedWriteBody::Delete => {
                                        let op = move |connection: Arc<Connection>| {
                                            let namespace = Arc::clone(&owned_namespace);
                                            let key = Arc::clone(&owned_key);
                                            async move { connection.delete(&namespace, &key).await }
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

    /// Namespace clear / flush-everything fan-out (issue #106's `clear`/
    /// `clear_all`): unlike `write`'s `op`, `op` here (built from
    /// `Connection::clear`/`Connection::clear_all`) isn't key-addressed —
    /// a namespace's keys are spread over every node by HRW — so instead
    /// of picking owners for one key, this sends `op` to every node
    /// currently known and requires all of them to ack.
    ///
    /// In single-node mode there is exactly one node, so this simply
    /// defers to `apply_reconnecting`'s own transparent redial-and-retry
    /// — the fan-out/refresh machinery below only makes sense once
    /// there's a node list to fan out over and refresh.
    ///
    /// On a cluster, if any node fails the first pass, the node list is
    /// refreshed once — the same path a `W`/dead-primary retry already
    /// uses (`maybe_refresh` counts a failed refresh in
    /// `stats().refresh_failures` on its own, so nothing extra is needed
    /// here) — and the whole fan-out is retried once more against every
    /// node of the *refreshed* list, not just the ones that failed: a
    /// clear is idempotent, so re-sending it to a node that already
    /// succeeded is harmless, and the refreshed list may not even be the
    /// same nodes. A node that still fails after that retry fails the
    /// whole call — this never returns success on a partial clear; the
    /// caller can simply retry the whole operation again.
    async fn clear_fanout<F, Fut>(&self, op: F) -> Result<()>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let single = matches!(self.inner.state.lock().await.target, Target::Single { .. });
        if single {
            return self.apply_reconnecting(None, &op).await;
        }

        if self.clear_fanout_once(&op).await.is_some() {
            self.maybe_refresh(true).await;
            if let Some(failed) = self.clear_fanout_once(&op).await {
                return Err(Error::ConnectionLost(format!(
                    "nanocached: clear failed on node(s): {} (even after a node-list refresh and retry)",
                    failed.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// One fan-out pass of `op` over every currently known cluster
    /// member, concurrently — every member is always attempted regardless
    /// of whether another already failed, so a single pass yields the
    /// complete failure list rather than just the first node to fail.
    /// `None` means every member acked; `Some` carries the names of the
    /// ones that didn't (a connection error, a mismatched reply, or —
    /// though the real protocol never sends one for `c`/`F` — anything
    /// else `apply_reconnecting` surfaces as an error).
    async fn clear_fanout_once<F, Fut>(&self, op: &F) -> Option<Vec<String>>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let names: Vec<String> = match &self.inner.state.lock().await.target {
            Target::Single { .. } => return None,
            Target::Cluster { members, .. } => members.keys().cloned().collect(),
        };

        let outcomes = futures_util::future::join_all(names.iter().map(|name| async move {
            (name.as_str(), self.apply_reconnecting(Some(name), op).await)
        }))
        .await;

        let failed: Vec<String> = outcomes
            .into_iter()
            .filter_map(|(name, result)| result.err().map(|_| name.to_string()))
            .collect();

        (!failed.is_empty()).then_some(failed)
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
        {
            let mut redials = self.inner.redials.lock().await;
            redials.retain(|slot, _| slot.is_empty() || live.contains(slot));
        }

        // Same rationale for the per-address reconnect cooldowns: a departed
        // node's address would otherwise leave its cooldown entry behind
        // forever in a churny deployment where nodes get a fresh IP:port on
        // every restart (issue #96).
        let live_addresses: std::collections::HashSet<String> =
            nodes.iter().map(|node| node.address.clone()).collect();
        let mut cooldowns = self.inner.reconnect_cooldowns.lock().await;
        cooldowns.retain(|address, _| live_addresses.contains(address));
    }

    /// Walks every address (discovery HA). Returns `None` — keep the
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

/// A namespaced view onto a [`NanocachedClient`] (Namespaces, issue #105):
/// the same key name under a different namespace — or under no namespace
/// at all — is a wholly independent entry. A namespace is a flat, opaque
/// byte string: there is no delimiter, no escaping, no hierarchy, and it
/// may contain any bytes.
///
/// Returned by [`NanocachedClient::namespace`]; cheap to create and cheap
/// to clone — it shares the client's connections, routing, and every
/// other option (compression, replication, hedging, ...), and opens no
/// sockets of its own. Every method here does nothing but forward to the
/// same internal `NanocachedClient` methods that `get`/`set`/`delete`
/// themselves call, just with this handle's namespace instead of the
/// default (empty) one — this crate's own networking is never
/// duplicated. A handle is invalid once its client is closed: using it
/// afterward fails with [`Error::AlreadyClosed`], exactly like calling
/// the client's own methods after close.
#[derive(Clone)]
pub struct Namespace {
    client: NanocachedClient,
    namespace: Arc<[u8]>,
}

impl Namespace {
    /// This handle's namespace, exactly as given to
    /// [`NanocachedClient::namespace`] — e.g. for a framework adapter
    /// built on this crate that needs to report or compare it.
    pub fn name(&self) -> &[u8] {
        &self.namespace
    }

    /// See [`NanocachedClient::get`]; scoped to this namespace.
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        match self.get_bytes(key).await? {
            Some(bytes) => Ok(Some(decode_utf8_value(bytes)?)),
            None => Ok(None),
        }
    }

    /// See [`NanocachedClient::get_bytes`]; scoped to this namespace.
    pub async fn get_bytes(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.client.get_bytes_in(&self.namespace, key).await
    }

    /// See [`NanocachedClient::set`]; scoped to this namespace.
    pub async fn set(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<()> {
        self.client
            .set_in(&self.namespace, key, value, ttl_seconds)
            .await
    }

    /// See [`NanocachedClient::delete`]; scoped to this namespace.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.client.delete_in(&self.namespace, key).await
    }

    /// Drops every entry in this namespace, across every node (issue
    /// #106's `c`) — other namespaces, and the default one, are
    /// untouched. On `namespace("")` (the default namespace's own
    /// handle) this clears the default namespace; see
    /// [`NanocachedClient::clear_all`] to clear every namespace at once
    /// instead. Fans out to every node and requires all of them to ack,
    /// refreshing the node list once and retrying if any fail — see
    /// [`NanocachedClient::clear_all`]'s own doc comment for the full
    /// fan-out/refresh-and-retry semantics, which this shares: success
    /// never means a partial clear.
    pub async fn clear(&self) -> Result<()> {
        self.client.clear_in(&self.namespace).await
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
            validate_key(DEFAULT_NAMESPACE, &oversized),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn validate_key_accepts_a_key_right_at_max_request_bytes() {
        let boundary = vec![0u8; MAX_REQUEST_BYTES];
        assert!(validate_key(DEFAULT_NAMESPACE, &boundary).is_ok());
    }

    #[test]
    fn validate_key_rejects_an_empty_key() {
        assert!(matches!(
            validate_key(DEFAULT_NAMESPACE, b""),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn validate_key_rejects_an_empty_key_even_with_a_namespace() {
        assert!(matches!(
            validate_key(b"users", b""),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn validate_key_sums_namespace_and_key_against_max_request_bytes() {
        // Neither alone exceeds the bound, but their sum does — the
        // namespace must count toward the same request-size limit as the
        // key (Namespaces, issue #105).
        let namespace = vec![0u8; MAX_REQUEST_BYTES / 2];
        let key = vec![0u8; MAX_REQUEST_BYTES / 2 + 1];
        assert!(validate_key(&namespace, &namespace[..1]).is_ok());
        assert!(matches!(
            validate_key(&namespace, &key),
            Err(Error::InvalidArgument(_))
        ));
    }
}
