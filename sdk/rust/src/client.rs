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

use crate::cas::{content_digest, CasToken};
use crate::compression::resolve_compression;
use crate::connection::{CasCondition, Connection, MultiEntry, REQUEST_TIMEOUT_MS};
use crate::error::{Error, Result};
use crate::hash_ring::HashRing;
use crate::identify::{
    connect_and_identify, resolve_tls, split_host_port, DiscoveredNode, DiscoveryQuery, Identified,
    Stream, TlsConfig, CONNECT_DEADLINE,
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

/// issue #151 — batched get/set: bounds how many keys `get_many`/
/// `get_many_bytes`/`set_many`/`set_many_bytes` pack into a single
/// `m`/`o` sub-frame per owner before splitting into more than one
/// (batch chunking) — same value the Go/TypeScript/Python/Java/.NET
/// SDKs use.
const MAX_BATCH_KEYS: usize = 400;

/// issue #222 — batch chunking's byte bound: fixed, non-per-entry
/// overhead the chunker in [`chunk_lengths`] reserves once per sub-frame,
/// on top of the real `namespace.len()` bytes it already counts.
/// `encode_multi_get`/`encode_multi_set` (connection.rs) put, ahead of
/// the per-entry `<key-length>[ <value-length>]` fields this constant
/// does *not* cover (those are priced exactly, per entry, by
/// [`get_entry_cost`]/[`set_entry_cost`]): the `m`/`o` marker and its
/// space, the `<namespace-length>` field (at most 7 digits —
/// `validate_key`/`validate_key_and_value` already bound `namespace.len()`
/// under `MAX_REQUEST_BYTES`), the `<n>` field (at most 3 digits, since a
/// chunk never holds more than `MAX_BATCH_KEYS` (400) entries), `o`'s
/// optional `<ttl-seconds>` field (`u64::MAX` is 20 digits), the optional
/// `<tag>` field both frames carry (`u32::MAX` is 10 digits), the
/// separating spaces, and the trailing newline. Sized generously —
/// correctness only needs an upper bound, not a tight one.
const MULTI_FRAME_FIXED_OVERHEAD_BYTES: usize = 64;

/// Decimal digit width of `n`, matching `format!("{n}")`'s length —
/// used by [`get_entry_cost`]/[`set_entry_cost`] to price the exact
/// `<length>` field(s) `encode_multi_get`/`encode_multi_set` add to the
/// header for one entry (issue #222), rather than billing every entry
/// for a worst-case guess.
fn decimal_digits(mut n: usize) -> usize {
    let mut digits = 1;
    while n >= 10 {
        n /= 10;
        digits += 1;
    }
    digits
}

/// The wire bytes one key adds to an `m` sub-frame: the key itself plus
/// the `" <key-length>"` header field `encode_multi_get` writes for it
/// (issue #222).
fn get_entry_cost(key: &[u8]) -> usize {
    key.len() + 1 + decimal_digits(key.len())
}

/// The wire bytes one key/value pair adds to an `o` sub-frame: the key
/// and value themselves plus the `" <key-length> <value-length>"` header
/// fields `encode_multi_set` writes for them (issue #222).
fn set_entry_cost(key: &[u8], value: &[u8]) -> usize {
    key.len() + value.len() + 2 + decimal_digits(key.len()) + decimal_digits(value.len())
}

/// Batch chunking's byte bound (issue #222): splits `entry_count`
/// entries — each priced by `entry_cost(i)` (see [`get_entry_cost`]/
/// [`set_entry_cost`]) — into contiguous sub-frame chunks, returning each
/// chunk's length. A chunk never exceeds `MAX_BATCH_KEYS` entries or
/// `MAX_REQUEST_BYTES` total bytes (`namespace_len` once per chunk, plus
/// [`MULTI_FRAME_FIXED_OVERHEAD_BYTES`], plus every entry's own cost) —
/// except that a chunk always holds at least one entry:
/// `validate_key`/`validate_key_and_value` already reject any single
/// namespace+key(+value) over `MAX_REQUEST_BYTES` before chunking ever
/// runs, so the one case this bound cannot also honor — a validated
/// entry that, only once this module's own per-entry header allowance is
/// added on top, nominally overshoots this client-side budget — still
/// fits under the server's real 1 MiB cap, inside `MAX_REQUEST_BYTES`'s
/// own 256-byte cushion below that cap.
fn chunk_lengths(
    namespace_len: usize,
    entry_count: usize,
    entry_cost: impl Fn(usize) -> usize,
) -> Vec<usize> {
    let mut lengths = Vec::new();
    let base = namespace_len + MULTI_FRAME_FIXED_OVERHEAD_BYTES;
    let mut chunk_len = 0usize;
    let mut chunk_bytes = 0usize;
    for i in 0..entry_count {
        let cost = entry_cost(i);
        if chunk_len > 0
            && (chunk_len == MAX_BATCH_KEYS || base + chunk_bytes + cost > MAX_REQUEST_BYTES)
        {
            lengths.push(chunk_len);
            chunk_len = 0;
            chunk_bytes = 0;
        }
        chunk_len += 1;
        chunk_bytes += cost;
    }
    if chunk_len > 0 {
        lengths.push(chunk_len);
    }
    lengths
}

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

/// [`NanocachedClient::get_many_in`]'s own UTF-8 decode, generalized
/// over a whole batch — shares [`decode_utf8_value`]'s strict-decoder
/// contract per value: any single non-UTF-8 value fails the whole
/// decode (issue #151), the same "one bad value poisons the batch's
/// text form" stance `get`/`get_bytes` already have for a single key.
fn decode_many(raw: HashMap<String, Vec<u8>>) -> Result<HashMap<String, String>> {
    let mut values = HashMap::with_capacity(raw.len());
    for (key, value) in raw {
        values.insert(key, decode_utf8_value(value)?);
    }
    Ok(values)
}

/// `decr`'s delta negation (issue #129) — shared by
/// [`NanocachedClient::decr`] and [`Namespace::decr`]. `i64::MIN` has no
/// valid `i64` negation (`i64::MAX` is one short of `|i64::MIN|`), so
/// rather than silently wrapping back to `i64::MIN` (which would send the
/// same delta `decr` was asked to negate away from), this rejects it
/// client-side before any I/O — mirrors `validate_key`'s own
/// "reject what the wire could never represent correctly" rule.
fn negate_delta(delta: i64) -> Result<i64> {
    delta.checked_neg().ok_or_else(|| {
        Error::InvalidArgument(
            "nanocached: decr delta must not be i64::MIN, which has no valid i64 negation"
                .to_string(),
        )
    })
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

/// Bounds the CUMULATIVE decompressed size of a single
/// `get_many`/`get_many_bytes` response (issue #386). The per-value cap
/// (`compression::MAX_DECOMPRESSED_LENGTH`, 64 MiB) bounds one value, but
/// a batch (up to `MAX_BATCH_KEYS` entries) could pair that per-value cap
/// with the key count to force ~`MAX_BATCH_KEYS` * 64 MiB of client
/// allocation from one small, highly-compressible wire response — the
/// per-value bomb defense amplified across the batch. 256 MiB leaves
/// ample room for a legitimate large batch (a 640 KiB average across 400
/// keys) while bounding the worst case. A plain const, not a mutable
/// static: `decompress_for_batch` takes the cap as a parameter, so tests
/// pass an explicit value instead of mutating process-wide state (the
/// race `read_one_response`'s wire bound already had to design out). Same
/// 256 MiB cap as the other five SDKs.
const MAX_MULTIGET_DECOMPRESSED_BYTES: u64 = 256 * 1024 * 1024;

/// Hedged reads' losing legs (issue #64), analogous to
/// `MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES` above (issue #276): a losing
/// leg is normally left running detached in `hedged_reads`, drained by
/// `close()` — with no bound, a client issuing many concurrent hedged
/// reads against a slow owner could accumulate an unbounded number of
/// them. Tracked against `hedged_reads`' own length, not a separate
/// permit pool shared with `background_replica_permits` — past this cap,
/// a read's remaining legs are awaited synchronously right there instead
/// of being left detached, the same "fall back to synchronous" shape
/// `background_replica_permits` uses past its own cap. Read once per
/// `connect`; public-but-hidden purely as a test hook, mirroring
/// `MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES`.
#[doc(hidden)]
pub static MAX_INFLIGHT_HEDGE_LOSER_LEGS: AtomicUsize = AtomicUsize::new(32);

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
    /// Retryable-error status `R` (issue #125): every `R` this client has
    /// ever received across every connection it has opened — including
    /// ones already superseded by a later redial — whether or not that
    /// particular request went on to succeed once retried transparently.
    /// See [`Error::Retryable`] for the case where the bounded retry
    /// itself was exhausted.
    pub transient_retries: u64,
}

/// The live, atomically-updated counters [`NanocachedClient::stats`]
/// snapshots into a [`Stats`]; kept separate so the atomic types stay an
/// implementation detail of `Inner`.
struct StatsCounters {
    replica_write_failures: AtomicU64,
    read_repair_failures: AtomicU64,
    refresh_failures: AtomicU64,
    /// Shared verbatim (the same `Arc`, not a copy) with every
    /// [`Connection`] this client ever opens — see `Connection::new`'s
    /// `transient_retries` parameter and its own field doc comment for
    /// why a plain `AtomicU64` here wouldn't be reachable from
    /// `connection.rs`'s retry loop.
    transient_retries: Arc<AtomicU64>,
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
    via_proxy: bool,
    keep_alive_interval: Option<Duration>,
    request_timeout: Option<Duration>,
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
            via_proxy: false,
            keep_alive_interval: None,
            request_timeout: None,
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

    /// SDK proxy mode (issue #122). Off by default. Meaningful only when
    /// [`Self::addresses`] names discovery server(s): `connect()` fetches
    /// the proxy roster (`Q`, not the node roster `L`) from a discovery
    /// seed and lands on ONE proxy chosen at random — spreading a fleet of
    /// clients across the proxy tier — instead of joining every node
    /// individually. If the first address reached identifies as a cache
    /// node rather than a discovery server, `connect()` fails fast with
    /// [`Error::InvalidArgument`]: proxy mode needs discovery addresses.
    ///
    /// A proxy looks, on the wire, exactly like a single node that owns
    /// every key (`A` answers `On`/`OnT`, full `G`/`S`/`D`/`g`/`s`/`d`/`c`
    /// support, never `W`), so from here on this client runs in its
    /// existing single-connection mode: no ring view, no per-node
    /// connections, and — since there are no replicas to hedge a read
    /// to — [`Self::read_hedge_after`] is simply inert if also set;
    /// namespaces, `clear`/`clear_all`, compression, and keep-alive all
    /// work unchanged over the one connection. If the connection to the
    /// proxy is lost, the same proxy is redialed first (it may simply have
    /// restarted); only once that also fails does the client re-fetch the
    /// roster from discovery and swap onto another, randomly chosen,
    /// reachable proxy — reusing this crate's existing reconnect/refresh
    /// plumbing and `stats().refresh_failures` counter rather than a
    /// second one. `close()` is unchanged.
    pub fn via_proxy(mut self, enabled: bool) -> Self {
        self.via_proxy = enabled;
        self
    }

    /// How often each connection sends an internal keep-alive when
    /// otherwise idle (issue #27). Unset defers to the SDK default (half
    /// the server's 60s idle timeout). Read once per connection at connect.
    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self
    }

    /// Per-request round-trip deadline: a request that hasn't completed by
    /// then poisons its connection (a half-open server that accepts but
    /// never answers is indistinguishable from a slow one). Unset defers to
    /// the SDK default (30s). Read fresh on every request.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
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

/// One owner's key/`is_primary` membership across one
/// [`NanocachedClient::multi_set_pass`] call — see that method's own
/// doc comment for why a key can appear here with `is_primary` false:
/// the same node can be primary for one key in the batch and a replica
/// for another (issue #151). `keys`/`values` hold this owner's slice of
/// the pass's key/value bytes as `Arc<[u8]>` (issue #233) — a key at
/// replication R shows up in up to R owners' batches, so a plain
/// `Vec<u8>::clone()` per owner would deep-copy the same bytes up to R
/// times; cloning the `Arc` instead is just a refcount bump.
#[derive(Default)]
struct MultiSetOwnerBatch {
    indices: Vec<usize>,
    is_primary: Vec<bool>,
    keys: Vec<Arc<[u8]>>,
    values: Vec<Arc<[u8]>>,
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
    /// Cap for how many legs `hedged_reads` may hold before `read_hedged`
    /// stops detaching its own still-outstanding losers and instead
    /// awaits them synchronously (issue #276). Captured once from
    /// `MAX_INFLIGHT_HEDGE_LOSER_LEGS` at `connect`, mirroring
    /// `background_replica_cap`.
    hedge_loser_cap: usize,
    stats: StatsCounters,
    /// SDK proxy mode (issue #122): see [`Options::via_proxy`]. When set,
    /// `target` is always `Target::Single` (a proxy is single-connection
    /// from the client's point of view), but `with_cluster_retry` treats a
    /// `ConnectionLost` from it differently than a genuine standalone
    /// node's — see `reconnect_proxy`.
    via_proxy: bool,
    /// Resolved once from [`Options::request_timeout`] (falling back to the
    /// `REQUEST_TIMEOUT_MS` static), then handed to every `Connection` this
    /// client opens — including the ones a later refresh/reconnect dials —
    /// so the per-request deadline is per-client, not a shared global.
    request_timeout: Duration,
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

/// SDK proxy mode (issue #122, `Options::via_proxy`): walks `addresses`
/// exactly like the normal cluster path — the same seed iteration,
/// unreachable/`B`-busy skipping, and `connect_and_identify` call — but
/// asks each for the *proxy* roster (`Q`) instead of the node roster
/// (`L`), and lands on one proxy chosen at random rather than joining
/// every node. Kept as its own function (called only from `connect`, in
/// place of the node/cluster loop) rather than threaded through that
/// loop: the two shapes share only the seed-walking idea, not any actual
/// code — a `Cluster` target owns a whole ring and every member's
/// connection, while this always produces a plain `Target::Single`.
///
/// An address that identifies as a cache node is a hard error, not a
/// skip — `Options::via_proxy`'s own doc comment: proxy mode needs
/// discovery addresses, and the same misconfiguration would just repeat
/// at every other configured address too. An address whose roster is
/// empty, or none of whose proxies can be dialed, is skipped in favor of
/// the next seed exactly like an empty node roster is in the normal
/// path.
///
/// The returned tracking key is the discovery address that actually
/// served `Q` — not whichever proxy address ends up live — mirroring
/// `Target::Cluster`'s own convention (see `Member`'s doc comment): every
/// socket this client ever opens, this first one and every later
/// reconnect dial alike (same proxy or a freshly chosen one), is counted
/// against that one open-targets key.
async fn connect_via_proxy(
    addresses: &[(String, u16)],
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
    transient_retries: &Arc<AtomicU64>,
    request_timeout: Duration,
) -> Result<(Target, String)> {
    let mut last_error: Option<Error> = None;

    for (host, port) in addresses {
        let key = format!("{host}:{port}");

        match connect_and_identify(
            host,
            *port,
            auth_secret,
            tls,
            CONNECT_DEADLINE,
            DiscoveryQuery::Proxies,
        )
        .await
        {
            Err(error) => {
                last_error = Some(error);
                continue;
            }
            Ok(Identified::Node { .. }) => {
                // The stream is simply dropped (closing the socket) — a
                // misconfiguration, not something another seed could fix
                // (every discovery replica would answer the same way a
                // single node does), so this fails fast instead of
                // skipping ahead, mirroring the non-proxy path's own
                // hard error for a node address that unexpectedly turns
                // out to be a discovery server.
                return Err(Error::InvalidArgument(format!(
                    "nanocached: via_proxy needs discovery server addresses, but {key} identifies \
                     as a cache node"
                )));
            }
            Ok(Identified::Cluster { .. }) => {
                // Cannot happen: this function always asks with
                // `DiscoveryQuery::Proxies`. Kept as a defensive error
                // rather than `unreachable!()`, matching this crate's
                // general stance on trusting a remote server's framing.
                return Err(Error::Protocol(format!(
                    "nanocached: discovery server at {key} answered a query this client never sent"
                )));
            }
            Ok(Identified::Proxies { proxies }) => {
                if proxies.is_empty() {
                    last_error = Some(Error::Protocol(format!(
                        "nanocached: no proxies registered with the discovery server at {key}"
                    )));
                    continue;
                }
                let Some((address, stream, tagged)) =
                    dial_random_proxy(&proxies, auth_secret, tls).await
                else {
                    last_error = Some(Error::ConnectionLost(format!(
                        "nanocached: none of the {} proxy(es) registered with {key} are reachable",
                        proxies.len()
                    )));
                    continue;
                };
                let connection = Arc::new(Connection::new(
                    stream,
                    key.clone(),
                    tagged,
                    Arc::clone(transient_retries),
                    request_timeout,
                ));
                return Ok((
                    Target::Single {
                        address,
                        connection,
                    },
                    key,
                ));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        Error::ConnectionLost("nanocached: could not connect to any address".to_string())
    }))
}

/// Proxy mode (issue #122): tries `proxies` in random order — see
/// `shuffled_indices` for why this crate rolls its own tiny shuffle
/// rather than pulling in `rand` — and dials the first entry that
/// identifies as a cache node, exactly what a proxy looks like on the
/// wire (the module doc comment). `None` only once every entry has been
/// tried and failed (an unparseable address, a dial failure, or —
/// defensively — an address that turns out not to be a node at all).
async fn dial_random_proxy(
    proxies: &[DiscoveredNode],
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
) -> Option<(String, Stream, bool)> {
    for index in shuffled_indices(proxies.len()) {
        let proxy = &proxies[index];
        let Ok((host, port)) = split_host_port(&proxy.address) else {
            continue;
        };
        if let Ok(Identified::Node { stream, tagged }) = connect_and_identify(
            &host,
            port,
            auth_secret,
            tls,
            CONNECT_DEADLINE,
            DiscoveryQuery::Nodes,
        )
        .await
        {
            return Some((proxy.address.clone(), stream, tagged));
        }
    }
    None
}

/// A Fisher-Yates shuffle of `0..n`, using this module's own
/// dependency-free `random_u64` (proxy mode, issue #122) — picking which
/// proxy a client lands on has no security requirement, only "spread a
/// fleet of clients across proxies" (the spec this shipped against), so
/// pulling in the `rand` crate for it would be pure overhead this
/// otherwise-minimal-dependency crate doesn't need (mirrors why `tls`/
/// `compression` stay behind optional features instead of always-on
/// dependencies).
fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..n).collect();
    for i in (1..n).rev() {
        let j = (random_u64() % (i as u64 + 1)) as usize;
        indices.swap(i, j);
    }
    indices
}

/// A small, non-cryptographic source of randomness for `shuffled_indices`
/// (proxy mode, issue #122): seeded from the current time's nanoseconds,
/// a per-process atomic counter (so two calls within the same nanosecond
/// still diverge), and this stack frame's own address (ASLR entropy),
/// mixed with `splitmix64`. Good enough to keep a fleet of fresh clients
/// from all piling onto the same proxy — not a security boundary, so
/// nothing here needs to resist prediction.
fn random_u64() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    let stack_entropy = &counter as *const u64 as u64;
    splitmix64(nanos ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ stack_entropy)
}

/// The SplitMix64 mixing step (public domain; Vigna's `splitmix64.c`) —
/// spreads `random_u64`'s not-very-random inputs into a well-distributed
/// output.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
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
        // Resolved once and handed to every connection this client opens
        // (bootstrap, proxy, and later refresh/reconnect dials), so the
        // per-request deadline is per-client rather than a shared global a
        // concurrent test could shorten — see `Inner::request_timeout`.
        let request_timeout = options.request_timeout.unwrap_or_else(|| {
            Duration::from_millis(REQUEST_TIMEOUT_MS.load(std::sync::atomic::Ordering::SeqCst))
        });

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
        // Retryable-error status `R` (issue #125): one counter, shared
        // (the same `Arc`, never re-created) with every connection this
        // client will ever open, initial ones included — see
        // `Connection::new`'s `transient_retries` parameter and
        // `StatsCounters::transient_retries`'s own doc comment.
        let transient_retries = Arc::new(AtomicU64::new(0));

        if options.via_proxy {
            // SDK proxy mode (issue #122): a wholly different connect
            // shape — fetch the proxy roster (`Q`) rather than the node
            // roster (`L`) and land on one proxy, not join every node —
            // so it gets its own function entirely rather than being
            // threaded through the node/cluster loop below. See
            // `connect_via_proxy`'s own doc comment.
            let (proxy_target, key) = connect_via_proxy(
                &options.addresses,
                auth_secret,
                tls.as_ref(),
                &transient_retries,
                request_timeout,
            )
            .await?;
            target = Some(proxy_target);
            tracking_key = key;
        } else {
            // Walk the addresses until one yields a working target; an
            // address that is unreachable, warming up (`B`, discovery HA), or
            // knows no live nodes is skipped — the next replica may do
            // better.
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

                match connect_and_identify(
                    host,
                    *port,
                    auth_secret,
                    tls.as_ref(),
                    CONNECT_DEADLINE,
                    DiscoveryQuery::Nodes,
                )
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
                            connection: Arc::new(Connection::new(
                                stream,
                                key.clone(),
                                tagged,
                                Arc::clone(&transient_retries),
                                request_timeout,
                            )),
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
                        let outcomes =
                            futures_util::future::join_all(nodes.iter().map(|node| async {
                                let (node_host, node_port) = split_host_port(&node.address)?;
                                connect_and_identify(
                                    &node_host,
                                    node_port,
                                    auth_secret,
                                    tls.as_ref(),
                                    CONNECT_DEADLINE,
                                    DiscoveryQuery::Nodes,
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
                                                Arc::clone(&transient_retries),
                                                request_timeout,
                                            )),
                                        },
                                    );
                                    reachable += 1;
                                }
                                Ok(Identified::Cluster { .. }) | Ok(Identified::Proxies { .. }) => {
                                    // The same wrong answer would come back
                                    // from every replica of this address, so
                                    // there's no point tolerating it or trying
                                    // another discovery address — a hard
                                    // error, same as before issue #67. Every
                                    // dial here uses `DiscoveryQuery::Nodes`,
                                    // so `Proxies` is only reachable if a
                                    // discovery-listed node address somehow
                                    // answers as a discovery server itself.
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
                            ring: HashRing::new(
                                nodes.iter().map(|node| node.name.clone()).collect(),
                            ),
                            members,
                            replication,
                        });
                        tracking_key = key;
                        break;
                    }
                    // Unreachable in practice — this loop always dials with
                    // `DiscoveryQuery::Nodes`, which never yields `Proxies` —
                    // but the match must stay exhaustive over `Identified`.
                    // `Options::via_proxy` connects via `connect_via_proxy`
                    // instead, never reaching this loop at all.
                    Ok(Identified::Proxies { .. }) => {
                        last_error = Some(Error::Protocol(format!(
                        "nanocached: discovery server at {key} answered a query this client never sent"
                    )));
                    }
                }
            }
        }

        let Some(target) = target else {
            return Err(last_error.unwrap_or_else(|| {
                Error::ConnectionLost("nanocached: could not connect to any address".to_string())
            }));
        };

        let background_replica_cap = MAX_INFLIGHT_BACKGROUND_REPLICA_WRITES.load(Ordering::SeqCst);
        let hedge_loser_cap = MAX_INFLIGHT_HEDGE_LOSER_LEGS.load(Ordering::SeqCst);
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
            hedge_loser_cap,
            stats: StatsCounters {
                replica_write_failures: AtomicU64::new(0),
                read_repair_failures: AtomicU64::new(0),
                refresh_failures: AtomicU64::new(0),
                // The exact same `Arc` every connection created above
                // (initial dials) already shares — not a fresh counter —
                // so retries that happened before `Inner` even existed
                // still show up in `stats()`.
                transient_retries,
            },
            via_proxy: options.via_proxy,
            request_timeout,
        });

        // Keep-alive is always on, with an internal interval (issue #27):
        // half the server's 60s idle timeout, so it never severs a healthy
        // client. A per-client `Options::keep_alive_interval` wins; otherwise
        // the `KEEPALIVE_INTERVAL_MS` static supplies the default. Read once
        // per connect. (The per-client override is why tests no longer need
        // to mutate the shared static — which, read on connect while the
        // suite runs concurrently, could perturb an unrelated test.)
        let interval = options.keep_alive_interval.unwrap_or_else(|| {
            Duration::from_millis(KEEPALIVE_INTERVAL_MS.load(std::sync::atomic::Ordering::SeqCst))
        });
        // Safety net for a client dropped without `close()` (issue #325):
        // this task holds only a `Weak<Inner>`, never a strong one. If
        // every `NanocachedClient` handle (the original plus every clone)
        // is dropped without ever calling `close()`, `inner.closed` never
        // becomes true and nothing else would tell this loop to stop — a
        // strong `Arc<Inner>` here would keep `Inner` (and every
        // connection it owns) alive forever, so `Connection::drop`'s own
        // safety net could never fire either. With a `Weak`, the last
        // strong `Arc<Inner>` dropping (i.e. the last client handle) is
        // enough on its own: `Inner` drops immediately, its connections
        // drop with it, and this task simply fails its next `upgrade()`
        // and exits.
        let keepalive = Some({
            let weak_inner = Arc::downgrade(&inner);
            Arc::new(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let inner = match weak_inner.upgrade() {
                        Some(inner) => inner,
                        // Every client handle was dropped without close()
                        // (issue #325) — nothing left to keep alive.
                        None => return,
                    };
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
            transient_retries: self.inner.stats.transient_retries.load(Ordering::Relaxed),
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

    /// Number of hedge legs currently tracked in `hedged_reads` — finished
    /// legs still awaiting their next reap (see `spawn_hedge_leg`, issue
    /// #180) as well as ones genuinely in flight. Public-but-hidden purely
    /// as a test hook to observe that the JoinSet stays bounded instead of
    /// growing for the client's lifetime.
    #[doc(hidden)]
    pub async fn hedged_reads_len(&self) -> usize {
        self.inner.hedged_reads.lock().await.len()
    }

    /// Number of per-address reconnect-cooldown entries currently held
    /// (issue #296). Public-but-hidden purely as a test hook to observe
    /// that a departed proxy's entry does not linger forever after a
    /// proxy-mode failover swaps the pinned address.
    #[doc(hidden)]
    pub async fn reconnect_cooldowns_len(&self) -> usize {
        self.inner.reconnect_cooldowns.lock().await.len()
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
        let value = self.read_raw_with_repair(namespace, key).await?;
        match value {
            Some(bytes) if self.inner.compress => {
                Ok(Some(crate::compression::decompress_value(&bytes)?))
            }
            other => Ok(other),
        }
    }

    /// The raw-wire-bytes read shared by [`Self::get_bytes_in`] and
    /// [`Self::get_with_token_in`] (compare-and-set, issue #141): the
    /// cluster-retried primary read, falling back to read repair on a
    /// clean miss exactly as `get_bytes_in` always has. Returns the bytes
    /// exactly as they came off the wire — the compression marker byte
    /// still attached when `compress` is enabled — since that is what
    /// `get_with_token_in` must hash (see `cas.rs`'s module doc comment
    /// for why hashing the *decompressed* value would never match the
    /// server's own digest).
    async fn read_raw_with_repair(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut value = self
            .with_cluster_retry(|| self.read(namespace, key))
            .await?;
        if value.is_none() && self.inner.read_repair {
            let clustered = matches!(self.inner.state.lock().await.target, Target::Cluster { .. });
            if clustered {
                value = self.try_read_repair(namespace, key).await;
            }
        }
        Ok(value)
    }

    /// Compare-and-set (CAS, issue #141): like [`Self::get_bytes`], but
    /// also returns a [`CasToken`] — the digest of the value's exact wire
    /// bytes — for use with [`Self::replace`]/[`Self::delete_if_matches`].
    /// Computed from the raw bytes *before* this client's own
    /// decompression step when `compress` is enabled, so the token always
    /// matches what the server itself would compute (the server never
    /// decompresses); hashing the decompressed value instead would
    /// silently never match. There is no extra wire round trip — the
    /// digest is derived client-side from the same `get` response
    /// [`Self::get_bytes`] already reads.
    pub async fn get_with_token(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, CasToken)>> {
        self.get_with_token_in(DEFAULT_NAMESPACE, key).await
    }

    /// The shared implementation behind [`Self::get_with_token`] and
    /// [`Namespace::get_with_token`] (Namespaces, issue #105).
    async fn get_with_token_in(
        &self,
        namespace: &[u8],
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, CasToken)>> {
        let key = key.as_ref();
        validate_key(namespace, key)?;
        self.before_operation().await?;
        let raw = self.read_raw_with_repair(namespace, key).await?;
        match raw {
            Some(raw) => {
                let token = CasToken::from(content_digest(&raw));
                let value = if self.inner.compress {
                    crate::compression::decompress_value(&raw)?
                } else {
                    raw
                };
                Ok(Some((value, token)))
            }
            None => Ok(None),
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

    // ── batched get/set (issue #151) ─────────────────────────────────
    // m/o — see docs/protocol.html#multi. Every requested key's owner is
    // still resolved via HashRing/owner_names, exactly like a single
    // get_bytes/set: get_many_bytes groups keys by primary owner and
    // issues one `m` sub-frame per owner (batch chunking splits a group
    // over MAX_BATCH_KEYS entries, or over MAX_REQUEST_BYTES cumulative
    // wire bytes (issue #222), further); set_many_bytes groups by every
    // owner across every rank, since one batch's keys can place the same
    // node as primary for one key and a replica for another. A batch
    // never fails as a whole (docs/protocol.html#multi): get_many_bytes
    // returns every key that resolved, wrapped in
    // Err(Error::PartialWrongNode) (carrying that partial map) only if
    // some keys are still wrong-node after one bounded refresh-and-retry
    // — the same policy get_bytes' own with_cluster_retry applies,
    // generalized to a per-key roster instead of an all-or-nothing
    // retry. set_many_bytes has nothing to return on success, so it just
    // returns Err(Error::WrongNode) on the same condition.

    /// As [`Self::get_many_bytes`], decoding every hit as UTF-8.
    pub async fn get_many<K: AsRef<str>>(&self, keys: &[K]) -> Result<HashMap<String, String>> {
        self.get_many_in(DEFAULT_NAMESPACE, keys).await
    }

    /// The shared implementation behind [`Self::get_many`] and
    /// [`Namespace::get_many`] (Namespaces, issue #105).
    async fn get_many_in<K: AsRef<str>>(
        &self,
        namespace: &[u8],
        keys: &[K],
    ) -> Result<HashMap<String, String>> {
        match self.get_many_bytes_in(namespace, keys).await {
            Ok(raw) => decode_many(raw),
            Err(Error::PartialWrongNode(partial)) => {
                Err(Error::PartialWrongNodeText(decode_many(partial)?))
            }
            Err(Error::PartialConnectionLost(partial, cause)) => Err(
                Error::PartialConnectionLostText(decode_many(partial)?, cause),
            ),
            Err(other) => Err(other),
        }
    }

    /// Returns every requested key's raw value in one round trip per
    /// owner (batched get, docs/protocol.html#multi) — a missing key is
    /// simply absent from the returned map, never an error, the same "a
    /// miss is not an error" contract [`Self::get_bytes`] itself has.
    /// `keys` must be non-empty.
    ///
    /// A batch never fails as a whole: if some keys are still wrong-node
    /// after one bounded refresh-and-retry, returns
    /// `Err(Error::PartialWrongNode)` whose map holds every key that DID
    /// resolve, rather than discarding a mostly-successful batch over a
    /// handful of stale placements. In single-node/proxy mode a `W`
    /// propagates immediately, exactly as [`Self::get_bytes`]'s own
    /// single-mode behavior does — there is no ring to refresh against.
    ///
    /// Larger batches are transparently split into more than one `m`
    /// sub-frame per owner (batch chunking, bounded by both
    /// [`MAX_BATCH_KEYS`] and cumulative wire bytes, issue #222) —
    /// callers never need to think about this.
    pub async fn get_many_bytes<K: AsRef<str>>(
        &self,
        keys: &[K],
    ) -> Result<HashMap<String, Vec<u8>>> {
        self.get_many_bytes_in(DEFAULT_NAMESPACE, keys).await
    }

    /// The shared implementation behind [`Self::get_many_bytes`] and
    /// [`Namespace::get_many_bytes`] (Namespaces, issue #105).
    async fn get_many_bytes_in<K: AsRef<str>>(
        &self,
        namespace: &[u8],
        keys: &[K],
    ) -> Result<HashMap<String, Vec<u8>>> {
        if keys.is_empty() {
            return Err(Error::InvalidArgument(
                "nanocached: get_many/get_many_bytes requires at least one key".to_string(),
            ));
        }
        let key_strings: Vec<&str> = keys.iter().map(|key| key.as_ref()).collect();
        let mut key_bytes: Vec<Vec<u8>> = Vec::with_capacity(key_strings.len());
        for key in &key_strings {
            let bytes = key.as_bytes().to_vec();
            validate_key(namespace, &bytes)?;
            key_bytes.push(bytes);
        }
        self.before_operation().await?;

        let mut values: HashMap<String, Vec<u8>> = HashMap::with_capacity(key_strings.len());
        // Cumulative decompressed bytes across this whole response — see
        // decompress_for_batch. Shared across both cluster passes so the
        // bound spans the entire batch, not one pass.
        let budget = AtomicU64::new(0);

        let single = matches!(self.inner.state.lock().await.target, Target::Single { .. });
        if single {
            match self.multi_get_chunked(None, namespace, &key_bytes).await {
                Ok(entries) => {
                    let wrong_node =
                        self.splice_multi_get_entries(entries, &key_strings, &mut values, &budget)?;
                    if wrong_node {
                        return Err(Error::PartialWrongNode(values));
                    }
                    return Ok(values);
                }
                // Issue #411: a connection failure mid-chunk, after the
                // built-in reconnect-and-retry (apply_reconnecting)
                // already failed once. `partial_entries` is empty when
                // the very first chunk failed — nothing resolved yet, so
                // the plain underlying error propagates exactly as
                // before. Once at least one chunk landed, surface what
                // it resolved instead of discarding it.
                Err((partial_entries, error)) => {
                    if partial_entries.is_empty() {
                        return Err(error);
                    }
                    self.splice_multi_get_entries(
                        partial_entries,
                        &key_strings,
                        &mut values,
                        &budget,
                    )?;
                    return Err(Error::PartialConnectionLost(values, Box::new(error)));
                }
            }
        }

        let indices: Vec<usize> = (0..key_strings.len()).collect();
        let retry = self
            .multi_get_pass(
                namespace,
                &key_strings,
                &key_bytes,
                &mut values,
                indices,
                &budget,
            )
            .await?;
        if retry.is_empty() {
            return Ok(values);
        }
        self.maybe_refresh(true).await;
        let retry = self
            .multi_get_pass(
                namespace,
                &key_strings,
                &key_bytes,
                &mut values,
                retry,
                &budget,
            )
            .await?;
        if !retry.is_empty() {
            return Err(Error::PartialWrongNode(values));
        }
        Ok(values)
    }

    /// Decompresses one hit value for a `get_many` batch and charges its
    /// decompressed size against the response's cumulative budget (issue
    /// #386). `decompress_value` already caps a single value; this bounds
    /// the whole response so a batch of highly compressible values can't
    /// amplify that per-value cap into gigabytes of allocation. The cap is
    /// a parameter (production passes [`MAX_MULTIGET_DECOMPRESSED_BYTES`])
    /// rather than a mutable static, so a unit test can exercise the bound
    /// without racing concurrently running tests through a process-wide
    /// global — the same stance `read_one_response`'s wire bound takes.
    /// An associated function, not a method, for the same reason: the unit
    /// test needs no connected client. `join_all` polls the owner legs on
    /// one task, so the relaxed atomics are never contended mid-check.
    ///
    /// The budget only applies when `compress` is actually enabled (issue
    /// #410b) — with it off, `value` below is returned unchanged and the
    /// per-response read size already bounds this path on its own, so
    /// charging it here would just be an undocumented total-batch cap on
    /// uncompressed batches. And the current entry is charged before the
    /// cap is checked (issue #410a) so the entry that actually crosses it
    /// is caught — and excluded — rather than slipping through, which
    /// matters most when it is the last hit in the response.
    fn decompress_for_batch(
        compress: bool,
        value: Vec<u8>,
        budget: &AtomicU64,
        cap: u64,
    ) -> Result<Vec<u8>> {
        let value = if compress {
            crate::compression::decompress_value(&value)?
        } else {
            value
        };
        if compress {
            let charged =
                budget.fetch_add(value.len() as u64, Ordering::Relaxed) + value.len() as u64;
            if charged > cap {
                return Err(Error::Decompression(
                    "nanocached: cumulative decompressed size of this get_many response exceeds \
                     the maximum — possible decompression bomb across the batch"
                        .to_string(),
                ));
            }
        }
        Ok(value)
    }

    /// Splices a (possibly partial) run of `multi_get_chunked`'s entries
    /// into `values`, decompressing hits through
    /// [`Self::decompress_for_batch`] — shared by both the full-success
    /// and issue #411 partial-connection-failure paths of
    /// [`Self::get_many_bytes_in`]'s single-node/proxy-mode branch, so a
    /// mid-batch connection failure decodes exactly the same way a
    /// fully successful batch does. `key_strings` must have at least
    /// `entries.len()` elements, in the same order `entries` was
    /// produced (`multi_get_chunked` never reorders keys). Returns
    /// whether any entry was wrong-node.
    fn splice_multi_get_entries(
        &self,
        entries: Vec<MultiEntry>,
        key_strings: &[&str],
        values: &mut HashMap<String, Vec<u8>>,
        budget: &AtomicU64,
    ) -> Result<bool> {
        let mut wrong_node = false;
        for (i, entry) in entries.into_iter().enumerate() {
            match entry {
                MultiEntry::Hit(value) => {
                    values.insert(
                        key_strings[i].to_string(),
                        Self::decompress_for_batch(
                            self.inner.compress,
                            value,
                            budget,
                            MAX_MULTIGET_DECOMPRESSED_BYTES,
                        )?,
                    );
                }
                MultiEntry::WrongNode => wrong_node = true,
                MultiEntry::Miss | MultiEntry::Stored => {}
            }
        }
        Ok(wrong_node)
    }

    /// Issues one or more `m` sub-frames against `slot`'s connection
    /// (`None` for the single/proxy target) — already grouped to one
    /// owner by the caller — splitting into chunks bounded by both
    /// [`MAX_BATCH_KEYS`] and, since issue #222, cumulative wire bytes
    /// ([`chunk_lengths`]) so no `m` sub-frame risks exceeding either the
    /// wire's header bound or the server's `MAX_REQUEST_SIZE` (a batch of
    /// individually valid keys that would sum past it is split into more
    /// sub-frames instead).
    ///
    /// Issue #411: on `Ok`, every key resolved. On `Err`, the tuple's
    /// first element is every entry from the chunk(s) that landed
    /// *before* the one that failed — always a prefix of `keys`, since
    /// chunks are issued strictly in order — so a caller with a
    /// partial-result carrier to fill (single-node/proxy-mode
    /// `get_many_bytes_in`/`set_many_bytes_in`, and the cluster-mode leg
    /// runners) can still report what those chunks already resolved
    /// instead of discarding it.
    async fn multi_get_chunked(
        &self,
        slot: Option<&str>,
        namespace: &[u8],
        keys: &[Vec<u8>],
    ) -> std::result::Result<Vec<MultiEntry>, (Vec<MultiEntry>, Error)> {
        let mut entries = Vec::with_capacity(keys.len());
        let lengths = chunk_lengths(namespace.len(), keys.len(), |i| get_entry_cost(&keys[i]));
        let mut start = 0;
        for len in lengths {
            let chunk = &keys[start..start + len];
            start += len;
            let op = |connection: Arc<Connection>| async move {
                connection.multi_get(namespace, chunk).await
            };
            match self.apply_reconnecting(slot, &op).await {
                Ok(chunk_entries) => entries.extend(chunk_entries),
                Err(error) => return Err((entries, error)),
            }
        }
        Ok(entries)
    }

    /// One pass of [`Self::get_many_bytes_in`]'s cluster routing: group
    /// the given `indices` (every key, on the initial pass, or just the
    /// keys a previous pass left unresolved) by their current primary
    /// owner (matching plain `get`'s own primary-first stance), dispatch
    /// one (possibly chunked) `m` exchange per owner concurrently,
    /// splice hits into `values`, and return the indices still
    /// unresolved: a per-key `W`, or a whole owner group whose call
    /// failed outright. Called once for the initial pass and once more,
    /// if needed, after a single forced refresh.
    async fn multi_get_pass(
        &self,
        namespace: &[u8],
        key_strings: &[&str],
        key_bytes: &[Vec<u8>],
        values: &mut HashMap<String, Vec<u8>>,
        indices: Vec<usize>,
        budget: &AtomicU64,
    ) -> Result<Vec<usize>> {
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        let mut retry = Vec::new();
        {
            let state = self.inner.state.lock().await;
            for idx in indices {
                let owners = Self::owner_names(&state, namespace, &key_bytes[idx]);
                match owners.into_iter().next() {
                    Some(primary) => groups.entry(primary).or_default().push(idx),
                    None => retry.push(idx),
                }
            }
        }

        let outcomes =
            futures_util::future::join_all(groups.iter().map(|(owner, group_indices)| {
                self.run_multi_get_leg(
                    namespace,
                    owner,
                    group_indices,
                    key_strings,
                    key_bytes,
                    budget,
                )
            }))
            .await;

        for outcome in outcomes {
            let outcome = outcome?;
            retry.extend(outcome.0);
            for (key, value) in outcome.1 {
                values.insert(key, value);
            }
        }
        Ok(retry)
    }

    /// One owner group's `m` exchange, run concurrently with every other
    /// group by [`Self::multi_get_pass`]: a connection-level failure
    /// retries whatever chunk(s) of the group didn't get a response —
    /// same stance [`Self::apply_reconnecting`]'s own callers take
    /// elsewhere, just scoped to the unresolved tail rather than the
    /// whole group since issue #411's `multi_get_chunked` now reports
    /// which chunk(s) landed before the failure; a per-key `W` retries
    /// just that key; a hit is returned for the caller to splice into
    /// `values` once every group has finished (a client-side `compress`
    /// mismatch propagates, aborting the batch immediately — never fed
    /// into the retry pass, since it isn't a routing outcome). Returns
    /// `(retry indices, decoded hits)` rather than mutating shared state
    /// directly, since every group runs concurrently with the others.
    async fn run_multi_get_leg(
        &self,
        namespace: &[u8],
        owner: &str,
        group_indices: &[usize],
        key_strings: &[&str],
        key_bytes: &[Vec<u8>],
        budget: &AtomicU64,
    ) -> Result<(Vec<usize>, Vec<(String, Vec<u8>)>)> {
        let group_keys: Vec<Vec<u8>> = group_indices
            .iter()
            .map(|&i| key_bytes[i].clone())
            .collect();
        let (entries, tail_failed) = match self
            .multi_get_chunked(Some(owner), namespace, &group_keys)
            .await
        {
            Ok(entries) => (entries, false),
            Err((partial_entries, _connection_failure)) => (partial_entries, true),
        };
        let resolved = entries.len();

        let mut retry = Vec::new();
        let mut hits = Vec::new();
        for (&idx, entry) in group_indices.iter().zip(entries) {
            match entry {
                MultiEntry::WrongNode => retry.push(idx),
                MultiEntry::Hit(value) => {
                    hits.push((
                        key_strings[idx].to_string(),
                        Self::decompress_for_batch(
                            self.inner.compress,
                            value,
                            budget,
                            MAX_MULTIGET_DECOMPRESSED_BYTES,
                        )?,
                    ));
                }
                MultiEntry::Miss | MultiEntry::Stored => {}
            }
        }
        if tail_failed {
            retry.extend_from_slice(&group_indices[resolved..]);
        }
        Ok((retry, hits))
    }

    pub async fn set_many(&self, values: &HashMap<String, String>, ttl_seconds: u64) -> Result<()> {
        self.set_many_in(DEFAULT_NAMESPACE, values, ttl_seconds)
            .await
    }

    /// The shared implementation behind [`Self::set_many`] and
    /// [`Namespace::set_many`] (Namespaces, issue #105).
    async fn set_many_in(
        &self,
        namespace: &[u8],
        values: &HashMap<String, String>,
        ttl_seconds: u64,
    ) -> Result<()> {
        let raw: HashMap<String, Vec<u8>> = values
            .iter()
            .map(|(key, value)| (key.clone(), value.as_bytes().to_vec()))
            .collect();
        self.set_many_bytes_in(namespace, &raw, ttl_seconds).await
    }

    /// Stores every raw value in `values` in one round trip per involved
    /// node (batched set, docs/protocol.html#multi). `ttl_seconds == 0`
    /// means no expiry, shared by the whole batch — not per key, since
    /// every real caller of a batched set (Django's `set_many`,
    /// cache-manager's `mset`) already passes one TTL per call. `values`
    /// must be non-empty. Transparently compresses values at or above
    /// `compression_threshold` when `compress` is enabled, exactly like
    /// [`Self::set`].
    ///
    /// Within one batch, the same node can be a key's primary and
    /// another key's replica at once — it receives exactly one `o`
    /// sub-frame either way, and only its answer for the keys it is
    /// primary for decides that key's outcome; a replica-held key's
    /// failure or `W` is logged-and-swallowed into
    /// `stats().replica_write_failures`, exactly like [`Self::set`]'s
    /// own replica legs ([`Self::write`]). A batch never fails as a
    /// whole: if some keys' primaries are still wrong-node after one
    /// bounded refresh-and-retry, this returns `Err(Error::WrongNode)` —
    /// every other key in the batch was still stored. In
    /// single-node/proxy mode a `W` propagates immediately, exactly as
    /// [`Self::set`]'s own single-mode behavior does.
    ///
    /// Larger batches are transparently split into more than one `o`
    /// sub-frame per node (batch chunking, bounded by both
    /// [`MAX_BATCH_KEYS`] and cumulative wire bytes, issue #222).
    pub async fn set_many_bytes(
        &self,
        values: &HashMap<String, Vec<u8>>,
        ttl_seconds: u64,
    ) -> Result<()> {
        self.set_many_bytes_in(DEFAULT_NAMESPACE, values, ttl_seconds)
            .await
    }

    /// The shared implementation behind [`Self::set_many_bytes`] and
    /// [`Namespace::set_many_bytes`] (Namespaces, issue #105).
    async fn set_many_bytes_in(
        &self,
        namespace: &[u8],
        values: &HashMap<String, Vec<u8>>,
        ttl_seconds: u64,
    ) -> Result<()> {
        if values.is_empty() {
            return Err(Error::InvalidArgument(
                "nanocached: set_many/set_many_bytes requires at least one key".to_string(),
            ));
        }
        let mut key_bytes: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        let mut value_bytes: Vec<Vec<u8>> = Vec::with_capacity(values.len());
        for (key, original) in values {
            let key_owned = key.as_bytes().to_vec();
            validate_key_and_value(namespace, &key_owned, original)?;
            key_bytes.push(key_owned);
            value_bytes.push(if self.inner.compress {
                crate::compression::compress_value(original, self.inner.compression_threshold)
            } else {
                original.clone()
            });
        }
        self.before_operation().await?;

        let single = matches!(self.inner.state.lock().await.target, Target::Single { .. });
        if single {
            match self
                .multi_set_chunked(None, namespace, &key_bytes, &value_bytes, ttl_seconds)
                .await
            {
                Ok(entries) => {
                    if entries
                        .iter()
                        .any(|entry| matches!(entry, MultiEntry::WrongNode))
                    {
                        return Err(Error::WrongNode);
                    }
                    return Ok(());
                }
                // Issue #411: a connection failure mid-chunk, after the
                // built-in reconnect-and-retry already failed once.
                // `partial_entries` is empty when the very first chunk
                // failed — nothing stored yet, so the plain underlying
                // error propagates exactly as before. Once at least one
                // chunk landed, surface which keys it actually stored
                // instead of discarding that.
                Err((partial_entries, error)) => {
                    if partial_entries.is_empty() {
                        return Err(error);
                    }
                    let succeeded: std::collections::HashSet<String> = partial_entries
                        .iter()
                        .enumerate()
                        .filter(|(_, entry)| matches!(entry, MultiEntry::Stored))
                        .map(|(i, _)| {
                            String::from_utf8(key_bytes[i].clone())
                                .expect("key bytes were validated as UTF-8 in this method")
                        })
                        .collect();
                    return Err(Error::PartialConnectionLostKeys(succeeded, Box::new(error)));
                }
            }
        }

        let indices: Vec<usize> = (0..key_bytes.len()).collect();
        let retry = self
            .multi_set_pass(namespace, &key_bytes, &value_bytes, ttl_seconds, indices)
            .await;
        if retry.is_empty() {
            return Ok(());
        }
        self.maybe_refresh(true).await;
        let retry = self
            .multi_set_pass(namespace, &key_bytes, &value_bytes, ttl_seconds, retry)
            .await;
        if !retry.is_empty() {
            return Err(Error::WrongNode);
        }
        Ok(())
    }

    /// [`Self::multi_get_chunked`]'s write-side twin: one or more `o`
    /// sub-frames against `slot`'s connection, split the same way —
    /// bounded by both [`MAX_BATCH_KEYS`] and, since issue #222,
    /// cumulative wire bytes ([`chunk_lengths`]), so a batch of
    /// individually valid key/value pairs whose sum would exceed the
    /// server's `MAX_REQUEST_SIZE` is split into more sub-frames instead
    /// of sent as one oversized `o` frame.
    /// Generic over the key/value byte container (issue #233): the
    /// single-target caller passes plain `Vec<u8>`s it already owns
    /// outright, while the cluster fan-out (`multi_set_pass`) passes
    /// `Arc<[u8]>`s shared across every owner a key was replicated to —
    /// this doesn't care which, it only ever needs `&[u8]`.
    ///
    /// Issue #411: same partial-on-error contract as
    /// [`Self::multi_get_chunked`] — `Err`'s first element is every
    /// entry from the chunk(s) that landed before the one that failed.
    async fn multi_set_chunked<B: AsRef<[u8]>>(
        &self,
        slot: Option<&str>,
        namespace: &[u8],
        keys: &[B],
        values: &[B],
        ttl_seconds: u64,
    ) -> std::result::Result<Vec<MultiEntry>, (Vec<MultiEntry>, Error)> {
        let mut entries = Vec::with_capacity(keys.len());
        let lengths = chunk_lengths(namespace.len(), keys.len(), |i| {
            set_entry_cost(keys[i].as_ref(), values[i].as_ref())
        });
        let mut start = 0;
        for len in lengths {
            let key_chunk = &keys[start..start + len];
            let value_chunk = &values[start..start + len];
            start += len;
            let op = |connection: Arc<Connection>| async move {
                connection
                    .multi_set(namespace, key_chunk, value_chunk, ttl_seconds)
                    .await
            };
            match self.apply_reconnecting(slot, &op).await {
                Ok(chunk_entries) => entries.extend(chunk_entries),
                Err(error) => return Err((entries, error)),
            }
        }
        Ok(entries)
    }

    /// One pass of [`Self::set_many_bytes_in`]'s cluster routing: for
    /// every key still needing resolution (every key, on the initial
    /// pass, or just what a previous pass left unresolved), build one
    /// sub-batch per **owner name across every rank** — not just
    /// primaries, unlike [`Self::multi_get_pass`] — because within one
    /// batch the same node can be primary for one key and a replica for
    /// another; each owner therefore gets exactly one `o` sub-frame
    /// covering every key it holds in any role. Only a leg's *primary*
    /// keys can end up in the returned retry list; a leg's replica-held
    /// keys are logged-and-swallowed into `stats().replica_write_failures`
    /// instead, mirroring [`Self::write`]'s stance for single-key set. A
    /// leg that is a pure replica for every key it holds is eligible for
    /// `fire_and_forget_replicas`, exactly like a single-key replica
    /// write — see [`Self::run_multi_set_leg`]. Infallible by design: a
    /// leg's connection-level failure is always swallowed into the retry
    /// list or `stats().replica_write_failures`, never propagated —
    /// matching every other batched-set SDK in this repo.
    async fn multi_set_pass(
        &self,
        namespace: &[u8],
        key_bytes: &[Vec<u8>],
        value_bytes: &[Vec<u8>],
        ttl_seconds: u64,
        indices: Vec<usize>,
    ) -> Vec<usize> {
        let mut owners: HashMap<String, MultiSetOwnerBatch> = HashMap::new();
        let mut retry = Vec::new();
        {
            let state = self.inner.state.lock().await;
            for idx in indices {
                let names = Self::owner_names(&state, namespace, &key_bytes[idx]);
                if names.is_empty() {
                    retry.push(idx);
                    continue;
                }
                // Issue #233: the `Arc`s are built once per key here, then
                // just refcount-cloned into every owner below — see
                // `MultiSetOwnerBatch`'s doc comment for why that matters
                // at replication > 1.
                let shared_key: Arc<[u8]> = Arc::from(key_bytes[idx].as_slice());
                let shared_value: Arc<[u8]> = Arc::from(value_bytes[idx].as_slice());
                for (rank, name) in names.into_iter().enumerate() {
                    let batch = owners.entry(name).or_default();
                    batch.indices.push(idx);
                    batch.is_primary.push(rank == 0);
                    batch.keys.push(Arc::clone(&shared_key));
                    batch.values.push(Arc::clone(&shared_value));
                }
            }
        }

        let mut joined = Vec::with_capacity(owners.len());
        for (name, batch) in owners {
            let pure_replica = !batch.is_primary.iter().any(|&primary| primary);

            // Fire-and-forget replica writes: with fire_and_forget_replicas,
            // up to background_replica_cap legs run detached on their own
            // tokio task instead of being awaited below — mirrors
            // `replicate_writes`'s own fire-and-forget branch exactly,
            // including its close()-race fallback.
            if self.inner.fire_and_forget_replicas && pure_replica {
                if let Ok(permit) =
                    Arc::clone(&self.inner.background_replica_permits).try_acquire_owned()
                {
                    if self.inner.closed.load(Ordering::SeqCst) {
                        drop(permit);
                    } else {
                        let client = self.clone();
                        let owned_namespace: Arc<[u8]> = Arc::from(namespace.to_vec());
                        tokio::spawn(async move {
                            let _permit = permit; // held until this task finishes
                            let _ = client
                                .run_multi_set_leg(&owned_namespace, &name, &batch, ttl_seconds)
                                .await;
                        });
                        continue;
                    }
                }
            }

            let client = self.clone();
            let owned_namespace = namespace.to_vec();
            joined.push(async move {
                client
                    .run_multi_set_leg(&owned_namespace, &name, &batch, ttl_seconds)
                    .await
            });
        }

        for mut leg_retry in futures_util::future::join_all(joined).await {
            retry.append(&mut leg_retry);
        }
        retry
    }

    /// Applies one `multi_set_chunked` entry's outcome for one key in a
    /// leg: only a primary-held wrong-node key is pushed onto `retry`;
    /// a replica-held key's wrong-node (or, from
    /// [`Self::run_multi_set_leg`]'s unresolved-tail path, outright
    /// connection failure) is instead counted into
    /// `stats().replica_write_failures`, mirroring [`Self::write`]'s own
    /// stance for single-key set. Factored out so both the
    /// fully-resolved and (issue #411) partially-resolved paths of
    /// `run_multi_set_leg` apply the exact same rule to a chunk that DID
    /// get a response — the fix for #411's stats-overcount report is
    /// precisely that a chunk which already succeeded must go through
    /// this, not through the unresolved-tail's blanket "count everything
    /// as failed" branch.
    fn apply_multi_set_leg_entry(
        &self,
        idx: usize,
        is_primary: bool,
        entry: &MultiEntry,
        retry: &mut Vec<usize>,
    ) {
        let wrong_node = matches!(entry, MultiEntry::WrongNode);
        if !is_primary {
            if wrong_node {
                self.inner
                    .stats
                    .replica_write_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        if wrong_node {
            retry.push(idx);
        }
    }

    /// Dispatches one owner's `o` sub-batch (via
    /// [`Self::multi_set_chunked`]) and returns the indices that need
    /// retrying: only primary-held keys can end up in the returned list;
    /// every replica-held key's failure or `W` is counted in
    /// `stats().replica_write_failures` instead, mirroring
    /// [`Self::write`]'s own stance for single-key set.
    ///
    /// Issue #411: a connection-level failure only affects the chunk(s)
    /// that never got a response — `multi_set_chunked`'s `Err` carries
    /// every entry from the chunk(s) that landed first, and those go
    /// through the exact same [`Self::apply_multi_set_leg_entry`] rule
    /// the fully-resolved path uses. Only the unresolved tail is treated
    /// key-by-key as an outright failure (a connection-level failure
    /// doesn't distinguish primary- from replica-held keys within the
    /// SAME sub-frame). Before this fix, ANY chunk failing counted the
    /// whole leg — including keys an earlier chunk had already stored —
    /// as failed, double-counting `stats().replica_write_failures` and
    /// needlessly retrying already-stored primary keys.
    async fn run_multi_set_leg(
        &self,
        namespace: &[u8],
        name: &str,
        batch: &MultiSetOwnerBatch,
        ttl_seconds: u64,
    ) -> Vec<usize> {
        let mut retry = Vec::new();
        match self
            .multi_set_chunked(
                Some(name),
                namespace,
                &batch.keys,
                &batch.values,
                ttl_seconds,
            )
            .await
        {
            Ok(entries) => {
                for ((&idx, &is_primary), entry) in
                    batch.indices.iter().zip(&batch.is_primary).zip(&entries)
                {
                    self.apply_multi_set_leg_entry(idx, is_primary, entry, &mut retry);
                }
            }
            Err((partial_entries, _connection_failure)) => {
                let resolved = partial_entries.len();
                for ((&idx, &is_primary), entry) in batch
                    .indices
                    .iter()
                    .zip(&batch.is_primary)
                    .zip(&partial_entries)
                {
                    self.apply_multi_set_leg_entry(idx, is_primary, entry, &mut retry);
                }
                for i in resolved..batch.indices.len() {
                    if batch.is_primary[i] {
                        retry.push(batch.indices[i]);
                    } else {
                        self.inner
                            .stats
                            .replica_write_failures
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        retry
    }

    /// Atomically adds `delta` (negative decrements — there is no separate
    /// wire opcode; see [`Self::decr`]) to the integer counter stored at
    /// `key`, and returns its new value — or `None` if the key is missing
    /// or expired, exactly like [`Self::get`]/[`Self::get_bytes`]'s own
    /// miss convention (issue #129's `N`). [`Error::NotNumeric`] if the
    /// key exists but its stored value isn't a plain signed-decimal
    /// integer, or applying `delta` would overflow `i64` (issue #129's
    /// `T`).
    ///
    /// **Incompatible with value compression** (issue #321):
    /// [`Error::CompressionIncompatible`] immediately, before any I/O, if
    /// this client was built with `compress(true)` — disable `compress` or
    /// use a separate client for counters.
    ///
    /// **As volatile as [`Self::set`], not a durable counter**: LRU
    /// eviction and TTL expiry reclaim an incremented value exactly like
    /// any other entry. Good for rate limiting or approximate counters;
    /// never for a count that must survive eviction (billing, inventory).
    ///
    /// In cluster mode, only the key's primary owner ever runs the
    /// increment — replicas receive its literal new value as an ordinary
    /// `set` instead of replaying the increment themselves, which is what
    /// keeps every replica byte-identical to the primary rather than
    /// letting them drift (see `incr_once`'s own doc comment for the full
    /// reasoning).
    ///
    /// **At-least-once, not exactly-once, under connection loss** (issue
    /// #225): `incr`/`decr` are not idempotent — replaying one would
    /// double-apply `delta` — so unlike `get`/`set`/`delete`, this SDK
    /// never silently retries an increment whose request had already been
    /// fully written to the socket when the connection was lost, since
    /// the server may have already applied it before the reply went
    /// missing. That case surfaces as a plain `Err(Error::ConnectionLost)`
    /// rather than a redial-and-retry; only a connection already dead
    /// *before* this call's request could reach it (the idle-FIN case) is
    /// retried, since nothing could have been applied yet. On
    /// `Err(Error::ConnectionLost)`, the counter may or may not have
    /// actually changed — check with a subsequent `get` if that matters
    /// to the caller.
    pub async fn incr(&self, key: impl AsRef<[u8]>, delta: i64) -> Result<Option<i64>> {
        self.incr_in(DEFAULT_NAMESPACE, key, delta).await
    }

    /// `incr(key, -delta)` — decrementing is never a separate wire opcode
    /// (issue #129's `i` is the only INCR frame), just a negated delta on
    /// the same one. `Err(Error::InvalidArgument)` for `delta ==
    /// i64::MIN`, which has no valid `i64` negation.
    pub async fn decr(&self, key: impl AsRef<[u8]>, delta: i64) -> Result<Option<i64>> {
        self.incr(key, negate_delta(delta)?).await
    }

    /// The shared implementation behind [`Self::incr`] and
    /// [`Namespace::incr`] (Namespaces, issue #105).
    ///
    /// Rejects outright, before any validation or I/O, when this client
    /// was built with `compress(true)` (issue #321): value compression has
    /// no marker byte on an INCR result, so a compressed replica write or a
    /// later `get` of the incremented key could never round-trip safely.
    async fn incr_in(
        &self,
        namespace: &[u8],
        key: impl AsRef<[u8]>,
        delta: i64,
    ) -> Result<Option<i64>> {
        if self.inner.compress {
            return Err(Error::CompressionIncompatible);
        }
        let key = key.as_ref();
        validate_key(namespace, key)?;
        self.before_operation().await?;
        self.with_cluster_retry(|| self.incr_once(namespace, key, delta))
            .await
    }

    /// The primary-then-replicate-result driver behind `incr_in` (issue
    /// #129). Deliberately *not* `write`'s same-op-to-every-owner shape:
    /// `i` goes to the primary only, and only once that succeeds does its
    /// literal new value get forwarded to the remaining owners as a `set`
    /// (`replicate_writes`, shared with `write`) — never replayed as
    /// another `i` there. Replaying the increment on a replica instead of
    /// forwarding the primary's result would let that replica drift from
    /// the primary (e.g. an earlier replica-leg write dropped after a
    /// transient failure, or the replica separately evicting and resetting
    /// the key) — forwarding the absolute result keeps every replica
    /// byte-identical to the primary, exactly like the node's own
    /// migration/decommission-handoff logic does server-side.
    ///
    /// A `NotFound`/`NotNumeric` primary outcome is returned as-is without
    /// touching any replica: nothing was written on the primary, so there
    /// is nothing to forward. On `WrongNode`/`ConnectionLost` from the
    /// primary leg, this propagates the error untouched — `with_cluster_retry`
    /// (in `incr_in`) is what refreshes the node list and retries the
    /// whole of this method once, which naturally retries only the
    /// primary leg again (replicas are never touched until a primary
    /// succeeds).
    async fn incr_once(&self, namespace: &[u8], key: &[u8], delta: i64) -> Result<Option<i64>> {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                let op = |connection: Arc<Connection>| async move {
                    connection.incr(namespace, key, delta).await
                };
                return Ok(self
                    .apply_reconnecting_no_replay(None, &op)
                    .await?
                    .map(|(value, _ttl_seconds)| value));
            }
            Self::owner_names(&state, namespace, key)
        };

        let Some((primary, replicas)) = owners.split_first() else {
            return Err(Error::ConnectionLost(
                "nanocached: no owner is reachable for this key".to_string(),
            ));
        };

        let primary_op = |connection: Arc<Connection>| async move {
            connection.incr(namespace, key, delta).await
        };
        let Some((value, ttl_seconds)) = self
            .apply_reconnecting_no_replay(Some(primary), &primary_op)
            .await?
        else {
            return Ok(None);
        };

        let value_bytes = value.to_string().into_bytes();
        let body = WriteBody::Set {
            value: &value_bytes,
            ttl_seconds: ttl_seconds.unwrap_or(0),
        };
        self.replicate_writes(namespace, key, replicas, body).await;

        Ok(Some(value))
    }

    /// Compare-and-set (CAS, issue #141): stores `value` only if `key` is
    /// currently absent (including lazily expired) — `add`/`putIfAbsent`.
    /// Returns `true` if it was stored, `false` if the key already
    /// existed and nothing changed — a mismatch is a plain boolean
    /// outcome here, exactly like [`Self::delete`] returning `false`
    /// rather than erroring when there was nothing to delete, never an
    /// [`Error`]. `ttl_seconds == 0` means no expiry, exactly like
    /// [`Self::set`]. Transparently compresses `value` exactly like
    /// [`Self::set`] when `compress` is enabled.
    ///
    /// **This is not a distributed lock.** LRU eviction reclaims a key
    /// exactly as it would after a plain `set`, CAS or not: a key used as
    /// a lock (`put_if_absent` to acquire, a TTL to eventually release)
    /// that gets evicted under memory pressure lets a second caller's
    /// `put_if_absent` succeed while the first still believes it holds
    /// the lock — a silent double-acquisition CAS cannot detect. See
    /// docs/protocol.html#cas.
    ///
    /// **At-least-once, not exactly-once, under connection loss** (issue
    /// #225): CAS is not idempotent — replaying a `k` that already
    /// succeeded could report a now-stale condition as a mismatch — so
    /// unlike `get`/`set`/`delete`, this SDK never silently retries a CAS
    /// request whose bytes had already been fully written to the socket
    /// when the connection was lost, since the server may have already
    /// applied it before the reply went missing. That case surfaces as a
    /// plain `Err(Error::ConnectionLost)` rather than a redial-and-retry;
    /// only a connection already dead *before* this call's request could
    /// reach it (the idle-FIN case) is retried, since nothing could have
    /// been applied yet. On `Err(Error::ConnectionLost)`, whether the
    /// write actually happened is unknown — check with a subsequent
    /// `get`/`get_with_token` if that matters to the caller. This applies
    /// identically to [`Self::replace_if_present`], [`Self::replace`], and
    /// [`Self::delete_if_matches`].
    pub async fn put_if_absent(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.cas_set_in(
            DEFAULT_NAMESPACE,
            key,
            value,
            ttl_seconds,
            CasCondition::Absent,
        )
        .await
    }

    /// Compare-and-set (CAS, issue #141): stores `value` only if `key`
    /// currently holds any (unexpired) value, whatever it is — the
    /// two-argument `replace(key, value)`. Returns `true` if it was
    /// stored, `false` if the key was absent and nothing changed. See
    /// [`Self::put_if_absent`] for the shared mismatch-is-a-bool and
    /// not-a-distributed-lock and at-least-once-under-connection-loss
    /// notes, which apply here identically.
    pub async fn replace_if_present(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.cas_set_in(
            DEFAULT_NAMESPACE,
            key,
            value,
            ttl_seconds,
            CasCondition::Present,
        )
        .await
    }

    /// Compare-and-set (CAS, issue #141): stores `new_value` only if
    /// `key` currently holds an unexpired value whose content digest
    /// equals `expected` exactly — the three-argument `replace(key, old,
    /// new)`. Returns `true` if it was stored, `false` if the key's
    /// current value (or its absence) didn't match and nothing changed.
    /// See [`Self::put_if_absent`] for the shared mismatch-is-a-bool and
    /// not-a-distributed-lock and at-least-once-under-connection-loss
    /// notes, which apply here identically.
    ///
    /// `expected` accepts a [`CasToken`] from a prior
    /// [`Self::get_with_token`] — always correct, since it hashes the
    /// same wire bytes the server itself compares against — or a bare
    /// `[u8; 16]` digest computed directly via [`content_digest`] from a
    /// value this caller already holds. That second path is exactly as
    /// sensitive to encoding as memcached's own value-based CAS: it is
    /// only correct if re-serializing (and, with `compress` enabled,
    /// re-compressing) that value reproduces byte-identical output to
    /// what the server actually stores — true within one client sharing
    /// one serializer/compressor, not guaranteed across languages with
    /// client-side compression enabled. Prefer reading the token back via
    /// `get_with_token` whenever the caller has one available.
    pub async fn replace(
        &self,
        key: impl AsRef<[u8]>,
        expected: impl Into<CasToken>,
        new_value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.cas_set_in(
            DEFAULT_NAMESPACE,
            key,
            new_value,
            ttl_seconds,
            CasCondition::Digest(expected.into().digest()),
        )
        .await
    }

    /// The shared implementation behind [`Self::put_if_absent`]/
    /// [`Self::replace_if_present`]/[`Self::replace`] and their
    /// [`Namespace`] counterparts (Namespaces, issue #105).
    async fn cas_set_in(
        &self,
        namespace: &[u8],
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
        condition: CasCondition,
    ) -> Result<bool> {
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
        validate_key_and_value(namespace, key, value)?;
        self.before_operation().await?;
        self.with_cluster_retry(|| self.cas_set_once(namespace, key, value, ttl_seconds, condition))
            .await
    }

    /// The primary-then-replicate-result driver behind `cas_set_in`
    /// (compare-and-set, issue #141) — mirrors `incr_once`'s shape
    /// exactly (see its own doc comment for the full reasoning): `k`
    /// goes to the primary only, and only once it succeeds does the
    /// literal value it just stored get forwarded to the remaining
    /// owners as a `set` (`replicate_writes`, shared with `write` and
    /// `incr_once`) — never replayed as another `k` there, since a
    /// replica evaluating `condition` against its own possibly-different
    /// copy could reach a different outcome than the primary just did. A
    /// mismatch (`Ok(false)`) touches no replica at all: nothing was
    /// written on the primary, so there is nothing to forward.
    async fn cas_set_once(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
        condition: CasCondition,
    ) -> Result<bool> {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                let op = |connection: Arc<Connection>| async move {
                    connection
                        .cas_set(namespace, key, value, condition, ttl_seconds)
                        .await
                };
                return self.apply_reconnecting_no_replay(None, &op).await;
            }
            Self::owner_names(&state, namespace, key)
        };

        let Some((primary, replicas)) = owners.split_first() else {
            return Err(Error::ConnectionLost(
                "nanocached: no owner is reachable for this key".to_string(),
            ));
        };

        let primary_op = |connection: Arc<Connection>| async move {
            connection
                .cas_set(namespace, key, value, condition, ttl_seconds)
                .await
        };
        let stored = self
            .apply_reconnecting_no_replay(Some(primary), &primary_op)
            .await?;
        if !stored {
            return Ok(false);
        }

        let body = WriteBody::Set { value, ttl_seconds };
        self.replicate_writes(namespace, key, replicas, body).await;

        Ok(true)
    }

    /// Compare-and-set (CAS, issue #141): removes `key` only if it
    /// currently holds an unexpired value whose content digest equals
    /// `expected` exactly — the two-argument `remove(key, old)`. Returns
    /// `true` if it was removed, `false` on a digest mismatch or a
    /// missing key — a mismatch is a plain boolean outcome, never an
    /// [`Error`], exactly like [`Self::delete`]'s own hit/miss
    /// convention. See [`Self::replace`]'s doc comment for `expected`'s
    /// [`CasToken`]-or-raw-digest acceptance and its encoding caveat, and
    /// [`Self::put_if_absent`]'s for the not-a-distributed-lock and
    /// at-least-once-under-connection-loss notes — both apply here
    /// identically (issue #225): a `Err(Error::ConnectionLost)` from this
    /// method means the delete may or may not have actually happened, and
    /// is never silently retried once its request was fully written.
    pub async fn delete_if_matches(
        &self,
        key: impl AsRef<[u8]>,
        expected: impl Into<CasToken>,
    ) -> Result<bool> {
        self.cas_delete_in(DEFAULT_NAMESPACE, key, expected.into().digest())
            .await
    }

    /// The shared implementation behind [`Self::delete_if_matches`] and
    /// [`Namespace::delete_if_matches`] (Namespaces, issue #105).
    async fn cas_delete_in(
        &self,
        namespace: &[u8],
        key: impl AsRef<[u8]>,
        digest: [u8; 16],
    ) -> Result<bool> {
        let key = key.as_ref();
        validate_key(namespace, key)?;
        self.before_operation().await?;
        self.with_cluster_retry(|| self.cas_delete_once(namespace, key, digest))
            .await
    }

    /// The primary-then-replicate-result driver behind `cas_delete_in`
    /// (compare-and-set, issue #141) — see `cas_set_once`'s doc comment
    /// for the shared reasoning; a success here replicates as a plain
    /// `delete` instead of a `set`.
    async fn cas_delete_once(
        &self,
        namespace: &[u8],
        key: &[u8],
        digest: [u8; 16],
    ) -> Result<bool> {
        let owners = {
            let state = self.inner.state.lock().await;
            if let Target::Single { .. } = state.target {
                drop(state);
                let op = |connection: Arc<Connection>| async move {
                    connection.cas_delete(namespace, key, digest).await
                };
                return self.apply_reconnecting_no_replay(None, &op).await;
            }
            Self::owner_names(&state, namespace, key)
        };

        let Some((primary, replicas)) = owners.split_first() else {
            return Err(Error::ConnectionLost(
                "nanocached: no owner is reachable for this key".to_string(),
            ));
        };

        let primary_op = |connection: Arc<Connection>| async move {
            connection.cas_delete(namespace, key, digest).await
        };
        let deleted = self
            .apply_reconnecting_no_replay(Some(primary), &primary_op)
            .await?;
        if !deleted {
            return Ok(false);
        }

        self.replicate_writes(namespace, key, replicas, WriteBody::Delete)
            .await;

        Ok(true)
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
    ///
    /// Proxy mode (issue #122) gets its own branch here rather than
    /// reusing the cluster one: `state.target` is always `Target::Single`
    /// in proxy mode (a proxy is single-connection to this client), so
    /// `clustered` below is always false for it — without this branch a
    /// `ConnectionLost` would simply propagate after `apply_reconnecting`'s
    /// own same-address redial already failed, exactly like a genuine
    /// standalone node, with no way to fail over to a different proxy at
    /// all. `reconnect_proxy` is that failover: it re-fetches the roster
    /// and swaps onto another, randomly chosen, reachable proxy before
    /// this retries the operation once more. `WrongNode` is not
    /// special-cased here — a well-behaved proxy never sends one (it owns
    /// every key), so one arriving anyway propagates rather than looping.
    /// A `ConnectionLostAfterSend` (issue #225) is deliberately excluded
    /// from every retry branch below — the outer refresh-and-retry this
    /// method performs would otherwise re-run a non-idempotent operation's
    /// `operation()` a second time (a *different* replay risk than
    /// `apply_reconnecting_no_replay`'s own, one layer up) after the
    /// server may already have applied the first attempt. It falls
    /// straight to the final arm instead, and — like every other error —
    /// is downgraded to a plain `ConnectionLost` before returning, so this
    /// method's own caller-visible error type never depends on which of
    /// the two variants actually occurred.
    async fn with_cluster_retry<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let result = match operation().await {
            Ok(value) => Ok(value),
            Err(Error::ConnectionLost(_)) if self.inner.via_proxy => {
                self.reconnect_proxy().await;
                operation().await
            }
            Err(error @ (Error::WrongNode | Error::ConnectionLost(_))) => {
                let clustered =
                    matches!(self.inner.state.lock().await.target, Target::Cluster { .. });
                if !clustered {
                    Err(error)
                } else {
                    self.maybe_refresh(true).await;
                    operation().await
                }
            }
            Err(error) => Err(error),
        };
        result.map_err(Self::downgrade_sent_error)
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
    /// never cancelled, and drained by `close()`. Once `hedged_reads`
    /// already holds `hedge_loser_cap` legs, though, this method's own
    /// still-outstanding legs are awaited synchronously instead of left
    /// detached (`resolve_hedge_losers`, issue #276) — the read still
    /// completes correctly, it just stops accumulating background tasks.
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
                Ok(Some(value)) => {
                    self.resolve_hedge_losers(&mut rx, in_flight).await;
                    return Ok(Some(value));
                }
                Ok(None) if outcome.index == 0 => {
                    self.resolve_hedge_losers(&mut rx, in_flight).await;
                    return Ok(None);
                }
                Ok(None) => replica_missed = true,
                Err(Error::WrongNode) => {
                    self.resolve_hedge_losers(&mut rx, in_flight).await;
                    return Err(Error::WrongNode);
                }
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

    /// Issue #276: `read_hedged` has already decided its answer, with
    /// `in_flight` of its own legs still running (already spawned into
    /// `hedged_reads` by `spawn_hedge_leg`, so counted in its length).
    /// Normally they're simply left running — detached, in the
    /// background, for `close()` to eventually drain, same as always.
    /// But past `hedge_loser_cap` concurrently tracked legs (checked
    /// against `hedged_reads`' own length, which still counts these
    /// `in_flight` legs at this point), leaving more of them detached
    /// would let one persistently slow owner accumulate one abandoned
    /// task per hedged call forever. Instead, this read's own remaining
    /// legs are drained right here — via `rx`, the same channel every
    /// leg already reports its outcome to — before returning, so the
    /// call still completes correctly, it just stops detaching. Outcomes
    /// are discarded either way: a loser's result was never going to be
    /// used.
    async fn resolve_hedge_losers(
        &self,
        rx: &mut mpsc::UnboundedReceiver<HedgeOutcome>,
        in_flight: usize,
    ) {
        if in_flight == 0 {
            return;
        }
        if self.inner.hedged_reads.lock().await.len() < self.inner.hedge_loser_cap {
            return;
        }
        for _ in 0..in_flight {
            let _ = rx.recv().await;
        }
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
        // Reap legs that finished since the last time anyone looked
        // (issue #180): nothing else calls `join_next` outside `close()`,
        // so without this the JoinSet grows for the lifetime of the
        // client on any long-lived hedged-read workload. `try_join_next`
        // never awaits, so this can't stall a leg that's still running,
        // and it runs before the `closed` recheck below — reaping is
        // unconditional and has no bearing on the close/closed ordering
        // that recheck protects.
        while hedged.try_join_next().is_some() {}
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

        // Fan out to the replicas concurrently with the primary write —
        // `set`/`delete` are idempotent, so replaying the same op on a
        // replica regardless of how the primary leg turns out is safe.
        // `replicate_writes` is shared with `incr_once` (issue #129),
        // which instead awaits it only *after* its own primary leg has
        // already succeeded — see that method's own doc comment for why
        // incr can't reuse this concurrent-with-primary shape.
        let primary_write = self.apply_reconnecting(Some(primary), &op);
        let replica_writes = self.replicate_writes(namespace, key, replicas, body);

        let (primary_result, ()) = tokio::join!(primary_write, replica_writes);
        primary_result
    }

    /// Fans `body` (a literal `set` or `delete`) out to `replicas`,
    /// best-effort: every failure is swallowed and counted in
    /// `stats().replica_write_failures` so operators can spot silently
    /// degrading replication, never fails the caller (client-side
    /// replication) — a dead or disagreeing replica just leaves the key
    /// under-replicated until the next node-list refresh.
    ///
    /// Fire-and-forget replica writes: with `fire_and_forget_replicas`, up
    /// to `background_replica_cap` legs run detached on their own tokio
    /// task instead of being awaited below — past that cap, further legs
    /// fall back to the synchronous path exactly as with the option off.
    ///
    /// Issue #424: the synchronous path (`fire_and_forget_replicas` off,
    /// or the background-replica permit pool exhausted) fans every leg
    /// out concurrently with `join_all`, matching
    /// [`Self::clear_fanout_once`]'s own N-node fan-out — a write at
    /// replication factor R pays `max()` of one round trip beyond the
    /// primary, not R-1 sequential ones. Per-leg error/stat handling is
    /// unchanged: each leg's own `apply_reconnecting` outcome still
    /// counts into `stats().replica_write_failures` independently.
    ///
    /// Shared between `write` (called concurrently with the primary leg,
    /// since `set`/`delete` are idempotent and safe to replay regardless
    /// of the primary's own outcome) and `incr_once` (issue #129, called
    /// only once the primary's `i` has already produced a value — an
    /// `incr` replica leg is always a `set` of that literal value, never
    /// another `i`, so this only ever sees `WriteBody::Set` from that
    /// caller).
    async fn replicate_writes(
        &self,
        namespace: &[u8],
        key: &[u8],
        replicas: &[String],
        body: WriteBody<'_>,
    ) {
        // Names that end up needing the synchronous path this call
        // (fire-and-forget off, or its permit pool was exhausted this
        // time) — collected instead of awaited inline in the loop below
        // so they can be fanned out concurrently afterward (issue #424).
        let mut sync_names: Vec<&str> = Vec::new();
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
            sync_names.push(name.as_str());
        }

        if sync_names.is_empty() {
            return;
        }
        let op = |connection: Arc<Connection>| {
            let body = &body;
            async move {
                match body {
                    WriteBody::Set { value, ttl_seconds } => {
                        connection.set(namespace, key, value, *ttl_seconds).await
                    }
                    WriteBody::Delete => connection.delete(namespace, key).await.map(|_| ()),
                }
            }
        };
        let outcomes = futures_util::future::join_all(
            sync_names
                .iter()
                .map(|&name| self.apply_reconnecting(Some(name), &op)),
        )
        .await;
        for outcome in outcomes {
            if outcome.is_err() {
                self.inner
                    .stats
                    .replica_write_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
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
    /// get/set/delete/clear are all idempotent — replaying one that
    /// actually reached the server before the reply was lost has no
    /// observable effect, so this retries on `ConnectionLostAfterSend`
    /// (issue #225) exactly the same as a plain `ConnectionLost`, and
    /// downgrades either variant back to a plain `ConnectionLost` before
    /// returning, so callers never see the internal distinction. `slot` is
    /// `None` in single mode.
    ///
    /// `incr`/`decr`, the CAS methods, and `delete_if_matches` are NOT
    /// idempotent and must never call this — see
    /// [`Self::apply_reconnecting_no_replay`].
    async fn apply_reconnecting<T, F, Fut>(&self, slot: Option<&str>, op: &F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let result = match op(self.slot_connection(slot).await?).await {
            Err(Error::ConnectionLost(_)) | Err(Error::ConnectionLostAfterSend(_)) => {
                op(self.slot_connection(slot).await?).await
            }
            outcome => outcome,
        };
        result.map_err(Self::downgrade_sent_error)
    }

    /// As [`Self::apply_reconnecting`], but for the non-idempotent
    /// operations — `incr`/`decr`, `put_if_absent`/`replace_if_present`/
    /// `replace`, and `delete_if_matches` (issue #225): replaying one of
    /// these after the server already applied it would double-apply the
    /// increment, or report an already-successful CAS as a mismatch. So
    /// the redial-and-retry only fires for a plain `ConnectionLost` — the
    /// request's frame was never fully written (the connection was
    /// already dead, the idle-FIN case), so nothing could have reached the
    /// server yet, exactly as safe to replay as get/set/delete's own case.
    /// `ConnectionLostAfterSend` — the frame WAS written and the reply is
    /// simply unknown — is never replayed: the server may already have
    /// applied it, so this returns immediately instead. Deliberately
    /// *not* downgraded to a plain `ConnectionLost` here (unlike
    /// [`Self::apply_reconnecting`]): `with_cluster_retry`, this method's
    /// only caller's caller, needs the undowngraded variant to likewise
    /// skip its own whole-operation retry for the same reason — it
    /// downgrades back to a plain `ConnectionLost` itself once that
    /// decision is made, so the caller-visible error type is identical
    /// either way. This makes these four methods at-least-once (not
    /// exactly-once) under connection loss, never worse — see their own
    /// doc comments.
    async fn apply_reconnecting_no_replay<T, F, Fut>(&self, slot: Option<&str>, op: &F) -> Result<T>
    where
        F: Fn(Arc<Connection>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        match op(self.slot_connection(slot).await?).await {
            Err(Error::ConnectionLost(_)) => op(self.slot_connection(slot).await?).await,
            outcome => outcome,
        }
    }

    /// Collapses the internal [`Error::ConnectionLostAfterSend`] signal
    /// back into a plain [`Error::ConnectionLost`] — see that variant's
    /// doc comment. A no-op for every other error, `Ok` included.
    fn downgrade_sent_error(error: Error) -> Error {
        match error {
            Error::ConnectionLostAfterSend(message) => Error::ConnectionLost(message),
            other => other,
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
            Arc::clone(&self.inner.stats.transient_retries),
            self.inner.request_timeout,
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
            DiscoveryQuery::Nodes,
        )
        .await?;
        match identified {
            Identified::Node { stream, tagged } => {
                if self.is_closed() {
                    return Err(Error::AlreadyClosed);
                }
                Ok((stream, tagged))
            }
            // `address` is always a node's own address here (a plain
            // single-node target, a cluster member, or — proxy mode,
            // issue #122 — the one proxy this client is pinned to): any
            // other answer means it stopped being a cache node underneath
            // this client, same treatment either way.
            Identified::Cluster { .. } | Identified::Proxies { .. } => Err(Error::Protocol(
                format!("nanocached: {address} no longer identifies as a cache node"),
            )),
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
                DiscoveryQuery::Nodes,
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

    /// Proxy mode reconnect-on-loss, second half (issue #122; see
    /// `Options::via_proxy`'s doc comment). Called from `with_cluster_retry`
    /// once the same-proxy redial already inside `apply_reconnecting`'s
    /// own one-shot retry has failed — the proxy itself is down, not just
    /// this one socket — so this re-fetches the proxy roster from the
    /// configured discovery address(es) and, if a reachable one turns up,
    /// swaps `state.target` onto it exactly like `connect_via_proxy` built
    /// it in the first place.
    ///
    /// Swallows every failure the same way `refresh_node_list` does: no
    /// error escapes this method itself (counted in
    /// `stats().refresh_failures` instead), leaving the already-dead
    /// connection in place — the caller's own retry runs against that
    /// same stale target and simply fails again with a fresh dial error,
    /// which is exactly what should surface when discovery itself has
    /// nothing left to offer.
    async fn reconnect_proxy(&self) {
        let Some((address, stream, tagged)) = self.dial_a_fresh_proxy().await else {
            self.inner
                .stats
                .refresh_failures
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let connection = Arc::new(Connection::new(
            stream,
            self.inner.tracking_key.clone(),
            tagged,
            Arc::clone(&self.inner.stats.transient_retries),
            self.inner.request_timeout,
        ));

        let mut state = self.inner.state.lock().await;
        if self.inner.closed.load(Ordering::SeqCst) {
            // close() ran while this was dialing (issue #10, mirrored
            // here exactly as in `slot_connection`): installing this
            // connection now would leak it past teardown.
            connection.close();
            return;
        }
        let departed_address = if let Target::Single {
            address: current_address,
            connection: current_connection,
        } = &mut state.target
        {
            current_connection.close();
            let departed = std::mem::replace(current_address, address);
            *current_connection = connection;
            Some(departed)
        } else {
            None
        };
        drop(state);

        // Issue #296: the address just abandoned by this swap is never
        // dialed again through this client's own redial path (proxy mode
        // pins to exactly one address at a time), so a reconnect-cooldown
        // entry left behind for it — set if the same-address redial that
        // triggered this failover had failed — would otherwise sit in
        // `reconnect_cooldowns` forever. `refresh_node_list`'s own prune
        // never runs here: `maybe_refresh` early-returns for
        // `Target::Single`, which proxy mode always is. Also covers the
        // (harmless but tidy) case where the freshly dialed address is
        // the same one abandoned — direct proxy dials never consult this
        // map, so an old cooldown entry for it would otherwise linger
        // despite the address being demonstrably reachable again.
        if let Some(departed_address) = departed_address {
            let mut cooldowns = self.inner.reconnect_cooldowns.lock().await;
            cooldowns.remove(&departed_address);
        }
    }

    /// Re-fetches the proxy roster from every configured discovery
    /// address in turn (discovery HA, mirroring `fetch_node_list`) and
    /// dials a random, reachable entry from the first non-empty roster —
    /// see `dial_random_proxy`. `None` once every address has been tried
    /// and none yielded a reachable proxy.
    async fn dial_a_fresh_proxy(&self) -> Option<(String, crate::identify::Stream, bool)> {
        for (host, port) in &self.inner.addresses {
            if let Ok(Identified::Proxies { proxies }) = connect_and_identify(
                host,
                *port,
                self.inner.auth_secret_bytes(),
                self.inner.tls.as_ref(),
                CONNECT_DEADLINE,
                DiscoveryQuery::Proxies,
            )
            .await
            {
                if !proxies.is_empty() {
                    if let Some(result) = dial_random_proxy(
                        &proxies,
                        self.inner.auth_secret_bytes(),
                        self.inner.tls.as_ref(),
                    )
                    .await
                    {
                        return Some(result);
                    }
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

    /// See [`NanocachedClient::get_many`]; scoped to this namespace
    /// (issue #151).
    pub async fn get_many<K: AsRef<str>>(&self, keys: &[K]) -> Result<HashMap<String, String>> {
        self.client.get_many_in(&self.namespace, keys).await
    }

    /// See [`NanocachedClient::get_many_bytes`]; scoped to this namespace
    /// (issue #151).
    pub async fn get_many_bytes<K: AsRef<str>>(
        &self,
        keys: &[K],
    ) -> Result<HashMap<String, Vec<u8>>> {
        self.client.get_many_bytes_in(&self.namespace, keys).await
    }

    /// See [`NanocachedClient::set_many`]; scoped to this namespace
    /// (issue #151).
    pub async fn set_many(&self, values: &HashMap<String, String>, ttl_seconds: u64) -> Result<()> {
        self.client
            .set_many_in(&self.namespace, values, ttl_seconds)
            .await
    }

    /// See [`NanocachedClient::set_many_bytes`]; scoped to this namespace
    /// (issue #151).
    pub async fn set_many_bytes(
        &self,
        values: &HashMap<String, Vec<u8>>,
        ttl_seconds: u64,
    ) -> Result<()> {
        self.client
            .set_many_bytes_in(&self.namespace, values, ttl_seconds)
            .await
    }

    /// See [`NanocachedClient::incr`]; scoped to this namespace.
    pub async fn incr(&self, key: impl AsRef<[u8]>, delta: i64) -> Result<Option<i64>> {
        self.client.incr_in(&self.namespace, key, delta).await
    }

    /// See [`NanocachedClient::decr`]; scoped to this namespace.
    pub async fn decr(&self, key: impl AsRef<[u8]>, delta: i64) -> Result<Option<i64>> {
        self.client
            .incr_in(&self.namespace, key, negate_delta(delta)?)
            .await
    }

    /// See [`NanocachedClient::get_with_token`]; scoped to this namespace.
    pub async fn get_with_token(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, CasToken)>> {
        self.client.get_with_token_in(&self.namespace, key).await
    }

    /// See [`NanocachedClient::put_if_absent`]; scoped to this namespace.
    pub async fn put_if_absent(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.client
            .cas_set_in(
                &self.namespace,
                key,
                value,
                ttl_seconds,
                CasCondition::Absent,
            )
            .await
    }

    /// See [`NanocachedClient::replace_if_present`]; scoped to this
    /// namespace.
    pub async fn replace_if_present(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.client
            .cas_set_in(
                &self.namespace,
                key,
                value,
                ttl_seconds,
                CasCondition::Present,
            )
            .await
    }

    /// See [`NanocachedClient::replace`]; scoped to this namespace.
    pub async fn replace(
        &self,
        key: impl AsRef<[u8]>,
        expected: impl Into<CasToken>,
        new_value: impl AsRef<[u8]>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.client
            .cas_set_in(
                &self.namespace,
                key,
                new_value,
                ttl_seconds,
                CasCondition::Digest(expected.into().digest()),
            )
            .await
    }

    /// See [`NanocachedClient::delete_if_matches`]; scoped to this
    /// namespace.
    pub async fn delete_if_matches(
        &self,
        key: impl AsRef<[u8]>,
        expected: impl Into<CasToken>,
    ) -> Result<bool> {
        self.client
            .cas_delete_in(&self.namespace, key, expected.into().digest())
            .await
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
    use std::sync::atomic::AtomicU64;

    // issue #386: the per-value decompression cap alone lets a batch
    // amplify it by the key count (batch × 64 MiB from one small wire
    // reply); the cumulative budget must abort the batch instead. Driven
    // through decompress_for_batch with an explicit cap — never by
    // mutating a process-wide bound — so concurrently running tests can't
    // observe it (the read_one_response precedent). The budget only
    // applies when compress is enabled (issue #410b), so these tests pass
    // compress=true and marker-prefix their values (MARKER_RAW = 0x00) so
    // decompress_value accepts them as valid, uncompressed wire values.
    fn raw_marked(len: usize) -> Vec<u8> {
        let mut value = vec![0u8];
        value.extend(std::iter::repeat_n(b'x', len));
        value
    }

    #[test]
    fn a_batch_over_the_cumulative_decompression_budget_is_rejected() {
        let budget = AtomicU64::new(0);
        // First value fits and charges the budget…
        let first =
            NanocachedClient::decompress_for_batch(true, raw_marked(8), &budget, 12).unwrap();
        assert_eq!(first.len(), 8);
        // …after which a second value that pushes the cumulative total
        // past the cap is rejected — charged (issue #410a) before the
        // check, so it is the crossing entry itself that is caught, not
        // some later one.
        let error = NanocachedClient::decompress_for_batch(true, raw_marked(8), &budget, 12)
            .expect_err("second value must trip the cumulative budget");
        match error {
            Error::Decompression(message) => {
                assert!(message.contains("across the batch"), "{message}");
            }
            other => panic!("expected a decompression error, got {other:?}"),
        }
    }

    #[test]
    fn a_batch_exactly_at_the_cumulative_decompression_budget_passes() {
        let budget = AtomicU64::new(0);
        for _ in 0..2 {
            NanocachedClient::decompress_for_batch(true, raw_marked(4), &budget, 8).unwrap();
        }
        // budget == cap is not over it: charge-then-check still admits a
        // cumulative total sitting exactly at the cap (the bound is a
        // budget, not a hard ceiling below it).
        assert_eq!(budget.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn a_value_that_pushes_an_exactly_at_cap_budget_over_it_is_rejected() {
        // Regression (pass-9 audit, issue #410a): under the old
        // check-before-charge order, a budget sitting exactly at the cap
        // passed the pre-charge check, so the next value's charge could
        // push it over without ever being caught. Charge-then-check must
        // reject this crossing entry.
        let budget = AtomicU64::new(0);
        for _ in 0..2 {
            NanocachedClient::decompress_for_batch(true, raw_marked(4), &budget, 8).unwrap();
        }
        let error = NanocachedClient::decompress_for_batch(true, raw_marked(1), &budget, 8)
            .expect_err("a value pushing an at-cap budget over it must be rejected");
        match error {
            Error::Decompression(message) => {
                assert!(message.contains("across the batch"), "{message}");
            }
            other => panic!("expected a decompression error, got {other:?}"),
        }
    }

    #[test]
    fn the_crossing_entry_is_caught_even_when_it_is_the_only_and_last_one() {
        // Regression (pass-9 audit, issue #410a): the cumulative budget
        // used to be checked BEFORE charging the current entry, so the
        // entry that actually crosses the cap always slipped through
        // uncaught — and if it was the last (or only) hit in the
        // response, the guard never fired at all. Charge-then-check must
        // still catch a lone crossing entry.
        let budget = AtomicU64::new(0);
        let error = NanocachedClient::decompress_for_batch(true, raw_marked(8), &budget, 4)
            .expect_err("the sole, crossing entry must be caught");
        match error {
            Error::Decompression(message) => {
                assert!(message.contains("across the batch"), "{message}");
            }
            other => panic!("expected a decompression error, got {other:?}"),
        }
    }

    #[test]
    fn the_budget_is_not_charged_or_enforced_when_compress_is_disabled() {
        // Regression (pass-9 audit, issue #410b): the budget used to be
        // charged and enforced even when compression is disabled, so a
        // large uncompressed batch could fail with a misleading
        // "decompression bomb" error. With compress=false, decompression
        // is a no-op and plain (unmarked) bytes pass straight through,
        // unbounded by a cap far smaller than either value.
        let budget = AtomicU64::new(0);
        let value = NanocachedClient::decompress_for_batch(false, vec![b'x'; 8], &budget, 4)
            .expect("compress=false must never charge or enforce the cumulative budget");
        assert_eq!(value.len(), 8);
        let value = NanocachedClient::decompress_for_batch(false, vec![b'y'; 8], &budget, 4)
            .expect("a second large value must still pass with compress disabled");
        assert_eq!(value.len(), 8);
    }

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
