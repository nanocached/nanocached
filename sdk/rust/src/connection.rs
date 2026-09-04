//! One already-identified connection to a single nanocached-node,
//! speaking the cache protocol (`G`/`S`/`D` — the `A` identify exchange
//! happens in `identify` before a `Connection` exists). Requests are
//! pipelined onto the socket and matched to responses in send order
//! (request pipelining): a dedicated read task, spawned in `new`, consumes
//! responses and dispatches each to the oldest still-pending request,
//! since nanocached-node itself only ever answers in the order it
//! received requests. Enqueuing the pending slot and writing the frame
//! happen under one `tokio::sync::Mutex`, so concurrent callers' queue
//! order always matches the order their frames actually hit the wire.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{split, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::sync::{oneshot, watch, Mutex};

use crate::error::{Error, Result};
use crate::identify::Stream;

/// The server's own request cap is 1 MiB; this constant doubles that as
/// headroom, so a claimed length beyond it is definitely a corrupt or
/// malicious frame, never just a legitimately large value.
const MAX_VALUE_LENGTH: usize = 2 * 1024 * 1024;

/// Caps a response header line (`read_line`, shared with identify.rs's
/// discovery node-list headers) before it can grow without bound: every
/// real header line (`V <len> <tag>`, `S <tag>`, a discovery `N <count>
/// <r>`/entry line, ...) is a few dozen bytes at most, so a peer that
/// never sends the terminating `\n` is corrupt or hostile, not just slow
/// — 4 KiB is generous headroom while still bounding its memory pressure
/// on the client (issue #47 audit item R2, mirrors `MAX_VALUE_LENGTH`'s
/// rationale for the `V` body).
const MAX_HEADER_LINE_LENGTH: usize = 4 * 1024;

/// Bounds an `M` (multi-get, issue #151) reply's hit bodies summed across
/// the whole reply. Each entry's own declared length is already rejected
/// above `MAX_VALUE_LENGTH`, but that alone doesn't bound the reply as a
/// whole (issue #207, follow-up to #179's per-value fix): a node
/// answering a several-hundred-key multi-get with every entry near the
/// per-value cap could still force hundreds of MB of allocation from a
/// single reply. 64 MiB, matching `compression.rs`'s own
/// `MAX_DECOMPRESSED_LENGTH` — a wire reply legitimately needing more
/// than that in one round trip is unreasonable; callers should batch
/// smaller rather than pushing this cap up. `O` (multi-set) acks carry no
/// bodies at all — just fixed single-character tokens on one header line
/// that's already bounded by `MAX_HEADER_LINE_LENGTH` — so they need no
/// equivalent cumulative bound. Public-but-hidden purely as a test hook,
/// mirroring `REQUEST_TIMEOUT_MS` below — read fresh on every `M` reply
/// rather than once at connect, so a test that lowers it should restore
/// it immediately after the one call it means to affect.
#[doc(hidden)]
pub static MAX_MULTI_GET_RESPONSE_BYTES: AtomicUsize = AtomicUsize::new(64 * 1024 * 1024);

/// Bounds a request's full round trip (write + wait for its matched
/// response), in milliseconds: without it, a half-open server that
/// accepts the TCP connection but never writes back — or stops
/// mid-stream — would hang `get`/`set`/`delete` forever awaiting a
/// response that never comes, wedging every other pending caller behind
/// it (and, transitively, `close()`'s in-flight background-write
/// drain). Generous versus the server's own 10s outbound timeouts.
///
/// Cross-SDK note: this is a *per-request* wall-clock bound (measured
/// from when each request is issued), which is deliberately stricter than
/// the Go SDK's *connection-level, progress-based* deadline (re-armed
/// whenever any response arrives). Under very deep pipelining against a
/// slow-but-healthy server the two differ — a request that waits out this
/// whole window for its turn is timed out here even while the server is
/// still answering others. That's intentional: this wrapper's job is to
/// guarantee an abandoned queue slot (one nothing will *ever* answer) is
/// cleared and the socket released, which requires a bound tied to the
/// individual request, not to whole-connection liveness. Kept as an
/// accepted difference rather than reworked, since making it
/// progress-based would mean threading connection-wide liveness state
/// through this SDK's cancellation-safe per-request wait — a change to
/// the most concurrency-sensitive path here for a benefit that only
/// shows up at pipelining depths past this timeout.
/// Public-but-hidden purely as a test hook, mirroring
/// `client::KEEPALIVE_INTERVAL_MS` — but read fresh on every request
/// rather than once at connect, so a test that lowers it should restore
/// it immediately after the one call it means to affect, and should
/// pick a value comfortably above every other test's own simulated
/// server delays to avoid tripping over a concurrently running one.
#[doc(hidden)]
pub static REQUEST_TIMEOUT_MS: AtomicU64 = AtomicU64::new(30_000);

/// A raw response marker byte, its value bytes (`V`/`I` only), and —
/// `I` only (issue #129) — the entry's remaining TTL in seconds, if the
/// response carried one. What the read task parses off the wire, before
/// `get`/`set`/`delete`/`incr` convert it into a [`ResponseKind`] or a
/// `WrongNode`/`NotNumeric`/protocol error. The echoed tag (echoed
/// response tags), when present, is verified against the pending slot's
/// expected tag by the read loop itself and never reaches this type — see
/// `WireResponse`.
/// The fourth field is only ever `Some` for an `M`/`O` response (issue
/// #151, docs/protocol.html#multi) — every other marker leaves it
/// `None`, `value`/`ttl_seconds` unused for those two markers instead.
type RawResponse = (u8, Option<Vec<u8>>, Option<u64>, Option<Vec<MultiEntry>>);
type RawResponseSender = oneshot::Sender<Result<RawResponse>>;

/// A pending request's queue slot: the sender its response ultimately
/// resolves, plus — on a tagged connection (echoed response tags) — the tag it was
/// sent with, which the read loop checks the response's echoed tag
/// against before handing the response off. `tag` is always `None` on an
/// untagged connection, and simply unused.
struct PendingSlot {
    tag: Option<u32>,
    tx: RawResponseSender,
}

struct WriteState {
    /// `None` once poisoned (or for the pre-poisoned placeholder,
    /// `dead()`, which never opened a socket) — further requests fail
    /// as connection-lost rather than reusing a torn-down half.
    write_half: Option<WriteHalf<Stream>>,
    pending: VecDeque<PendingSlot>,
    /// Echoed response tags: this connection's tag counter, a u32 wrapping at its
    /// width — claimed under this same lock, in the same critical section
    /// that enqueues the pending slot and writes the frame, so tag order
    /// can never skew from queue/wire order (request pipelining's invariant).
    /// Unused (stays 0) on an untagged connection.
    next_tag: u32,
}

struct Shared {
    write_state: Mutex<WriteState>,
    closed: AtomicBool,
    /// Milliseconds since `epoch` of the last request — what the
    /// keep-alive timer checks against its interval.
    last_used_ms: AtomicU64,
    epoch: Instant,
    /// The open-targets key this connection was counted against (see
    /// `open_targets`) — `None` for the pre-poisoned `dead()` placeholder,
    /// which never opened a socket and so was never counted.
    tracking_key: Option<String>,
    /// Echoed response tags: negotiated during identify (see `identify::Identified`) —
    /// when true, every request carries a tag the server echoes, and the
    /// read loop verifies the echo against the oldest pending slot before
    /// resolving it.
    tagged: bool,
}

impl Shared {
    /// Flips `closed` and wakes the read task, which performs the actual
    /// async cleanup (draining pending, dropping the write half) on its
    /// own exit — see `read_loop`. Sync and safe to call more than once;
    /// only the first call has any effect, so every poison trigger
    /// (a failed write, a failed read, a caller-detected mismatch, an
    /// explicit `close()`, a write cancelled mid-flight) can call this
    /// directly without coordinating with the others.
    fn mark_closed(&self, shutdown: &watch::Sender<bool>) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(key) = &self.tracking_key {
            crate::open_targets::decrement(key);
        }
        let _ = shutdown.send(true);
    }
}

pub(crate) struct Connection {
    shared: Arc<Shared>,
    shutdown: watch::Sender<bool>,
    /// Per-request round-trip deadline (see `request`). Resolved once per
    /// client from `Options::request_timeout` (falling back to the
    /// `REQUEST_TIMEOUT_MS` static) and carried per connection, so tests can
    /// shorten it on their own client without mutating the shared static —
    /// which, read on every request while the suite runs concurrently, could
    /// time out an unrelated test's request.
    request_timeout: Duration,
    /// Retryable-error status `R` (issue #125): every `R` this connection
    /// ever receives increments this — shared with every other connection
    /// this client opens (see `NanocachedClient::connect`'s
    /// `transient_retries` and `slot_connection`/`reconnect_proxy`'s
    /// reuse of `Inner.stats.transient_retries`), so the count survives
    /// this one connection being replaced by a later redial. Never read
    /// directly here; `NanocachedClient::stats()` is the only reader.
    transient_retries: Arc<AtomicU64>,
}

pub(crate) enum ResponseKind {
    Value(Vec<u8>),
    NotFound,
    Stored,
    Deleted,
    /// `C` — issue #106's `clear`/`clear_all` ack. Neither `c` nor `F` is
    /// key-addressed, so this is the only outcome besides an error; there
    /// is no `NotFound`/`W` counterpart to distinguish (clearing an
    /// already-empty namespace still acks `C`).
    Cleared,
    /// `I` — issue #129's `incr`/`decr` ack: the entry's new counter
    /// value, plus its remaining TTL in seconds when it has one (mirrors
    /// `S`'s own optional TTL field, just on the response side). The
    /// key-missing (`N`) and not-numeric (`T`) outcomes are surfaced as
    /// [`ResponseKind::NotFound`] and [`Error::NotNumeric`] respectively,
    /// not as variants here — see `Connection::incr`.
    Incr(i64, Option<u64>),
    /// `M` (multi-get) or `O` (multi-set) — issue #151,
    /// docs/protocol.html#multi. One entry per requested key, in request
    /// order; see [`MultiEntry`].
    Multi(Vec<MultiEntry>),
}

/// One key's outcome inside an `M` (multi-get) or `O` (multi-set)
/// response (issue #151, docs/protocol.html#multi) — a batch never
/// fails as a whole, so each key's result is independent of every
/// other key's. Reused for both response kinds rather than two
/// near-identical types:
/// - `M`: [`Self::Hit`] (the value, possibly empty) or [`Self::Miss`]
///   (`-`); [`Self::WrongNode`] is a per-key `W`.
/// - `O`: [`Self::Stored`] (`S`) or [`Self::WrongNode`] (`W`); there is
///   no miss shape (`O` never uses [`Self::Hit`]/[`Self::Miss`]).
#[derive(Debug, Clone)]
pub(crate) enum MultiEntry {
    Hit(Vec<u8>),
    Miss,
    Stored,
    WrongNode,
}

/// The three `<cond>` shapes `k`/`x` accept (compare-and-set, issue
/// #141) — see `cas_condition_token`, `Connection::cas_set`, and
/// `Connection::cas_delete`. `x` only ever uses `Digest` (an
/// absent/present-only conditioned delete is already the plain,
/// unconditional `D`/`d` — see `Connection::cas_delete`'s doc comment),
/// but the type is shared with `k` rather than split in two, since
/// `Digest` is identical either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CasCondition {
    /// Wire `A`: succeeds only if the key is absent (including lazily
    /// expired) — `add`/`putIfAbsent`.
    Absent,
    /// Wire `P`: succeeds only if the key currently holds any (unexpired)
    /// value, whatever it is — the two-argument `replace(key, value)`.
    Present,
    /// Wire: the 32-character lowercase hex encoding of this digest —
    /// succeeds only if the key holds an unexpired value whose
    /// [`crate::cas::content_digest`] equals it exactly.
    Digest([u8; 16]),
}

/// The literal wire token for one of [`CasCondition`]'s three shapes:
/// `A`/`P` bare, or the digest's 32-character lowercase hex encoding
/// (reusing [`crate::cas::CasToken`]'s `Display` so the hex-encoding
/// logic isn't duplicated between the wire-writing and the public-API
/// sides of CAS).
fn cas_condition_token(condition: CasCondition) -> String {
    match condition {
        CasCondition::Absent => "A".to_string(),
        CasCondition::Present => "P".to_string(),
        CasCondition::Digest(digest) => crate::cas::CasToken::from(digest).to_string(),
    }
}

/// RAII guard around the write half of a round trip: if the enclosing
/// future is dropped while `write_all` is still pending (a caller
/// abandoning the request, e.g. via `tokio::time::timeout`), the frame
/// may be only partially on the wire — desyncing every request queued
/// behind this one too, unlike abandoning a request *after* its write
/// completed (see `Connection::request`, which leaves that case for the
/// read task to handle by simply finding no one listening). `completed`
/// is set only once `write_all` actually returns (Ok or Err); Drop while
/// it's still `false` means we were cancelled mid-write.
struct WriteGuard<'a> {
    shared: &'a Shared,
    shutdown: &'a watch::Sender<bool>,
    completed: bool,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.shared.mark_closed(self.shutdown);
        }
    }
}

/// Safety net for a `Connection` discarded without `close()`: the read
/// task only exits on the shutdown signal, so without this it — and the
/// socket it holds — would outlive the handle for as long as the server
/// kept the connection open. Every current call site does call `close()`
/// first, in which case this is a no-op (`mark_closed` is idempotent).
impl Drop for Connection {
    fn drop(&mut self) {
        self.shared.mark_closed(&self.shutdown);
    }
}

impl Connection {
    /// `tracking_key` is the client's winning connect address ("host:port"
    /// of whichever configured address answered `connect()`) — every
    /// socket the client ever opens, regardless of which node it dials,
    /// is counted against that one key (see `open_targets`). `transient_retries`
    /// is the client-wide counter (issue #125) every connection this
    /// client ever opens shares — see the field's own doc comment.
    pub(crate) fn new(
        stream: Stream,
        tracking_key: String,
        tagged: bool,
        transient_retries: Arc<AtomicU64>,
        request_timeout: Duration,
    ) -> Self {
        crate::open_targets::increment(&tracking_key);
        let (read_half, write_half) = split(stream);
        // Issue #191: every response header (`read_line`, below) was read
        // one byte at a time straight off the raw `ReadHalf`, costing a
        // syscall/poll per byte. Wrapping it here, in `BufReader::new`
        // (freshly created, never populated from an existing socket read),
        // means the buffer lives for exactly this connection's lifetime —
        // it's moved into `read_loop`, this connection's only reader (see
        // that fn's own doc comment), and dropped with it, so a poisoned
        // or dropped connection can never leak buffered bytes into another
        // one. Mirrors `identify.rs`'s `BufReader::new(stream)` for the
        // same auth/discovery header reads.
        let read_half = BufReader::new(read_half);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = Arc::new(Shared {
            write_state: Mutex::new(WriteState {
                write_half: Some(write_half),
                pending: VecDeque::new(),
                next_tag: 0,
            }),
            closed: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            tracking_key: Some(tracking_key),
            tagged,
        });

        let read_shared = Arc::clone(&shared);
        let read_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(read_loop(
            read_half,
            read_shared,
            read_shutdown_tx,
            shutdown_rx,
        ));

        Self {
            shared,
            shutdown: shutdown_tx,
            transient_retries,
            request_timeout,
        }
    }

    /// A pre-poisoned placeholder for a newly discovered node — see the
    /// `write_state` field docs. Never actually processes a request (it
    /// fails closed before touching the wire), so its own
    /// `transient_retries` counter is a throwaway, never shared with — or
    /// read back through — the client's real one.
    pub(crate) fn dead() -> Self {
        let (shutdown_tx, _) = watch::channel(true);
        Self {
            shared: Arc::new(Shared {
                write_state: Mutex::new(WriteState {
                    write_half: None,
                    pending: VecDeque::new(),
                    next_tag: 0,
                }),
                closed: AtomicBool::new(true),
                last_used_ms: AtomicU64::new(0),
                epoch: Instant::now(),
                tracking_key: None,
                tagged: false,
            }),
            shutdown: shutdown_tx,
            transient_retries: Arc::new(AtomicU64::new(0)),
            // Never runs a request (fails closed first), so the value is
            // immaterial; the static default keeps it self-consistent.
            request_timeout: Duration::from_millis(REQUEST_TIMEOUT_MS.load(Ordering::SeqCst)),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn close(&self) {
        self.shared.mark_closed(&self.shutdown);
    }

    pub(crate) fn idle(&self) -> Duration {
        let last = self.shared.last_used_ms.load(Ordering::SeqCst);
        self.shared
            .epoch
            .elapsed()
            .saturating_sub(Duration::from_millis(last))
    }

    /// `namespace` empty means the default namespace — see `encode_get`
    /// for the legacy/namespaced wire split (issue #105).
    pub(crate) async fn get(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.request(|tag| encode_get(namespace, key, tag)).await? {
            ResponseKind::Value(value) => Ok(Some(value)),
            ResponseKind::NotFound => Ok(None),
            other => Err(self.mismatch(&other)),
        }
    }

    /// `ttl_seconds == 0` means no expiry — mapped to the wire exactly as
    /// the absent-TTL frame always was. `namespace` empty means the
    /// default namespace — see `encode_set` (issue #105).
    pub(crate) async fn set(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> Result<()> {
        match self
            .request(|tag| encode_set(namespace, key, value, ttl_seconds, tag))
            .await?
        {
            ResponseKind::Stored => Ok(()),
            other => Err(self.mismatch(&other)),
        }
    }

    /// `namespace` empty means the default namespace — see `encode_delete`
    /// (issue #105).
    pub(crate) async fn delete(&self, namespace: &[u8], key: &[u8]) -> Result<bool> {
        match self
            .request(|tag| encode_delete(namespace, key, tag))
            .await?
        {
            ResponseKind::Deleted => Ok(true),
            ResponseKind::NotFound => Ok(false),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Drops every entry in `namespace` on this one node (issue #106's
    /// `c`) — `namespace` empty clears the default namespace. Not
    /// key-addressed (a namespace's keys are spread over every node by
    /// HRW), so unlike `get`/`set`/`delete` this alone never sees a `W`;
    /// fanning the call out to every node and aggregating the result is
    /// [`crate::client::NanocachedClient`]'s job, not this connection's.
    pub(crate) async fn clear(&self, namespace: &[u8]) -> Result<()> {
        match self.request(|tag| encode_clear(namespace, tag)).await? {
            ResponseKind::Cleared => Ok(()),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Drops every namespace on this one node, the default one included
    /// (issue #106's `F`). See `clear`'s doc comment for the
    /// not-key-addressed / fan-out note, which applies here too.
    pub(crate) async fn clear_all(&self) -> Result<()> {
        match self.request(encode_clear_all).await? {
            ResponseKind::Cleared => Ok(()),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Sends `i` (issue #129) to this one node only, and returns the new
    /// counter value plus its remaining TTL (if any) — `None` on a
    /// key-missing `N`, or `Err(Error::NotNumeric)` (propagated by `?` off
    /// `request`) on a not-numeric/overflow `T`. `namespace` empty means
    /// the default namespace, exactly like `get`/`set`/`delete` — but
    /// unlike those, `i` has no legacy uppercase form; even the default
    /// namespace goes out namespaced (see `encode_incr`).
    ///
    /// Deliberately dumb: this alone never fans anything out to replicas.
    /// Cluster replication of the result — forwarding the literal new
    /// value to the remaining owners as an ordinary `set`, never replaying
    /// `i` on them — is [`crate::client::NanocachedClient`]'s job (see its
    /// `incr_in`), precisely because replaying the increment on a replica
    /// could let it drift from the primary (a dropped earlier replica
    /// write, an independent eviction) instead of staying byte-identical.
    pub(crate) async fn incr(
        &self,
        namespace: &[u8],
        key: &[u8],
        delta: i64,
    ) -> Result<Option<(i64, Option<u64>)>> {
        match self
            .request(|tag| encode_incr(namespace, key, delta, tag))
            .await?
        {
            ResponseKind::Incr(value, ttl_seconds) => Ok(Some((value, ttl_seconds))),
            ResponseKind::NotFound => Ok(None),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Sends `k` (compare-and-set, issue #141) to this one node only, and
    /// returns whether `condition` held: `true` (wire `S`, the same
    /// acknowledgement a plain `set` gives) means `value` is now stored;
    /// `false` (wire `N`, the same "nothing to act on" status a miss
    /// already uses) means the condition didn't hold and nothing changed
    /// — a mismatch is a plain boolean outcome here, never an error.
    /// `namespace` empty means the default namespace, exactly like
    /// `incr` — `k` has no legacy uppercase form either.
    ///
    /// Deliberately dumb, exactly like `incr`: never fans anything out to
    /// replicas on its own. Cluster replication of a success — forwarding
    /// the literal new value to the remaining owners as an ordinary
    /// `set`, never replaying `k` on them — is
    /// [`crate::client::NanocachedClient`]'s job (see its `cas_set_once`),
    /// for exactly the reason `incr`'s replication is: a replica
    /// evaluating `condition` against its own possibly-different copy
    /// could reach a different outcome than the primary just did.
    pub(crate) async fn cas_set(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
        condition: CasCondition,
        ttl_seconds: u64,
    ) -> Result<bool> {
        match self
            .request(|tag| encode_cas_set(namespace, key, value, condition, ttl_seconds, tag))
            .await?
        {
            ResponseKind::Stored => Ok(true),
            ResponseKind::NotFound => Ok(false),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Sends `x` (compare-and-set, issue #141) to this one node only —
    /// the two-argument `remove(key, old)`. `condition` here is always a
    /// digest: an absent- or present-only conditioned delete is already
    /// the plain, unconditional `delete`. See `cas_set`'s doc comment for
    /// the boolean-not-error mismatch convention (wire `D`/`N`, same as a
    /// plain `delete`'s own hit/miss) and the "replicate the result,
    /// never the op" replication rule, both of which apply here
    /// identically — a success replicates as a plain `delete`.
    pub(crate) async fn cas_delete(
        &self,
        namespace: &[u8],
        key: &[u8],
        digest: [u8; 16],
    ) -> Result<bool> {
        match self
            .request(|tag| encode_cas_delete(namespace, key, digest, tag))
            .await?
        {
            ResponseKind::Deleted => Ok(true),
            ResponseKind::NotFound => Ok(false),
            other => Err(self.mismatch(&other)),
        }
    }

    /// Sends `m` (issue #151, docs/protocol.html#multi) — one round trip
    /// for every key in `keys`. `entries[i]` answers `keys[i]`, in
    /// request order. A reply whose roster length doesn't match
    /// `keys.len()` is treated as a desynced connection, same stance as
    /// [`Self::mismatch`] — a malformed reply can't be trusted
    /// key-for-key.
    pub(crate) async fn multi_get(
        &self,
        namespace: &[u8],
        keys: &[Vec<u8>],
    ) -> Result<Vec<MultiEntry>> {
        match self
            .request(|tag| encode_multi_get(namespace, keys, tag))
            .await?
        {
            ResponseKind::Multi(entries) if entries.len() == keys.len() => Ok(entries),
            ResponseKind::Multi(entries) => {
                self.close();
                Err(Error::ConnectionLost(format!(
                    "nanocached: multi-get response roster length {} does not match request key count {} (connection desynced)",
                    entries.len(),
                    keys.len()
                )))
            }
            other => Err(self.mismatch(&other)),
        }
    }

    /// Sends `o` (issue #151) — stores every key/value pair in one round
    /// trip, one shared `ttl_seconds` (0 means no expiry) for the whole
    /// batch rather than per key. `entries[i]` answers
    /// `keys[i]`/`values[i]`, in request order; see [`Self::multi_get`]
    /// for the same "only a desynced roster is an error" stance.
    /// Generic over the key/value byte container (issue #233) — see
    /// [`crate::client::NanocachedClient::multi_set_chunked`]'s own doc
    /// comment for why.
    pub(crate) async fn multi_set<B: AsRef<[u8]>>(
        &self,
        namespace: &[u8],
        keys: &[B],
        values: &[B],
        ttl_seconds: u64,
    ) -> Result<Vec<MultiEntry>> {
        match self
            .request(|tag| encode_multi_set(namespace, keys, values, ttl_seconds, tag))
            .await?
        {
            ResponseKind::Multi(entries) if entries.len() == keys.len() => Ok(entries),
            ResponseKind::Multi(entries) => {
                self.close();
                Err(Error::ConnectionLost(format!(
                    "nanocached: multi-set response roster length {} does not match request key count {} (connection desynced)",
                    entries.len(),
                    keys.len()
                )))
            }
            other => Err(self.mismatch(&other)),
        }
    }

    /// Wraps `request_uncapped` in `REQUEST_TIMEOUT_MS`: if the whole
    /// round trip hasn't completed by then, the server is presumed dead
    /// (a half-open server that accepts but never answers looks
    /// identical to one that's still slow) and this connection is
    /// poisoned so the abandoned request's queue slot — otherwise stuck
    /// forever with no receiver ever coming back for it, since nothing
    /// will ever answer — gets cleared and the socket released, instead
    /// of merely leaving it for a read that will never arrive to
    /// eventually skip over.
    async fn request<F>(&self, build: F) -> Result<ResponseKind>
    where
        F: Fn(Option<u32>) -> Vec<u8>,
    {
        let timeout = self.request_timeout;
        match tokio::time::timeout(timeout, self.request_uncapped(build)).await {
            Ok(result) => result,
            Err(_) => {
                self.close();
                Err(Error::ConnectionLost(format!(
                    "nanocached: request timed out after {timeout:?} waiting for a response"
                )))
            }
        }
    }

    /// Retryable-error status `R` (issue #125): up to 2 retries (3
    /// attempts total) of the same request on this same connection, no
    /// redial, no teardown — see the module-level `R` notes on
    /// `read_one_response`/`read_loop`. Slept before the first and
    /// second retry respectively; `RETRY_DELAYS_MS.len()` is the number
    /// of retries, one less than the attempt count.
    const RETRY_DELAYS_MS: [u64; 2] = [50, 100];

    /// Runs `build` against this connection, transparently retrying up to
    /// [`Self::RETRY_DELAYS_MS`]'s length more times (issue #125) whenever
    /// the server answers `R` — this request specifically failed
    /// transiently (e.g. a proxy's upstream node was briefly unreachable)
    /// and the connection itself is fine, so unlike a `ConnectionLost`
    /// this never redials: the exact same connection just tries again,
    /// after the matching delay. Every `R` received — including the
    /// last, exhausting one — counts in `transient_retries`. If every
    /// attempt answers `R`, this gives up (without closing anything) and
    /// returns [`Error::Retryable`] instead.
    ///
    /// `build` must be callable more than once (`Fn`, not `FnOnce`) since
    /// a retry claims a fresh tag and rebuilds the frame from scratch —
    /// every call site here only ever closes over `Copy` data (references,
    /// a `u64` TTL), so this is free.
    async fn request_uncapped<F>(&self, build: F) -> Result<ResponseKind>
    where
        F: Fn(Option<u32>) -> Vec<u8>,
    {
        const ATTEMPTS: usize = Connection::RETRY_DELAYS_MS.len() + 1;
        for attempt in 0..ATTEMPTS {
            let raw = self.single_attempt(&build).await?;
            if raw.0 != b'R' {
                return ResponseKind::try_from(raw);
            }
            self.transient_retries.fetch_add(1, Ordering::Relaxed);
            if let Some(&delay_ms) = Self::RETRY_DELAYS_MS.get(attempt) {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
        Err(Error::Retryable(format!(
            "nanocached: request failed transiently {ATTEMPTS} times in a row"
        )))
    }

    /// One request/response round trip: enqueues a pending slot and
    /// writes `frame` under one lock (see the module doc comment), then
    /// waits on its own oneshot receiver — not the socket. If this future
    /// is dropped while awaiting that receiver (the write already
    /// completed), the slot is simply left in the queue: the read task
    /// will eventually find no receiver listening and move on, exactly
    /// like the TypeScript SDK's Connection, whose plain Promises can't
    /// be cancelled out from under `pending` at all — every request
    /// behind it in the queue is unaffected. (`request`'s timeout wrapper
    /// additionally poisons the connection outright when it fires, since
    /// in that case nothing is ever going to answer.)
    ///
    /// Returns the raw `(marker, value)` rather than a [`ResponseKind`] —
    /// `request_uncapped` inspects the marker itself first (`R`, issue
    /// #125, never becomes a `ResponseKind` at all) before converting.
    async fn single_attempt<F>(&self, build: &F) -> Result<RawResponse>
    where
        F: Fn(Option<u32>) -> Vec<u8>,
    {
        if self.is_closed() {
            return Err(Error::ConnectionLost(
                "nanocached: connection is closed".to_string(),
            ));
        }

        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.shared.write_state.lock().await;
            if state.write_half.is_none() {
                return Err(Error::ConnectionLost(
                    "nanocached: connection is closed".to_string(),
                ));
            }
            self.shared.last_used_ms.store(
                self.shared.epoch.elapsed().as_millis() as u64,
                Ordering::SeqCst,
            );

            // Echoed response tags: claim this connection's next tag (if tagged) and
            // build the frame in the same critical section that enqueues
            // the pending slot and writes it, so tag order can never skew
            // from queue/wire order (request pipelining's invariant). `None` on an
            // untagged connection produces exactly the pre-0019 frame.
            let tag = if self.shared.tagged {
                let tag = state.next_tag;
                state.next_tag = state.next_tag.wrapping_add(1);
                Some(tag)
            } else {
                None
            };
            let frame = build(tag);

            state.pending.push_back(PendingSlot { tag, tx });
            let write_half = state.write_half.as_mut().expect("checked above");

            let mut guard = WriteGuard {
                shared: &self.shared,
                shutdown: &self.shutdown,
                completed: false,
            };
            let write_result = write_half.write_all(&frame).await;
            guard.completed = true;

            if let Err(error) = write_result {
                drop(state);
                self.close();
                return Err(Error::ConnectionLost(format!(
                    "nanocached: connection failed: {error}"
                )));
            }
        }

        // Everything past this point runs only once `write_all` above has
        // returned `Ok` — the frame is fully on the wire, so any failure
        // from here on can no longer be reported as "never sent" (issue
        // #225): `apply_reconnecting_no_replay`'s non-idempotent callers
        // (incr/decr, CAS, delete_if_matches) key off exactly this
        // distinction to decide whether redialing and replaying `op` could
        // double-apply an effect the server already committed.
        match rx.await {
            Ok(Ok(raw)) => Ok(raw),
            Ok(Err(error)) => Err(Self::mark_sent(error)),
            Err(_) => Err(Error::ConnectionLostAfterSend(
                "nanocached: connection is closed".to_string(),
            )),
        }
    }

    /// Reclassifies a plain [`Error::ConnectionLost`] as
    /// [`Error::ConnectionLostAfterSend`] — see that variant's doc comment.
    /// Any other error (a tag-mismatch/desync `Protocol`, say) is already
    /// never replayed by `apply_reconnecting`/`apply_reconnecting_no_replay`
    /// regardless of this distinction, so it passes through unchanged.
    fn mark_sent(error: Error) -> Error {
        match error {
            Error::ConnectionLost(message) => Error::ConnectionLostAfterSend(message),
            other => other,
        }
    }

    /// A well-formed response of the wrong kind (a `S` answering a G)
    /// means the request/response streams are misaligned — every later
    /// response would answer the wrong request, silently returning other
    /// keys' data. Poison the connection, and classify as connection-lost
    /// so the client's retry layer redials and retries once. Requests
    /// still pending behind this one may already have been resolved with
    /// misaligned data by the time this runs — an inherent limitation of
    /// matching-by-order pipelining shared with the TypeScript SDK's
    /// Connection (request pipelining), not something this SDK introduces.
    fn mismatch(&self, kind: &ResponseKind) -> Error {
        let name = match kind {
            ResponseKind::Value(_) => "value",
            ResponseKind::NotFound => "not-found",
            ResponseKind::Stored => "stored",
            ResponseKind::Deleted => "deleted",
            ResponseKind::Cleared => "cleared",
            ResponseKind::Incr(_, _) => "incr",
            ResponseKind::Multi(_) => "multi",
        };
        self.close();
        Error::ConnectionLost(format!(
            "nanocached: response \"{name}\" does not match the request (connection desynced)"
        ))
    }
}

/// Builds a `G`/`g` frame (Namespaces, issue #105): the default (empty)
/// namespace always produces the legacy `G <key-len>[ <tag>]\n<key>`
/// bytes untouched — the SDK rule that keeps an unchanged client talking
/// to a pre-namespace server working — and only a non-empty namespace
/// switches to `g <ns-len> <key-len>[ <tag>]\n<namespace><key>`. The
/// namespace is never interpreted (no delimiter, no escaping): it is
/// simply sliced by its declared length like every other body field, so
/// it may contain any bytes.
fn encode_get(namespace: &[u8], key: &[u8], tag: Option<u32>) -> Vec<u8> {
    let mut frame = if namespace.is_empty() {
        match tag {
            Some(tag) => format!("G {} {tag}\n", key.len()),
            None => format!("G {}\n", key.len()),
        }
    } else {
        match tag {
            Some(tag) => format!("g {} {} {tag}\n", namespace.len(), key.len()),
            None => format!("g {} {}\n", namespace.len(), key.len()),
        }
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Builds an `S`/`s` frame — see `encode_get` for the legacy/namespaced
/// split. The tag, when present, is always the header's last field,
/// after the TTL when there is one, on both the uppercase and lowercase
/// forms alike.
fn encode_set(
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
    ttl_seconds: u64,
    tag: Option<u32>,
) -> Vec<u8> {
    let header = if namespace.is_empty() {
        match (ttl_seconds, tag) {
            (0, None) => format!("S {} {}\n", key.len(), value.len()),
            (0, Some(tag)) => format!("S {} {} {tag}\n", key.len(), value.len()),
            (ttl, None) => format!("S {} {} {ttl}\n", key.len(), value.len()),
            (ttl, Some(tag)) => format!("S {} {} {ttl} {tag}\n", key.len(), value.len()),
        }
    } else {
        match (ttl_seconds, tag) {
            (0, None) => format!("s {} {} {}\n", namespace.len(), key.len(), value.len()),
            (0, Some(tag)) => {
                format!(
                    "s {} {} {} {tag}\n",
                    namespace.len(),
                    key.len(),
                    value.len()
                )
            }
            (ttl, None) => {
                format!(
                    "s {} {} {} {ttl}\n",
                    namespace.len(),
                    key.len(),
                    value.len()
                )
            }
            (ttl, Some(tag)) => format!(
                "s {} {} {} {ttl} {tag}\n",
                namespace.len(),
                key.len(),
                value.len()
            ),
        }
    };
    let mut frame = header.into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.extend_from_slice(value);
    frame
}

/// Builds a `D`/`d` frame — see `encode_get` for the legacy/namespaced
/// split.
fn encode_delete(namespace: &[u8], key: &[u8], tag: Option<u32>) -> Vec<u8> {
    let mut frame = if namespace.is_empty() {
        match tag {
            Some(tag) => format!("D {} {tag}\n", key.len()),
            None => format!("D {}\n", key.len()),
        }
    } else {
        match tag {
            Some(tag) => format!("d {} {} {tag}\n", namespace.len(), key.len()),
            None => format!("d {} {}\n", namespace.len(), key.len()),
        }
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Builds a `c` frame (issue #106): `c <namespace-length>[ <tag>]\n<namespace>`.
/// Unlike `encode_get`/`encode_set`/`encode_delete`, there is no legacy
/// uppercase form to preserve here — `clear` is new as of #106, so even
/// the default (empty) namespace goes out as `c 0[ <tag>]\n` rather than
/// switching frames.
fn encode_clear(namespace: &[u8], tag: Option<u32>) -> Vec<u8> {
    let mut frame = match tag {
        Some(tag) => format!("c {} {tag}\n", namespace.len()),
        None => format!("c {}\n", namespace.len()),
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame
}

/// Builds an `F` frame (issue #106): `F[ <tag>]\n` — no body at all, since
/// flushing every namespace names nothing.
fn encode_clear_all(tag: Option<u32>) -> Vec<u8> {
    match tag {
        Some(tag) => format!("F {tag}\n"),
        None => "F\n".to_string(),
    }
    .into_bytes()
}

/// Builds an `i` frame (issue #129): `i <namespace-length> <key-length>
/// <delta>[ <tag>]\n<namespace><key>`. Unlike `encode_get`/`encode_set`/
/// `encode_delete`, there is no legacy uppercase form to preserve — `incr`
/// is new as of #129, so even the default (empty) namespace goes out
/// namespaced rather than switching command letters (mirrors
/// `encode_clear`'s own no-legacy-form rule, issue #106). `delta` is
/// always emitted in its canonical signed-decimal form — `i64::to_string()`
/// (which `{delta}` uses via `Display`) already produces exactly that: an
/// optional leading `-`, no leading zeros, no `+`.
fn encode_incr(namespace: &[u8], key: &[u8], delta: i64, tag: Option<u32>) -> Vec<u8> {
    let mut frame = match tag {
        Some(tag) => format!("i {} {} {delta} {tag}\n", namespace.len(), key.len()),
        None => format!("i {} {} {delta}\n", namespace.len(), key.len()),
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Builds a `k` frame (compare-and-set, issue #141): `k <namespace-length>
/// <key-length> <value-length> <cond> [<ttl-seconds>] [<tag>]\n<namespace><key><value>`
/// — always namespaced, no legacy uppercase form, exactly like
/// `encode_incr`. `<cond>` is a mandatory bare token ahead of `encode_set`'s
/// own optional trailing `[ttl] [tag]` pair, which this reuses verbatim
/// (same tagged-mode-aware trailing-field-count idiom).
fn encode_cas_set(
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
    condition: CasCondition,
    ttl_seconds: u64,
    tag: Option<u32>,
) -> Vec<u8> {
    let cond = cas_condition_token(condition);
    let mut frame = match (ttl_seconds, tag) {
        (0, None) => format!(
            "k {} {} {} {cond}\n",
            namespace.len(),
            key.len(),
            value.len()
        ),
        (0, Some(tag)) => format!(
            "k {} {} {} {cond} {tag}\n",
            namespace.len(),
            key.len(),
            value.len()
        ),
        (ttl, None) => format!(
            "k {} {} {} {cond} {ttl}\n",
            namespace.len(),
            key.len(),
            value.len()
        ),
        (ttl, Some(tag)) => format!(
            "k {} {} {} {cond} {ttl} {tag}\n",
            namespace.len(),
            key.len(),
            value.len()
        ),
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.extend_from_slice(value);
    frame
}

/// Builds an `x` frame (compare-and-set, issue #141): `x <namespace-length>
/// <key-length> <cond> [<tag>]\n<namespace><key>` — `<cond>` is always a
/// digest here (never `A`/`P`, which are already the plain, unconditional
/// `D`/`d` — see `Connection::cas_delete`'s doc comment).
fn encode_cas_delete(namespace: &[u8], key: &[u8], digest: [u8; 16], tag: Option<u32>) -> Vec<u8> {
    let cond = cas_condition_token(CasCondition::Digest(digest));
    let mut frame = match tag {
        Some(tag) => format!("x {} {} {cond} {tag}\n", namespace.len(), key.len()),
        None => format!("x {} {} {cond}\n", namespace.len(), key.len()),
    }
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Builds an `m` frame (issue #151, batched get/set,
/// docs/protocol.html#multi): `m <namespace-length> <n> <key-length-1>
/// ... <key-length-n>[ <tag>]\n<namespace><key-1>...<key-n>` — always
/// namespaced, no legacy uppercase form, exactly like `encode_incr`/
/// `encode_cas_set`.
fn encode_multi_get(namespace: &[u8], keys: &[Vec<u8>], tag: Option<u32>) -> Vec<u8> {
    let mut header = format!("m {} {}", namespace.len(), keys.len());
    // Issue #233: `write!` straight into `header` instead of a per-key
    // `format!` + `push_str` (an extra `String` allocated and thrown away
    // per key).
    for key in keys {
        let _ = write!(header, " {}", key.len());
    }
    if let Some(tag) = tag {
        let _ = write!(header, " {tag}");
    }
    header.push('\n');
    let mut frame = header.into_bytes();
    frame.extend_from_slice(namespace);
    for key in keys {
        frame.extend_from_slice(key);
    }
    frame
}

/// Builds an `o` frame (issue #151): `o <namespace-length> <n>
/// <key-length-1> <value-length-1> ... <key-length-n> <value-length-n>
/// [<ttl-seconds>][ <tag>]\n<namespace><key-1><value-1>...<key-n><value-n>`.
/// The optional TTL sits ahead of the tag, same convention `encode_set`'s
/// own `[ttl] [tag]` uses.
fn encode_multi_set<B: AsRef<[u8]>>(
    namespace: &[u8],
    keys: &[B],
    values: &[B],
    ttl_seconds: u64,
    tag: Option<u32>,
) -> Vec<u8> {
    let mut header = format!("o {} {}", namespace.len(), keys.len());
    // Issue #233: `write!` straight into `header` instead of a per-key
    // `format!` + `push_str`.
    for (key, value) in keys.iter().zip(values) {
        let _ = write!(header, " {} {}", key.as_ref().len(), value.as_ref().len());
    }
    if ttl_seconds != 0 {
        let _ = write!(header, " {ttl_seconds}");
    }
    if let Some(tag) = tag {
        let _ = write!(header, " {tag}");
    }
    header.push('\n');
    let mut frame = header.into_bytes();
    frame.extend_from_slice(namespace);
    for (key, value) in keys.iter().zip(values) {
        frame.extend_from_slice(key.as_ref());
        frame.extend_from_slice(value.as_ref());
    }
    frame
}

/// This connection's only reader, for its whole lifetime — nothing else
/// may read from `read_half`. Consumes responses off the wire and
/// dispatches each to the oldest pending request (FIFO —
/// Request pipelining), until told to stop (poisoned by any of the
/// triggers in `Shared::mark_closed`) or a read itself fails.
async fn read_loop(
    mut read_half: BufReader<ReadHalf<Stream>>,
    shared: Arc<Shared>,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let response = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                drain_pending(&shared, None).await;
                return;
            }
            result = read_one_response(
                &mut read_half,
                shared.tagged,
                // Production reads the process-wide default here; the bound
                // is a parameter only so tests can pass an explicit value
                // (see read_one_response).
                MAX_MULTI_GET_RESPONSE_BYTES.load(Ordering::SeqCst),
            ) => result,
        };

        let (marker, value, ttl_seconds, tag, entries) = match response {
            Ok(response) => response,
            Err(error) => {
                // error belongs to whichever request has been waiting
                // longest — the read loop only ever reads one response
                // at a time, in order, so a failure here is always about
                // the oldest pending request specifically, not the
                // connection in general (see drain_pending).
                shared.mark_closed(&shutdown_tx);
                drain_pending(&shared, Some(error)).await;
                return;
            }
        };

        let (was_empty, slot) = {
            let mut state = shared.write_state.lock().await;
            let was_empty = state.pending.is_empty();
            let slot = if was_empty {
                None
            } else {
                state.pending.pop_front()
            };
            (was_empty, slot)
        };

        // An unsolicited "busy" response means the server hit its
        // connection limit right after accept and is about to close the
        // connection; it isn't an answer to anything we sent (mirrors
        // the TypeScript SDK's Connection.onData).
        if marker == b'B' && was_empty {
            shared.mark_closed(&shutdown_tx);
            drain_pending(&shared, None).await;
            return;
        }
        let Some(slot) = slot else {
            // Unsolicited and not the known busy case — desync.
            shared.mark_closed(&shutdown_tx);
            drain_pending(&shared, None).await;
            return;
        };

        // Echoed response tags: on a tagged connection, verify the echoed tag against
        // the request this response is about to answer — *before* it can
        // reach any caller. A mismatch means the streams are misaligned;
        // unlike the caller-side kind check (`mismatch()`), catching it
        // here stops the misdelivery instead of merely noticing it later.
        if shared.tagged && tag != slot.tag {
            let message = format!(
                "nanocached: response tag {tag:?} does not answer request tag {:?} (connection desynced)",
                slot.tag
            );
            shared.mark_closed(&shutdown_tx);
            // The popped slot is no longer in `pending`, so drain_pending
            // won't reach it — reject it here. Every request still queued
            // behind it never got any response at all, but is just as
            // misaligned, so it gets the same "desynced" error rather
            // than the generic "connection closed" drain_pending would
            // otherwise give it.
            let _ = slot.tx.send(Err(Error::ConnectionLost(message.clone())));
            drain_pending(&shared, Some(Error::ConnectionLost(message))).await;
            return;
        }

        // A marker that answers no request kind at all (the only one a
        // server ever emits is `B`, and that only before identify) means
        // the streams are misaligned just as surely as a wrong tag: the
        // popped slot wasn't answered, and nothing queued behind it is
        // lined up with what comes next. Handing it to the caller as a
        // plain `Protocol` error would leave the connection open and
        // permanently off by one — poison it here instead, the same way
        // as a tag mismatch (mirrors Go's `default: mismatch`).
        if !matches!(
            marker,
            b'V' | b'N' | b'S' | b'D' | b'W' | b'C' | b'R' | b'I' | b'T' | b'M' | b'O'
        ) {
            let message = format!(
                "nanocached: unexpected response from server: {} (connection desynced)",
                marker as char
            );
            shared.mark_closed(&shutdown_tx);
            let _ = slot.tx.send(Err(Error::Protocol(message.clone())));
            drain_pending(&shared, Some(Error::ConnectionLost(message))).await;
            return;
        }

        // An Err here just means the caller abandoned this request after
        // its write completed (see `Connection::request`) — nothing to
        // do, the queue position was already correctly consumed above.
        let _ = slot.tx.send(Ok((marker, value, ttl_seconds, entries)));
    }
}

/// Rejects every request still queued, and drops the write half so the
/// socket actually closes now rather than waiting for the last
/// `Connection` clone to drop. When `first_error` is given — the specific
/// error that actually triggered the read loop's exit (a malformed
/// frame, a tag mismatch, a read failure) — every still-queued request
/// gets a *clone* of that same error, matching the Go/TypeScript/Python
/// SDKs: none of them have any better answer for "why did my request
/// never get a response" than the one error that's actually known to be
/// true, so broadcasting a generic "connection closed" to everyone but
/// the oldest request only threw that information away for no reason
/// (`Error`'s `Clone` derive exists precisely to make this cheap — see
/// its doc comment in error.rs). When `first_error` is `None` — the
/// read loop was told to shut down (`close()`, or another poison trigger
/// with no parseable cause of its own) rather than having failed a read
/// itself — there is no specific cause to attribute, so every request
/// still falls back to the generic "connection closed".
async fn drain_pending(shared: &Shared, first_error: Option<Error>) {
    let mut state = shared.write_state.lock().await;
    state.write_half = None;
    let pending = state.pending.drain(..);
    match first_error {
        Some(error) => {
            for slot in pending {
                let _ = slot.tx.send(Err(error.clone()));
            }
        }
        None => {
            for slot in pending {
                let _ = slot.tx.send(Err(Error::ConnectionLost(
                    "nanocached: connection closed".to_string(),
                )));
            }
        }
    }
}

/// True for a non-empty ASCII-digits-only field (`^[0-9]+$`) — the wire
/// grammar every plain integer field this SDK reads off the wire (a
/// length, a count, a ttl, a tag, a port, ...) must match (issue #462).
/// Rust's `FromStr` impls for the unsigned/signed integer types already
/// reject `_`, internal whitespace, and exponents on their own, but they
/// accept a leading `+` that this wire grammar does not — so every such
/// field is checked against this predicate (via [`parse_strict`]) before
/// ever reaching `.parse()`, rather than relying on that stdlib leniency
/// to happen to line up with the grammar. Leading zeros (`"007"`) ARE
/// allowed, matching the server's own `parse_length` grammar
/// (`src/command.rs`, repo root), which loops byte-by-byte over ASCII
/// digits with no such restriction.
fn is_strict_digits(field: &str) -> bool {
    !field.is_empty() && field.bytes().all(|byte| byte.is_ascii_digit())
}

/// [`is_strict_digits`]'s one exception: the INCR/DECR counter body (the
/// decimal value the `I` response echoes back) additionally allows
/// exactly one leading `-` (never `+`), mirroring the request's own
/// `delta` field grammar. Reference precedent: Python's
/// `_INCR_VALUE_RE = re.compile(rb"-?[0-9]{1,19}")`
/// (`sdk/python/src/nanocached/_connection.py`) and .NET's
/// `TryParseWireCounter`, which explicitly rejects a leading `+` before
/// parsing (`sdk/dotnet/src/Nanocached/Connection.cs`).
fn is_strict_counter(field: &str) -> bool {
    is_strict_digits(field.strip_prefix('-').unwrap_or(field))
}

/// Parses `field` as `T`, first rejecting anything that doesn't match
/// [`is_strict_digits`] — the single point of truth every non-counter
/// integer field on the wire is routed through, so `.parse()`'s own
/// leniency (a leading `+`) never becomes this SDK's leniency.
pub(crate) fn parse_strict<T: std::str::FromStr>(field: &str) -> Option<T> {
    if is_strict_digits(field) {
        field.parse().ok()
    } else {
        None
    }
}

/// A marker byte, its value bytes (`V`/`I` only), its TTL in seconds
/// (`I` only, issue #129, when the entry has one), and — on a tagged
/// connection (echoed response tags) — the tag it echoed, straight off the wire
/// before the read loop has verified that tag against anything. Only
/// `read_one_response`/`read_loop` ever see this fourth field; once
/// verified it's stripped down to a plain [`RawResponse`] before being
/// handed to a waiting caller.
type WireResponse = (
    u8,
    Option<Vec<u8>>,
    Option<u64>,
    Option<u32>,
    Option<Vec<MultiEntry>>,
);

async fn read_one_response(
    read_half: &mut BufReader<ReadHalf<Stream>>,
    tagged: bool,
    // The multi-get cumulative-reply cap, passed in rather than read from
    // the `MAX_MULTI_GET_RESPONSE_BYTES` static here, so a unit test can
    // exercise the bound with an explicit value instead of mutating the
    // process-wide static — which, read on each connection's background
    // read task while `cargo test` runs the suite concurrently, would let
    // one test's lowered bound reject another's ordinary reply (a rare
    // flake, reproduced in CI on 2026-09-01).
    max_multi_get_response_bytes: usize,
) -> Result<WireResponse> {
    let marker = read_half.read_u8().await?;
    match marker {
        b'V' => {
            let header = read_line(read_half).await?;
            let header = header.trim();
            // Untagged: `V <len>`. Tagged: `V <len> <tag>` (echoed response tags) —
            // the tag rides as a second field on the same header line.
            let (length_field, tag) = if tagged {
                let mut fields = header.splitn(2, ' ');
                match (fields.next(), fields.next()) {
                    (Some(length), Some(tag)) => (length, Some(parse_tag(tag)?)),
                    _ => {
                        return Err(Error::Protocol(
                            "nanocached: invalid value header in response".to_string(),
                        ));
                    }
                }
            } else {
                (header, None)
            };
            // The server never stores values above its 1 MiB request
            // limit, so a claimed length beyond MAX_VALUE_LENGTH is a
            // corrupt or malicious frame (issue #12); reject before
            // allocating.
            let length: usize = parse_strict(length_field)
                .filter(|length| *length <= MAX_VALUE_LENGTH)
                .ok_or_else(|| {
                    Error::Protocol("nanocached: invalid value length in response".to_string())
                })?;
            let mut value = vec![0u8; length];
            read_half.read_exact(&mut value).await?;
            Ok((marker, Some(value), None, tag, None))
        }
        b'I' => {
            // INCR success (issue #129): `<value-length> [<ttl-seconds>]
            // [<tag>]`. Disambiguating whether a trailing field is the ttl
            // or the tag is the *decoding* side of the exact same
            // "count trailing fields, tagged-mode-aware" idiom
            // `encode_set` already uses to *build* `S`'s own optional
            // `[ttl] [tag]` header fields: untagged, 0 trailing fields
            // after the length means no ttl, 1 means ttl; tagged, 1
            // trailing field means "just the tag", 2 means "ttl then tag"
            // — decided purely by whether this connection is tagged, never
            // guessed frame by frame.
            let header = read_line(read_half).await?;
            let header = header.trim();
            let mut fields = header.split(' ').filter(|field| !field.is_empty());
            let length_field = fields.next().ok_or_else(|| {
                Error::Protocol("nanocached: invalid incr response header".to_string())
            })?;
            let rest: Vec<&str> = fields.collect();
            let (ttl_field, tag) = if tagged {
                match rest.as_slice() {
                    [tag] => (None, Some(parse_tag(tag)?)),
                    [ttl, tag] => (Some(*ttl), Some(parse_tag(tag)?)),
                    _ => {
                        return Err(Error::Protocol(
                            "nanocached: invalid incr response header".to_string(),
                        ));
                    }
                }
            } else {
                match rest.as_slice() {
                    [] => (None, None),
                    [ttl] => (Some(*ttl), None),
                    _ => {
                        return Err(Error::Protocol(
                            "nanocached: invalid incr response header".to_string(),
                        ));
                    }
                }
            };
            let length: usize = parse_strict(length_field)
                .filter(|length| *length <= MAX_VALUE_LENGTH)
                .ok_or_else(|| {
                    Error::Protocol("nanocached: invalid value length in response".to_string())
                })?;
            let ttl_seconds = match ttl_field {
                Some(field) => Some(parse_strict::<u64>(field).ok_or_else(|| {
                    Error::Protocol("nanocached: invalid ttl in incr response".to_string())
                })?),
                None => None,
            };
            let mut value = vec![0u8; length];
            read_half.read_exact(&mut value).await?;
            Ok((marker, Some(value), ttl_seconds, tag, None))
        }
        b'B' => {
            // `B` (busy) is always untagged — unsolicited, sent before
            // auth (and so before tagging) even completes (echoed response tags).
            read_half.read_u8().await?; // the trailing '\n'
            Ok((marker, None, None, None, None))
        }
        b'S' | b'D' | b'N' | b'W' | b'C' | b'R' | b'T' => {
            if tagged {
                // `S <tag>\n` / `D <tag>\n` / `N <tag>\n` / `W <tag>\n` /
                // `C <tag>\n` (issue #106) / `R <tag>\n` (retryable-error
                // status, issue #125) / `T <tag>\n` (not-numeric, issue
                // #129) — every no-value marker's tag rides as its header
                // line's sole field.
                let line = read_line(read_half).await?;
                Ok((marker, None, None, Some(parse_tag(line.trim())?), None))
            } else {
                read_half.read_u8().await?; // the trailing '\n'
                Ok((marker, None, None, None, None))
            }
        }
        // issue #151 — batched get/set (docs/protocol.html#multi): `M`
        // answers `m` (multi-get), `O` answers `o` (multi-set). `M <n>
        // <result-1> ... <result-n>[ <tag>]\n<hit values, concatenated in
        // request order>` — each result token is "-" (miss), "W" (wrong
        // node), or a decimal byte length (a hit — that many trailing
        // body bytes belong to this key, read here, inline, in token
        // order, since only hit tokens consume body bytes). `O`'s
        // reply has the same header shape but no body (a set has
        // nothing to echo back) and only "S"/"W" tokens.
        b'M' | b'O' => {
            let header = read_line(read_half).await?;
            let header = header.trim();
            let mut fields = header.split(' ').filter(|field| !field.is_empty());
            let count: usize = fields.next().and_then(parse_strict).ok_or_else(|| {
                Error::Protocol(format!(
                    "nanocached: invalid multi-{} header in response",
                    if marker == b'M' { "get" } else { "set" }
                ))
            })?;
            let rest: Vec<&str> = fields.collect();
            // `count` is untrusted (a buggy or hostile node picks it): every
            // other length field in this crate is bounded, so bound this one
            // too. Compare against `rest.len()` rather than computing
            // `count + 1`, which overflows to 0 for `count == usize::MAX` and
            // in release builds would slip past the check straight into an
            // out-of-bounds `rest[..count]` panic. `checked_sub` on the
            // known-good `rest.len()` cannot overflow.
            let (result_tokens, tag): (&[&str], Option<u32>) = if tagged {
                if rest.len().checked_sub(1) != Some(count) {
                    return Err(Error::Protocol(format!(
                        "nanocached: invalid multi-{} header in response",
                        if marker == b'M' { "get" } else { "set" }
                    )));
                }
                (&rest[..count], Some(parse_tag(rest[count])?))
            } else {
                if rest.len() != count {
                    return Err(Error::Protocol(format!(
                        "nanocached: invalid multi-{} header in response",
                        if marker == b'M' { "get" } else { "set" }
                    )));
                }
                (&rest[..], None)
            };

            let mut entries = Vec::with_capacity(count);
            if marker == b'M' {
                // issue #207: each entry's own `length` is already capped
                // above by MAX_VALUE_LENGTH, but the reply as a whole
                // isn't — track the running total of every hit body this
                // reply has claimed so far, and reject before allocating
                // or reading a body that would push it past
                // MAX_MULTI_GET_RESPONSE_BYTES (see that static's own doc
                // comment).
                let mut total_bytes: usize = 0;
                let max_response_bytes = max_multi_get_response_bytes;
                for token in result_tokens {
                    match *token {
                        "-" => entries.push(MultiEntry::Miss),
                        "W" => entries.push(MultiEntry::WrongNode),
                        length_field => {
                            let length: usize = parse_strict(length_field)
                                .filter(|length| *length <= MAX_VALUE_LENGTH)
                                .ok_or_else(|| {
                                    Error::Protocol(
                                        "nanocached: invalid multi-get result length in response"
                                            .to_string(),
                                    )
                                })?;
                            total_bytes = total_bytes.saturating_add(length);
                            if total_bytes > max_response_bytes {
                                return Err(Error::Protocol(format!(
                                    "nanocached: multi-get response exceeds {max_response_bytes} bytes"
                                )));
                            }
                            let mut value = vec![0u8; length];
                            read_half.read_exact(&mut value).await?;
                            entries.push(MultiEntry::Hit(value));
                        }
                    }
                }
            } else {
                // `O` (multi-set) acks carry no bodies — just "S"/"W"
                // tokens on this one already-length-bounded header line
                // (MAX_HEADER_LINE_LENGTH), so `count` itself is already
                // bounded and there's nothing analogous to `M`'s
                // cumulative-bytes check to add here (issue #207).
                for token in result_tokens {
                    match *token {
                        "S" => entries.push(MultiEntry::Stored),
                        "W" => entries.push(MultiEntry::WrongNode),
                        _ => {
                            return Err(Error::Protocol(
                                "nanocached: invalid multi-set result token in response"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
            Ok((marker, None, None, tag, Some(entries)))
        }
        other => Err(Error::Protocol(format!(
            "nanocached: unexpected response from server: {}",
            other as char
        ))),
    }
}

/// Parses a response's echoed tag (echoed response tags): a `u32` in decimal,
/// matching the wire width the client itself claims tags from.
fn parse_tag(field: &str) -> Result<u32> {
    parse_strict(field)
        .ok_or_else(|| Error::Protocol("nanocached: invalid response tag".to_string()))
}

/// Converts `request`'s raw `(marker, value)` into the higher-level kind
/// `get`/`set`/`delete` switch on, or `Error::WrongNode` for `W`.
impl TryFrom<RawResponse> for ResponseKind {
    type Error = Error;

    fn try_from((marker, value, ttl_seconds, entries): RawResponse) -> Result<Self> {
        match marker {
            b'V' => Ok(ResponseKind::Value(value.unwrap_or_default())),
            b'N' => Ok(ResponseKind::NotFound),
            b'S' => Ok(ResponseKind::Stored),
            b'D' => Ok(ResponseKind::Deleted),
            b'C' => Ok(ResponseKind::Cleared),
            // issue #151: M/O always carry `entries` (see `RawResponse`'s
            // own doc comment) — `unwrap_or_default` only ever matters
            // for tests that construct a bare `RawResponse` by hand.
            b'M' | b'O' => Ok(ResponseKind::Multi(entries.unwrap_or_default())),
            b'I' => {
                let text = value.unwrap_or_default();
                // The counter body is the one field allowed a leading `-`
                // (issue #462, `is_strict_counter`'s own doc comment) — every
                // other integer field on the wire goes through
                // `parse_strict`/`is_strict_digits` instead.
                let counter = std::str::from_utf8(&text)
                    .ok()
                    .filter(|text| is_strict_counter(text))
                    .and_then(|text| text.parse::<i64>().ok())
                    .ok_or_else(|| {
                        Error::Protocol("nanocached: invalid incr value in response".to_string())
                    })?;
                Ok(ResponseKind::Incr(counter, ttl_seconds))
            }
            // Not-numeric (issue #129): the key exists but its stored
            // value isn't INCR's counter grammar, or the delta would
            // overflow `i64` — never a `ResponseKind`, just like `W`
            // is never one, since every caller must handle it as an
            // error rather than a value to switch on.
            b'T' => Err(Error::NotNumeric),
            b'W' => Err(Error::WrongNode),
            other => Err(Error::Protocol(format!(
                "nanocached: unexpected response from server: {}",
                other as char
            ))),
        }
    }
}

pub(crate) async fn read_line<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> Result<String> {
    let mut line = Vec::new();
    loop {
        let byte = stream.read_u8().await?;
        if byte == b'\n' {
            return String::from_utf8(line)
                .map_err(|_| Error::Protocol("nanocached: non-UTF-8 response header".to_string()));
        }
        line.push(byte);
        if line.len() > MAX_HEADER_LINE_LENGTH {
            return Err(Error::Protocol(format!(
                "nanocached: response header line exceeds {MAX_HEADER_LINE_LENGTH} bytes without a terminator"
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── strict wire-integer grammar (issue #462) ────────────────────

    #[test]
    fn is_strict_digits_accepts_only_ascii_digits_only_and_non_empty() {
        for (field, expected) in [
            ("5", true),
            ("0", true),
            ("007", true),
            ("+5", false),
            (" 5", false),
            ("5 ", false),
            ("1_000", false),
            ("1e2", false),
            ("-5", false),
            ("", false),
        ] {
            assert_eq!(
                is_strict_digits(field),
                expected,
                "is_strict_digits({field:?}) should be {expected}"
            );
        }
    }

    #[test]
    fn is_strict_counter_additionally_allows_exactly_one_leading_minus() {
        for (field, expected) in [
            ("5", true),
            ("0", true),
            ("007", true),
            ("-5", true),
            ("-007", true),
            ("+5", false),
            (" 5", false),
            ("5 ", false),
            ("1_000", false),
            ("1e2", false),
            ("--5", false),
            ("-", false),
            ("", false),
        ] {
            assert_eq!(
                is_strict_counter(field),
                expected,
                "is_strict_counter({field:?}) should be {expected}"
            );
        }
    }

    #[test]
    fn parse_strict_rejects_non_digit_grammar_and_accepts_leading_zeros() {
        assert_eq!(parse_strict::<u64>("5"), Some(5));
        assert_eq!(parse_strict::<u64>("007"), Some(7));
        assert_eq!(parse_strict::<u64>("0"), Some(0));
        assert_eq!(parse_strict::<u64>("+5"), None);
        assert_eq!(parse_strict::<u64>(" 5"), None);
        assert_eq!(parse_strict::<u64>("5 "), None);
        assert_eq!(parse_strict::<u64>("1_000"), None);
        assert_eq!(parse_strict::<u64>("1e2"), None);
        assert_eq!(parse_strict::<u64>("-5"), None);
        assert_eq!(parse_strict::<u64>(""), None);
    }

    #[test]
    fn parse_strict_still_enforces_the_target_type_range() {
        // The digits-only grammar check is layered on top of, not instead
        // of, `FromStr`'s own overflow rejection — a field that is all
        // digits but too big for the target type must still fail.
        assert_eq!(parse_strict::<u16>("65535"), Some(65535));
        assert_eq!(parse_strict::<u16>("65536"), None);
        assert_eq!(parse_strict::<u8>("255"), Some(255));
        assert_eq!(parse_strict::<u8>("256"), None);
    }

    /// Feeds `wire` to `read_one_response` over a real socket pair (the
    /// function is generic only over `tagged`, not the stream, so a
    /// concrete `Stream::Plain` is the only way to drive it) and returns
    /// its result.
    async fn read_one_from_bytes(
        wire: &[u8],
        tagged: bool,
        max_multi_get_response_bytes: usize,
    ) -> Result<WireResponse> {
        use tokio::net::{TcpListener, TcpStream};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let wire = wire.to_vec();
        let writer = tokio::spawn(async move {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(&wire).await.unwrap();
            // Hold the connection open so a well-formed frame isn't
            // truncated by an early close mid-read.
            client.shutdown().await.unwrap();
        });
        let (server, _) = listener.accept().await.unwrap();
        let (read_half, _write_half) = split(Stream::Plain(server));
        let mut read_half = BufReader::new(read_half);
        let result = read_one_response(&mut read_half, tagged, max_multi_get_response_bytes).await;
        writer.await.unwrap();
        result
    }

    /// A bound large enough not to interfere, for tests unconcerned with it.
    const UNBOUNDED_MULTI_GET: usize = usize::MAX;

    #[tokio::test]
    async fn a_value_length_with_a_leading_plus_is_a_protocol_error_not_silently_accepted() {
        // Issue #462: `str::parse::<usize>()` alone would accept a leading
        // `+` here, unlike the server's own `^[0-9]+$` length grammar
        // (`parse_length` in `src/command.rs`). A reply that violates the
        // grammar must be rejected as a protocol error, which — one layer
        // up, in `read_loop` — poisons the connection rather than being
        // coerced or silently accepted.
        let result = read_one_from_bytes(b"V +5\nhello", false, UNBOUNDED_MULTI_GET).await;
        assert!(
            matches!(result, Err(Error::Protocol(ref message)) if message.contains("invalid value length")),
            "a value length with a leading '+' must be a protocol error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn a_malformed_value_length_poisons_the_connection_for_later_requests() {
        // Issue #462, connection-level: once `read_one_response` rejects a
        // reply's wire grammar, `read_loop` treats that exactly like any
        // other malformed frame — `mark_closed` before `drain_pending`
        // (see `read_loop`'s own doc comment) — so a request on the same
        // `Connection` *after* the malformed one must also fail, not
        // silently keep using a desynced stream.
        use tokio::net::{TcpListener, TcpStream};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Don't bother parsing the request frame — any bytes at all
            // trigger this canned, wire-grammar-violating reply (a
            // leading `+`, issue #462).
            let mut buf = [0u8; 64];
            let _ = socket.read(&mut buf).await;
            socket.write_all(b"V +5\nhello").await.unwrap();
            // Hold the socket open so the second request (which never
            // gets an actual answer) can't race an early close.
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let connection = Connection::new(
            Stream::Plain(client),
            "127.0.0.1:0".to_string(),
            false,
            Arc::new(AtomicU64::new(0)),
            Duration::from_millis(200),
        );

        let first = connection.get(b"", b"key").await;
        assert!(
            matches!(first, Err(Error::Protocol(ref message)) if message.contains("invalid value length")),
            "a malformed value length must be a protocol error, got {first:?}"
        );

        let second = connection.get(b"", b"key2").await;
        assert!(
            matches!(second, Err(Error::ConnectionLost(_))),
            "a request after a malformed reply must see the connection already closed \
             (poisoned), got {second:?}"
        );

        server.abort();
    }

    #[tokio::test]
    async fn multi_get_header_with_an_overflowing_count_is_a_protocol_error_not_a_panic() {
        // Regression (pass-7 audit): `count` is untrusted. `usize::MAX`
        // once made `count + 1` wrap to 0 in release builds, slipping past
        // the field-count check straight into an out-of-bounds
        // `rest[..count]` panic inside the spawned read loop. It must be a
        // clean protocol error instead.
        let result =
            read_one_from_bytes(b"M 18446744073709551615\n", true, UNBOUNDED_MULTI_GET).await;
        assert!(
            matches!(result, Err(Error::Protocol(_))),
            "an overflowing multi-get count must be a protocol error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn multi_get_reply_over_the_cumulative_bound_is_rejected_before_the_last_body() {
        // Issue #207, moved off the process-wide MAX_MULTI_GET_RESPONSE_BYTES
        // static (2026-09-01): a reply whose hit bodies sum past the cap is
        // a protocol error, rejected before the offending body is read. Two
        // 2-byte hits (running total 4) trip a bound of 3. Passing the bound
        // explicitly here — instead of shrinking the shared static — keeps
        // this from racing any concurrently-running multi-get test.
        let result = read_one_from_bytes(b"M 2 2 2\nxyzz", false, 3).await;
        assert!(
            matches!(result, Err(Error::Protocol(ref message)) if message.contains("exceeds 3 bytes")),
            "a reply over the cumulative bound must be a protocol error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn multi_get_reply_within_the_cumulative_bound_is_accepted() {
        // The bound-not-tripped counterpart: two hits summing to exactly the
        // 3-byte bound (2 + 1) are accepted, proving the check doesn't reject
        // a reply merely for being close to the bound.
        let result = read_one_from_bytes(b"M 2 2 1\nxyz", false, 3).await;
        let marker = result.expect("a reply within the bound must be accepted").0;
        assert_eq!(marker, b'M');
    }

    #[test]
    fn encode_get_emits_the_legacy_frame_for_the_default_namespace() {
        assert_eq!(encode_get(b"", b"name", None), b"G 4\nname".to_vec());
    }

    #[test]
    fn encode_get_emits_the_tagged_legacy_frame() {
        assert_eq!(encode_get(b"", b"name", Some(7)), b"G 4 7\nname".to_vec());
    }

    #[test]
    fn encode_get_emits_the_namespaced_frame() {
        assert_eq!(
            encode_get(b"users", b"alpha", None),
            [b"g 5 5\n".as_slice(), b"users", b"alpha"].concat()
        );
    }

    #[test]
    fn encode_get_emits_the_tagged_namespaced_frame() {
        assert_eq!(
            encode_get(b"users", b"alpha", Some(3)),
            [b"g 5 5 3\n".as_slice(), b"users", b"alpha"].concat()
        );
    }

    #[test]
    fn encode_get_accepts_a_binary_namespace() {
        // No delimiter, no escaping — a namespace may contain any bytes,
        // including ones that would be ambiguous in text (0xff, 0x00).
        assert_eq!(
            encode_get(b"\xff\x00", b"beta", None),
            [b"g 2 4\n".as_slice(), b"\xff\x00", b"beta"].concat()
        );
    }

    #[test]
    fn encode_set_omits_the_ttl_field_when_zero_for_the_default_namespace() {
        assert_eq!(
            encode_set(b"", b"k", b"v", 0, None),
            [b"S 1 1\n".as_slice(), b"k", b"v"].concat()
        );
    }

    #[test]
    fn encode_set_includes_ttl_for_the_default_namespace() {
        assert_eq!(
            encode_set(b"", b"k", b"v", 60, None),
            [b"S 1 1 60\n".as_slice(), b"k", b"v"].concat()
        );
    }

    #[test]
    fn encode_set_namespaced_without_ttl_or_tag() {
        assert_eq!(
            encode_set(b"ns", b"k", b"v", 0, None),
            [b"s 2 1 1\n".as_slice(), b"ns", b"k", b"v"].concat()
        );
    }

    #[test]
    fn encode_set_namespaced_with_ttl_and_tag() {
        // The spec's own callout: the ttl+tag `s` form.
        assert_eq!(
            encode_set(b"ns", b"k", b"v", 60, Some(9)),
            [b"s 2 1 1 60 9\n".as_slice(), b"ns", b"k", b"v"].concat()
        );
    }

    #[test]
    fn encode_set_namespaced_with_tag_but_no_ttl() {
        assert_eq!(
            encode_set(b"ns", b"k", b"v", 0, Some(9)),
            [b"s 2 1 1 9\n".as_slice(), b"ns", b"k", b"v"].concat()
        );
    }

    #[test]
    fn encode_delete_emits_the_legacy_frame_for_the_default_namespace() {
        assert_eq!(encode_delete(b"", b"name", None), b"D 4\nname".to_vec());
    }

    #[test]
    fn encode_delete_emits_the_tagged_namespaced_frame() {
        assert_eq!(
            encode_delete(b"users", b"alpha", Some(3)),
            [b"d 5 5 3\n".as_slice(), b"users", b"alpha"].concat()
        );
    }

    #[test]
    fn encode_clear_untagged_default_namespace() {
        // Unlike get/set/delete, clear has no legacy uppercase frame to
        // preserve — even the empty (default) namespace goes out as `c 0`
        // rather than switching command letters (issue #106).
        assert_eq!(encode_clear(b"", None), b"c 0\n".to_vec());
    }

    #[test]
    fn encode_clear_tagged_default_namespace() {
        assert_eq!(encode_clear(b"", Some(5)), b"c 0 5\n".to_vec());
    }

    #[test]
    fn encode_clear_untagged_named_namespace() {
        assert_eq!(
            encode_clear(b"users", None),
            [b"c 5\n".as_slice(), b"users"].concat()
        );
    }

    #[test]
    fn encode_clear_tagged_named_namespace() {
        assert_eq!(
            encode_clear(b"users", Some(3)),
            [b"c 5 3\n".as_slice(), b"users"].concat()
        );
    }

    #[test]
    fn encode_clear_all_untagged() {
        assert_eq!(encode_clear_all(None), b"F\n".to_vec());
    }

    #[test]
    fn encode_clear_all_tagged() {
        assert_eq!(encode_clear_all(Some(9)), b"F 9\n".to_vec());
    }

    #[test]
    fn cleared_response_converts_from_the_c_marker() {
        assert!(matches!(
            ResponseKind::try_from((b'C', None, None, None)),
            Ok(ResponseKind::Cleared)
        ));
    }

    // ── incr (issue #129) ────────────────────────────────────────────

    #[test]
    fn encode_incr_emits_the_namespaced_frame_for_the_default_namespace() {
        // Unlike get/set/delete, incr has no legacy uppercase frame to
        // preserve — even the empty (default) namespace goes out as
        // `i 0 ...` rather than switching command letters (issue #129,
        // mirrors encode_clear's own no-legacy-form rule).
        assert_eq!(
            encode_incr(b"", b"hits", 5, None),
            [b"i 0 4 5\n".as_slice(), b"hits"].concat()
        );
    }

    #[test]
    fn encode_incr_emits_a_negative_delta_in_canonical_form() {
        assert_eq!(
            encode_incr(b"", b"hits", -5, None),
            [b"i 0 4 -5\n".as_slice(), b"hits"].concat()
        );
    }

    #[test]
    fn encode_incr_emits_the_tagged_frame() {
        assert_eq!(
            encode_incr(b"", b"hits", 5, Some(7)),
            [b"i 0 4 5 7\n".as_slice(), b"hits"].concat()
        );
    }

    #[test]
    fn encode_incr_emits_the_namespaced_frame() {
        assert_eq!(
            encode_incr(b"users", b"hits", 5, None),
            [b"i 5 4 5\n".as_slice(), b"users", b"hits"].concat()
        );
    }

    #[test]
    fn encode_incr_emits_the_tagged_namespaced_frame() {
        assert_eq!(
            encode_incr(b"users", b"hits", -3, Some(9)),
            [b"i 5 4 -3 9\n".as_slice(), b"users", b"hits"].concat()
        );
    }

    #[test]
    fn incr_response_converts_from_the_i_marker_without_a_ttl() {
        assert!(matches!(
            ResponseKind::try_from((b'I', Some(b"42".to_vec()), None, None)),
            Ok(ResponseKind::Incr(42, None))
        ));
    }

    #[test]
    fn incr_response_converts_from_the_i_marker_with_a_ttl() {
        assert!(matches!(
            ResponseKind::try_from((b'I', Some(b"42".to_vec()), Some(60), None)),
            Ok(ResponseKind::Incr(42, Some(60)))
        ));
    }

    #[test]
    fn incr_response_converts_a_negative_value_from_the_i_marker() {
        assert!(matches!(
            ResponseKind::try_from((b'I', Some(b"-7".to_vec()), None, None)),
            Ok(ResponseKind::Incr(-7, None))
        ));
    }

    // ── compare-and-set (issue #141) ────────────────────────────────

    #[test]
    fn encode_cas_set_emits_the_absent_condition_for_the_default_namespace() {
        // Like incr, k has no legacy uppercase frame — even the empty
        // (default) namespace goes out as `k 0 ...`.
        assert_eq!(
            encode_cas_set(b"", b"name", b"Alice", CasCondition::Absent, 0, None),
            [b"k 0 4 5 A\n".as_slice(), b"name", b"Alice"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_present_condition() {
        assert_eq!(
            encode_cas_set(b"", b"name", b"Bob", CasCondition::Present, 0, None),
            [b"k 0 4 3 P\n".as_slice(), b"name", b"Bob"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_digest_condition_as_lowercase_hex() {
        let digest = crate::cas::content_digest(b"Alice");
        let expected_header = format!("k 0 4 3 {}\n", crate::cas::CasToken::from(digest));
        assert_eq!(
            encode_cas_set(b"", b"name", b"Bob", CasCondition::Digest(digest), 0, None),
            [expected_header.as_bytes(), b"name", b"Bob"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_ttl_field_when_present() {
        assert_eq!(
            encode_cas_set(b"", b"name", b"Alice", CasCondition::Absent, 60, None),
            [b"k 0 4 5 A 60\n".as_slice(), b"name", b"Alice"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_tagged_frame_without_a_ttl() {
        assert_eq!(
            encode_cas_set(b"", b"name", b"Alice", CasCondition::Absent, 0, Some(7)),
            [b"k 0 4 5 A 7\n".as_slice(), b"name", b"Alice"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_tagged_frame_with_a_ttl() {
        assert_eq!(
            encode_cas_set(b"", b"name", b"Alice", CasCondition::Absent, 60, Some(7)),
            [b"k 0 4 5 A 60 7\n".as_slice(), b"name", b"Alice"].concat()
        );
    }

    #[test]
    fn encode_cas_set_emits_the_namespaced_frame() {
        assert_eq!(
            encode_cas_set(b"users", b"name", b"Alice", CasCondition::Present, 0, None),
            [b"k 5 4 5 P\n".as_slice(), b"users", b"name", b"Alice"].concat()
        );
    }

    #[test]
    fn encode_cas_delete_emits_the_digest_condition() {
        let digest = crate::cas::content_digest(b"Alice");
        let expected_header = format!("x 0 4 {}\n", crate::cas::CasToken::from(digest));
        assert_eq!(
            encode_cas_delete(b"", b"name", digest, None),
            [expected_header.as_bytes(), b"name"].concat()
        );
    }

    #[test]
    fn encode_cas_delete_emits_the_tagged_frame() {
        let digest = crate::cas::content_digest(b"Alice");
        let expected_header = format!("x 0 4 {} 9\n", crate::cas::CasToken::from(digest));
        assert_eq!(
            encode_cas_delete(b"", b"name", digest, Some(9)),
            [expected_header.as_bytes(), b"name"].concat()
        );
    }

    #[test]
    fn encode_cas_delete_emits_the_namespaced_frame() {
        let digest = crate::cas::content_digest(b"Alice");
        let expected_header = format!("x 5 4 {}\n", crate::cas::CasToken::from(digest));
        assert_eq!(
            encode_cas_delete(b"users", b"name", digest, None),
            [expected_header.as_bytes(), b"users", b"name"].concat()
        );
    }

    #[test]
    fn cas_condition_token_matches_the_pinned_cross_language_vector() {
        // Same vector docs/protocol.html#cas pins, exercised through the
        // wire-token path this module actually sends.
        let digest = crate::cas::content_digest(b"nanocached-cas-vector");
        assert_eq!(
            cas_condition_token(CasCondition::Digest(digest)),
            "36287141940ca57acbd7695ccdde9d43"
        );
    }

    #[test]
    fn not_found_response_converts_from_the_n_marker() {
        assert!(matches!(
            ResponseKind::try_from((b'N', None, None, None)),
            Ok(ResponseKind::NotFound)
        ));
    }

    #[test]
    fn not_numeric_response_converts_from_the_t_marker_to_an_error() {
        assert!(matches!(
            ResponseKind::try_from((b'T', None, None, None)),
            Err(Error::NotNumeric)
        ));
    }
}
