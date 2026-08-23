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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
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

/// A raw response marker byte plus its value bytes (`V` only) — what the
/// read task parses off the wire, before `get`/`set`/`delete` convert it
/// into a [`ResponseKind`] or a `WrongNode`/protocol error. The echoed
/// tag (echoed response tags), when present, is verified against the pending slot's
/// expected tag by the read loop itself and never reaches this type — see
/// `WireResponse`.
type RawResponse = (u8, Option<Vec<u8>>);
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
    /// is counted against that one key (see `open_targets`).
    pub(crate) fn new(stream: Stream, tracking_key: String, tagged: bool) -> Self {
        crate::open_targets::increment(&tracking_key);
        let (read_half, write_half) = split(stream);
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
        }
    }

    /// A pre-poisoned placeholder for a newly discovered node — see the
    /// `write_state` field docs.
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
        F: FnOnce(Option<u32>) -> Vec<u8>,
    {
        let timeout = Duration::from_millis(REQUEST_TIMEOUT_MS.load(Ordering::SeqCst));
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

    /// Enqueues a pending slot and writes `frame` under one lock (see the
    /// module doc comment), then waits on its own oneshot receiver — not
    /// the socket. If this future is dropped while awaiting that
    /// receiver (the write already completed), the slot is simply left
    /// in the queue: the read task will eventually find no receiver
    /// listening and move on, exactly like the TypeScript SDK's
    /// Connection, whose plain Promises can't be cancelled out from
    /// under `pending` at all — every request behind it in the queue is
    /// unaffected. (`request`'s timeout wrapper additionally poisons the
    /// connection outright when it fires, since in that case nothing is
    /// ever going to answer.)
    async fn request_uncapped<F>(&self, build: F) -> Result<ResponseKind>
    where
        F: FnOnce(Option<u32>) -> Vec<u8>,
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

        match rx.await {
            Ok(Ok(raw)) => ResponseKind::try_from(raw),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(Error::ConnectionLost(
                "nanocached: connection is closed".to_string(),
            )),
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

/// This connection's only reader, for its whole lifetime — nothing else
/// may read from `read_half`. Consumes responses off the wire and
/// dispatches each to the oldest pending request (FIFO —
/// Request pipelining), until told to stop (poisoned by any of the
/// triggers in `Shared::mark_closed`) or a read itself fails.
async fn read_loop(
    mut read_half: ReadHalf<Stream>,
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
            result = read_one_response(&mut read_half, shared.tagged) => result,
        };

        let (marker, value, tag) = match response {
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
        if !matches!(marker, b'V' | b'N' | b'S' | b'D' | b'W' | b'C') {
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
        let _ = slot.tx.send(Ok((marker, value)));
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

/// A marker byte, its value bytes (`V` only), and — on a tagged
/// connection (echoed response tags) — the tag it echoed, straight off the wire
/// before the read loop has verified that tag against anything. Only
/// `read_one_response`/`read_loop` ever see this third field; once
/// verified it's stripped down to a plain [`RawResponse`] before being
/// handed to a waiting caller.
type WireResponse = (u8, Option<Vec<u8>>, Option<u32>);

async fn read_one_response(read_half: &mut ReadHalf<Stream>, tagged: bool) -> Result<WireResponse> {
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
                        ))
                    }
                }
            } else {
                (header, None)
            };
            // The server never stores values above its 1 MiB request
            // limit, so a claimed length beyond MAX_VALUE_LENGTH is a
            // corrupt or malicious frame (issue #12); reject before
            // allocating.
            let length: usize = length_field
                .parse()
                .ok()
                .filter(|length| *length <= MAX_VALUE_LENGTH)
                .ok_or_else(|| {
                    Error::Protocol("nanocached: invalid value length in response".to_string())
                })?;
            let mut value = vec![0u8; length];
            read_half.read_exact(&mut value).await?;
            Ok((marker, Some(value), tag))
        }
        b'B' => {
            // `B` (busy) is always untagged — unsolicited, sent before
            // auth (and so before tagging) even completes (echoed response tags).
            read_half.read_u8().await?; // the trailing '\n'
            Ok((marker, None, None))
        }
        b'S' | b'D' | b'N' | b'W' | b'C' => {
            if tagged {
                // `S <tag>\n` / `D <tag>\n` / `N <tag>\n` / `W <tag>\n` /
                // `C <tag>\n` (issue #106).
                let line = read_line(read_half).await?;
                Ok((marker, None, Some(parse_tag(line.trim())?)))
            } else {
                read_half.read_u8().await?; // the trailing '\n'
                Ok((marker, None, None))
            }
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
    field
        .parse()
        .map_err(|_| Error::Protocol("nanocached: invalid response tag".to_string()))
}

/// Converts `request`'s raw `(marker, value)` into the higher-level kind
/// `get`/`set`/`delete` switch on, or `Error::WrongNode` for `W`.
impl TryFrom<RawResponse> for ResponseKind {
    type Error = Error;

    fn try_from((marker, value): RawResponse) -> Result<Self> {
        match marker {
            b'V' => Ok(ResponseKind::Value(value.unwrap_or_default())),
            b'N' => Ok(ResponseKind::NotFound),
            b'S' => Ok(ResponseKind::Stored),
            b'D' => Ok(ResponseKind::Deleted),
            b'C' => Ok(ResponseKind::Cleared),
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
            ResponseKind::try_from((b'C', None)),
            Ok(ResponseKind::Cleared)
        ));
    }
}
