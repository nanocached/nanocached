//! One already-identified connection to a single nanocached-node,
//! speaking the cache protocol (`G`/`S`/`D` — the `A` identify exchange
//! happens in `identify` before a `Connection` exists). Requests are
//! pipelined onto the socket and matched to responses in send order
//! (doc/adr/0016-*.md): a dedicated read task, spawned in `new`, consumes
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

/// The server never stores values above its 1 MiB request limit.
const MAX_VALUE_LENGTH: usize = 2 * 1024 * 1024;

/// A raw response marker byte plus its value bytes (`V` only) — what the
/// read task parses off the wire, before `get`/`set`/`delete` convert it
/// into a [`ResponseKind`] or a `WrongNode`/protocol error.
type RawResponse = (u8, Option<Vec<u8>>);
type RawResponseSender = oneshot::Sender<Result<RawResponse>>;

struct WriteState {
    /// `None` once poisoned (or for the pre-poisoned placeholder,
    /// `dead()`, which never opened a socket) — further requests fail
    /// as connection-lost rather than reusing a torn-down half.
    write_half: Option<WriteHalf<Stream>>,
    pending: VecDeque<RawResponseSender>,
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

impl Connection {
    /// `tracking_key` is the client's winning connect address ("host:port"
    /// of whichever configured address answered `connect()`) — every
    /// socket the client ever opens, regardless of which node it dials,
    /// is counted against that one key (see `open_targets`).
    pub(crate) fn new(stream: Stream, tracking_key: String) -> Self {
        crate::open_targets::increment(&tracking_key);
        let (read_half, write_half) = split(stream);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let shared = Arc::new(Shared {
            write_state: Mutex::new(WriteState {
                write_half: Some(write_half),
                pending: VecDeque::new(),
            }),
            closed: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            tracking_key: Some(tracking_key),
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
                }),
                closed: AtomicBool::new(true),
                last_used_ms: AtomicU64::new(0),
                epoch: Instant::now(),
                tracking_key: None,
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

    pub(crate) async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut frame = format!("G {}\n", key.len()).into_bytes();
        frame.extend_from_slice(key);
        match self.request(&frame).await? {
            ResponseKind::Value(value) => Ok(Some(value)),
            ResponseKind::NotFound => Ok(None),
            other => Err(self.mismatch(&other)),
        }
    }

    /// `ttl_seconds == 0` means no expiry — mapped to the wire exactly as
    /// the absent-TTL frame always was.
    pub(crate) async fn set(&self, key: &[u8], value: &[u8], ttl_seconds: u64) -> Result<()> {
        let header = if ttl_seconds == 0 {
            format!("S {} {}\n", key.len(), value.len())
        } else {
            format!("S {} {} {}\n", key.len(), value.len(), ttl_seconds)
        };
        let mut frame = header.into_bytes();
        frame.extend_from_slice(key);
        frame.extend_from_slice(value);
        match self.request(&frame).await? {
            ResponseKind::Stored => Ok(()),
            other => Err(self.mismatch(&other)),
        }
    }

    pub(crate) async fn delete(&self, key: &[u8]) -> Result<bool> {
        let mut frame = format!("D {}\n", key.len()).into_bytes();
        frame.extend_from_slice(key);
        match self.request(&frame).await? {
            ResponseKind::Deleted => Ok(true),
            ResponseKind::NotFound => Ok(false),
            other => Err(self.mismatch(&other)),
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
    /// unaffected.
    async fn request(&self, frame: &[u8]) -> Result<ResponseKind> {
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
            state.pending.push_back(tx);
            let write_half = state.write_half.as_mut().expect("checked above");

            let mut guard = WriteGuard {
                shared: &self.shared,
                shutdown: &self.shutdown,
                completed: false,
            };
            let write_result = write_half.write_all(frame).await;
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
    /// Connection (doc/adr/0016-*.md), not something this SDK introduces.
    fn mismatch(&self, kind: &ResponseKind) -> Error {
        let name = match kind {
            ResponseKind::Value(_) => "value",
            ResponseKind::NotFound => "not-found",
            ResponseKind::Stored => "stored",
            ResponseKind::Deleted => "deleted",
        };
        self.close();
        Error::ConnectionLost(format!(
            "nanocached: response \"{name}\" does not match the request (connection desynced)"
        ))
    }
}

/// This connection's only reader, for its whole lifetime — nothing else
/// may read from `read_half`. Consumes responses off the wire and
/// dispatches each to the oldest pending request (FIFO —
/// doc/adr/0016-*.md), until told to stop (poisoned by any of the
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
            result = read_one_response(&mut read_half) => result,
        };

        let (marker, value) = match response {
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

        let (was_empty, tx) = {
            let mut state = shared.write_state.lock().await;
            let was_empty = state.pending.is_empty();
            let tx = if was_empty {
                None
            } else {
                state.pending.pop_front()
            };
            (was_empty, tx)
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
        let Some(tx) = tx else {
            // Unsolicited and not the known busy case — desync.
            shared.mark_closed(&shutdown_tx);
            drain_pending(&shared, None).await;
            return;
        };
        // An Err here just means the caller abandoned this request after
        // its write completed (see `Connection::request`) — nothing to
        // do, the queue position was already correctly consumed above.
        let _ = tx.send(Ok((marker, value)));
    }
}

/// Rejects every request still queued, and drops the write half so the
/// socket actually closes now rather than waiting for the last
/// `Connection` clone to drop. When `first_error` is given, it's
/// delivered to whichever request was oldest — the one whose response
/// actually failed to parse, if that's why the read loop is exiting;
/// every other still-queued request gets a generic "connection closed",
/// since their responses were never received at all.
async fn drain_pending(shared: &Shared, first_error: Option<Error>) {
    let mut state = shared.write_state.lock().await;
    state.write_half = None;
    let mut pending = state.pending.drain(..);
    if let Some(error) = first_error {
        if let Some(tx) = pending.next() {
            let _ = tx.send(Err(error));
        }
    }
    for tx in pending {
        let _ = tx.send(Err(Error::ConnectionLost(
            "nanocached: connection closed".to_string(),
        )));
    }
}

async fn read_one_response(read_half: &mut ReadHalf<Stream>) -> Result<RawResponse> {
    let marker = read_half.read_u8().await?;
    match marker {
        b'V' => {
            let header = read_line(read_half).await?;
            // The server never stores values above its 1 MiB request
            // limit, so a claimed length beyond MAX_VALUE_LENGTH is a
            // corrupt or malicious frame (issue #12); reject before
            // allocating.
            let length: usize = header
                .trim()
                .parse()
                .ok()
                .filter(|length| *length <= MAX_VALUE_LENGTH)
                .ok_or_else(|| {
                    Error::Protocol("nanocached: invalid value length in response".to_string())
                })?;
            let mut value = vec![0u8; length];
            read_half.read_exact(&mut value).await?;
            Ok((marker, Some(value)))
        }
        b'S' | b'D' | b'N' | b'W' | b'B' => {
            read_half.read_u8().await?; // the trailing '\n'
            Ok((marker, None))
        }
        other => Err(Error::Protocol(format!(
            "nanocached: unexpected response from server: {}",
            other as char
        ))),
    }
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
    }
}
