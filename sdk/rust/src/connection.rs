//! One already-identified connection to a single nanocached-node,
//! speaking the cache protocol (`G`/`S`/`D` — the `A` identify exchange
//! happens in `identify` before a `Connection` exists). Requests are
//! serialized per connection (a tokio mutex around each round trip) — a
//! deliberate v1 simplification over the TypeScript SDK's pipelining:
//! nanocached-node answers in arrival order, so serializing is always
//! correct, just less concurrent. Concurrent callers queue on the mutex.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::identify::Stream;

/// The server never stores values above its 1 MiB request limit.
const MAX_VALUE_LENGTH: usize = 2 * 1024 * 1024;

pub(crate) struct Connection {
    /// `None` only for the pre-poisoned placeholder (`dead()`) a node-list
    /// refresh installs for a newly discovered member — the first request
    /// through it fails as connection-lost and triggers the lazy redial.
    stream: Mutex<Option<BufReader<Stream>>>,
    closed: AtomicBool,
    /// True while a round trip is on the wire, cleared when its response
    /// has been fully read. A caller dropping the request future mid-way
    /// (`tokio::time::timeout` and friends) leaves it set — the response
    /// is still unread, so the stream is desynced and must not be reused.
    in_flight: AtomicBool,
    /// Milliseconds since `epoch` of the last request — what the
    /// keep-alive timer checks against its interval.
    last_used_ms: AtomicU64,
    epoch: Instant,
}

pub(crate) enum ResponseKind {
    Value(Vec<u8>),
    NotFound,
    Stored,
    Deleted,
}

impl Connection {
    pub(crate) fn new(stream: Stream) -> Self {
        Self {
            stream: Mutex::new(Some(BufReader::new(stream))),
            closed: AtomicBool::new(false),
            in_flight: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    /// A pre-poisoned placeholder for a newly discovered node — see the
    /// `stream` field docs.
    pub(crate) fn dead() -> Self {
        Self {
            stream: Mutex::new(None),
            closed: AtomicBool::new(true),
            in_flight: AtomicBool::new(false),
            last_used_ms: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub(crate) fn close(&self) {
        // Dropping the TCP stream happens when the Connection is dropped;
        // marking closed is what keeps further requests off it.
        self.closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn idle(&self) -> Duration {
        let last = self.last_used_ms.load(Ordering::SeqCst);
        self.epoch
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

    pub(crate) async fn set(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let header = match ttl_seconds {
            Some(ttl) => format!("S {} {} {}\n", key.len(), value.len(), ttl),
            None => format!("S {} {}\n", key.len(), value.len()),
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

    async fn request(&self, frame: &[u8]) -> Result<ResponseKind> {
        if self.is_closed() {
            return Err(Error::ConnectionLost(
                "nanocached: connection is closed".to_string(),
            ));
        }

        let mut guard = self.stream.lock().await;
        let Some(stream) = guard.as_mut() else {
            return Err(Error::ConnectionLost(
                "nanocached: connection is closed".to_string(),
            ));
        };
        self.last_used_ms
            .store(self.epoch.elapsed().as_millis() as u64, Ordering::SeqCst);

        if self.in_flight.load(Ordering::SeqCst) {
            // The previous request's future was dropped mid-round-trip;
            // its response is still unread on the wire, so reusing this
            // stream would answer every later request with an earlier
            // response. Poison, and let the caller redial.
            self.close();
            return Err(Error::ConnectionLost(
                "nanocached: a previous request was abandoned mid-flight (connection desynced)"
                    .to_string(),
            ));
        }

        self.in_flight.store(true, Ordering::SeqCst);
        let outcome = async {
            stream.get_mut().write_all(frame).await?;
            self.read_response(stream).await
        }
        .await;
        self.in_flight.store(false, Ordering::SeqCst);

        if matches!(outcome, Err(Error::ConnectionLost(_) | Error::Protocol(_))) {
            // The stream state after a failed round trip is unknown —
            // poison the connection so the client redials lazily.
            self.close();
        }
        outcome
    }

    /// A well-formed response of the wrong kind (a `S` answering a G)
    /// means the request/response streams are misaligned — every later
    /// response would answer the wrong request, silently returning other
    /// keys' data. Poison the connection, and classify as connection-lost
    /// so the client's retry layer redials and retries once.
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

    async fn read_response(&self, stream: &mut BufReader<Stream>) -> Result<ResponseKind> {
        let marker = stream.read_u8().await?;
        match marker {
            b'V' => {
                let header = read_line(stream).await?;
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
                stream.read_exact(&mut value).await?;
                Ok(ResponseKind::Value(value))
            }
            b'S' | b'D' | b'N' | b'W' => {
                stream.read_u8().await?; // the trailing '\n'
                match marker {
                    b'S' => Ok(ResponseKind::Stored),
                    b'D' => Ok(ResponseKind::Deleted),
                    b'N' => Ok(ResponseKind::NotFound),
                    _ => Err(Error::WrongNode),
                }
            }
            b'B' => Err(Error::ConnectionLost(
                "nanocached: server rejected the connection (connection limit reached)".to_string(),
            )),
            other => Err(Error::Protocol(format!(
                "nanocached: unexpected response from server: {}",
                other as char
            ))),
        }
    }
}

pub(crate) async fn read_line(stream: &mut BufReader<Stream>) -> Result<String> {
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
